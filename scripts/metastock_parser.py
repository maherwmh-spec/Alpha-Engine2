"""
MetaStock File Parser — Alpha-Engine2
======================================
يقرأ ملفات MetaStock الثنائية (MASTER / EMASTER / XMASTER + F{n}.DAT / F{n}.MWD)
ويحوّلها إلى DataFrame جاهز للإدخال في جدول market_data.ohlcv.

تنسيق MetaStock:
  - MASTER  : فهرس الرموز (حتى 255 رمزاً)، حجم كل سجل = 53 بايت
  - EMASTER : فهرس موسّع (حتى 255 رمزاً)، حجم كل سجل = 192 بايت
  - XMASTER : فهرس موسّع جديد (أكثر من 255 رمزاً)، حجم كل سجل = 150 بايت
  - F{n}.DAT : ملف بيانات الشموع (رقم 1-255)
  - F{n}.MWD : ملف بيانات الشموع (رقم 256+)

الأرقام العائمة مخزّنة بتنسيق Microsoft Binary Format (MBF)،
وليس IEEE 754 المعتاد في Python.
"""

from __future__ import annotations

import os
import struct
import zipfile
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger


# ─────────────────────────────────────────────────────────────────────────────
# تحويل MBF ← → IEEE
# ─────────────────────────────────────────────────────────────────────────────

def _mbf4_to_float(raw: bytes) -> float:
    """
    تحويل 4 بايت بتنسيق Microsoft Binary Format (MBF-4) إلى float Python.
    الخوارزمية مأخوذة من مواصفات MetaStock الرسمية.
    """
    if len(raw) < 4:
        return 0.0
    b0, b1, b2, b3 = raw[0], raw[1], raw[2], raw[3]
    # البايت الأخير هو الأس
    exp = b3
    if exp == 0:
        return 0.0
    # إعادة بناء IEEE 754
    sign = (b2 & 0x80) >> 7
    mantissa = ((b2 & 0x7F) << 16) | (b1 << 8) | b0
    # تحويل الأس: MBF bias=128, IEEE bias=127
    ieee_exp = exp - 2   # (exp - 128 + 127) = exp - 1 ، لكن MBF يضع الـ MSB ضمنياً
    # بناء IEEE 754 single precision
    ieee_bits = (sign << 31) | (ieee_exp << 23) | mantissa
    try:
        return struct.unpack('f', struct.pack('I', ieee_bits & 0xFFFFFFFF))[0]
    except Exception:
        return 0.0


def _mbf_date_to_date(raw: bytes) -> Optional[date]:
    """تحويل تاريخ MetaStock (float) إلى كائن date Python."""
    val = _mbf4_to_float(raw)
    if val == 0.0:
        return None
    try:
        d = int(val)
        year  = 1900 + (d // 10000)
        month = (d % 10000) // 100
        day   = d % 100
        return date(year, month, day)
    except (ValueError, OverflowError):
        return None


def _mbf_time_to_time(raw: bytes) -> Optional[Tuple[int, int]]:
    """تحويل وقت MetaStock (float) إلى (hour, minute)."""
    val = _mbf4_to_float(raw)
    if val == 0.0:
        return None
    try:
        t = int(val)
        hour   = t // 10000
        minute = (t % 10000) // 100
        return (hour, minute)
    except (ValueError, OverflowError):
        return None


def _read_short(data: bytes) -> int:
    """قراءة عدد صحيح 2 بايت little-endian."""
    return struct.unpack('<H', data[:2])[0]


def _read_byte(data: bytes) -> int:
    """قراءة بايت واحد."""
    return data[0] if data else 0


def _read_str(data: bytes) -> str:
    """قراءة سلسلة نصية وإزالة البايتات الفارغة."""
    return data.split(b'\x00')[0].decode('latin-1', errors='replace').strip()


# ─────────────────────────────────────────────────────────────────────────────
# بنية معلومات الرمز
# ─────────────────────────────────────────────────────────────────────────────

class SymbolInfo:
    """معلومات رمز واحد مستخرجة من ملف الفهرس."""

    __slots__ = (
        'file_num', 'num_fields', 'symbol', 'name',
        'time_frame', 'first_date', 'last_date',
        'columns',
    )

    def __init__(self):
        self.file_num: int = 0
        self.num_fields: int = 7
        self.symbol: str = ''
        self.name: str = ''
        self.time_frame: str = 'D'
        self.first_date: Optional[date] = None
        self.last_date: Optional[date] = None
        self.columns: List[str] = ['DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL', 'OI']

    def __repr__(self) -> str:
        return (
            f"SymbolInfo(file_num={self.file_num}, symbol={self.symbol!r}, "
            f"name={self.name!r}, tf={self.time_frame}, "
            f"fields={self.num_fields})"
        )


# ─────────────────────────────────────────────────────────────────────────────
# قراءة ملفات الفهرس
# ─────────────────────────────────────────────────────────────────────────────

def _read_emaster(path: Path) -> List[SymbolInfo]:
    """
    قراءة ملف EMASTER (الفهرس الموسّع لـ MetaStock 6.x).
    حجم كل سجل = 192 بايت.
    السجل الأول هو رأس الملف.
    """
    symbols: List[SymbolInfo] = []
    record_size = 192

    with open(path, 'rb') as fh:
        header = fh.read(record_size)
        if len(header) < record_size:
            logger.warning(f"EMASTER header too short: {path}")
            return symbols

        num_records = _read_short(header[2:4])
        logger.debug(f"EMASTER: {num_records} records expected")

        for _ in range(num_records):
            rec = fh.read(record_size)
            if len(rec) < record_size:
                break

            si = SymbolInfo()
            si.file_num   = _read_byte(rec[2:3])
            si.num_fields = _read_byte(rec[6:7])
            si.symbol     = _read_str(rec[10:24])
            si.name       = _read_str(rec[31:47])
            si.time_frame = chr(rec[59]) if rec[59] else 'D'
            si.first_date = _mbf_date_to_date(rec[63:67])
            si.last_date  = _mbf_date_to_date(rec[71:75])

            if si.symbol:
                symbols.append(si)

    logger.info(f"EMASTER: parsed {len(symbols)} symbols")
    return symbols


def _read_xmaster(path: Path) -> List[SymbolInfo]:
    """
    قراءة ملف XMASTER (الفهرس الموسّع لـ MetaStock 7+، يدعم أكثر من 255 رمزاً).
    حجم كل سجل = 150 بايت.
    السجل الأول هو رأس الملف.
    """
    symbols: List[SymbolInfo] = []
    record_size = 150

    with open(path, 'rb') as fh:
        header = fh.read(record_size)
        if len(header) < record_size:
            logger.warning(f"XMASTER header too short: {path}")
            return symbols

        num_records = _read_short(header[2:4])
        logger.debug(f"XMASTER: {num_records} records expected")

        for _ in range(num_records):
            rec = fh.read(record_size)
            if len(rec) < record_size:
                break

            si = SymbolInfo()
            si.symbol     = _read_str(rec[1:16])
            si.name       = _read_str(rec[16:66])
            si.time_frame = chr(rec[66]) if rec[66] else 'D'
            si.num_fields = _read_byte(rec[67:68])
            si.file_num   = _read_short(rec[108:110])
            si.first_date = _mbf_date_to_date(rec[70:74])
            si.last_date  = _mbf_date_to_date(rec[78:82])

            if si.symbol:
                symbols.append(si)

    logger.info(f"XMASTER: parsed {len(symbols)} symbols")
    return symbols


def _read_master(path: Path) -> List[SymbolInfo]:
    """
    قراءة ملف MASTER (الفهرس القديم، حتى 255 رمزاً).
    حجم كل سجل = 53 بايت.
    """
    symbols: List[SymbolInfo] = []
    record_size = 53

    with open(path, 'rb') as fh:
        header = fh.read(record_size)
        if len(header) < record_size:
            return symbols

        num_records = _read_byte(header[2:3])

        for _ in range(num_records):
            rec = fh.read(record_size)
            if len(rec) < record_size:
                break

            si = SymbolInfo()
            si.file_num   = _read_byte(rec[0:1])
            si.time_frame = chr(rec[1]) if rec[1] else 'D'
            si.num_fields = _read_byte(rec[2:3])
            si.symbol     = _read_str(rec[3:17])
            si.first_date = _mbf_date_to_date(rec[17:21])
            si.last_date  = _mbf_date_to_date(rec[21:25])
            si.name       = _read_str(rec[25:41])

            if si.symbol:
                symbols.append(si)

    logger.info(f"MASTER: parsed {len(symbols)} symbols")
    return symbols


# ─────────────────────────────────────────────────────────────────────────────
# قراءة ملف DOP (أعمدة البيانات)
# ─────────────────────────────────────────────────────────────────────────────

_DEFAULT_COLUMNS = ['DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL', 'OI']

def _read_dop(dop_path: Path, num_fields: int) -> List[str]:
    """
    قراءة ملف DOP لمعرفة أسماء الأعمدة.
    إذا لم يوجد الملف يُستخدم الترتيب الافتراضي.
    """
    if not dop_path.exists():
        return _DEFAULT_COLUMNS[:num_fields]

    try:
        content = dop_path.read_text(encoding='latin-1', errors='replace')
        import re
        cols = re.findall(r'"([^"]+)"', content)
        if cols:
            return [c.upper() for c in cols]
    except Exception as e:
        logger.warning(f"Cannot read DOP file {dop_path}: {e}")

    return _DEFAULT_COLUMNS[:num_fields]


# ─────────────────────────────────────────────────────────────────────────────
# قراءة ملف DAT / MWD (بيانات الشموع)
# ─────────────────────────────────────────────────────────────────────────────

def _read_dat_file(
    dat_path: Path,
    si: SymbolInfo,
    dop_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    قراءة ملف DAT أو MWD وإرجاع DataFrame بالأعمدة:
    [time, symbol, timeframe, open, high, low, close, volume, open_interest]
    """
    if not dat_path.exists():
        logger.warning(f"DAT file not found: {dat_path}")
        return pd.DataFrame()

    file_size = dat_path.stat().st_size
    if file_size <= 28:
        logger.warning(f"DAT file too small (possibly corrupt): {dat_path}")
        return pd.DataFrame()

    # قراءة أسماء الأعمدة
    if dop_path and dop_path.exists():
        columns = _read_dop(dop_path, si.num_fields)
    else:
        columns = _DEFAULT_COLUMNS[:si.num_fields]

    rows = []
    field_size = 4  # كل حقل = 4 بايت MBF

    with open(dat_path, 'rb') as fh:
        max_recs  = _read_short(fh.read(2))
        last_rec  = _read_short(fh.read(2))
        fh.read(24)  # تخطي رأس الملف المتبقي

        num_candles = last_rec - 1
        logger.debug(
            f"DAT {dat_path.name}: max_recs={max_recs}, "
            f"last_rec={last_rec}, candles={num_candles}"
        )

        for _ in range(num_candles):
            raw_row = fh.read(si.num_fields * field_size)
            if len(raw_row) < si.num_fields * field_size:
                break

            row_data: Dict[str, object] = {}
            for i, col_name in enumerate(columns):
                chunk = raw_row[i * field_size: (i + 1) * field_size]
                if col_name == 'DATE':
                    row_data['date'] = _mbf_date_to_date(chunk)
                elif col_name == 'TIME':
                    row_data['time_hm'] = _mbf_time_to_time(chunk)
                elif col_name in ('OPEN',):
                    row_data['open'] = round(_mbf4_to_float(chunk), 4)
                elif col_name in ('HIGH',):
                    row_data['high'] = round(_mbf4_to_float(chunk), 4)
                elif col_name in ('LOW',):
                    row_data['low'] = round(_mbf4_to_float(chunk), 4)
                elif col_name in ('CLOSE',):
                    row_data['close'] = round(_mbf4_to_float(chunk), 4)
                elif col_name in ('VOL', 'VOLUME'):
                    row_data['volume'] = int(_mbf4_to_float(chunk))
                elif col_name in ('OI',):
                    row_data['open_interest'] = int(_mbf4_to_float(chunk))

            if 'date' not in row_data or row_data['date'] is None:
                continue

            # بناء الطابع الزمني
            d: date = row_data['date']
            hm = row_data.get('time_hm')
            if hm:
                dt = datetime(d.year, d.month, d.day, hm[0], hm[1], tzinfo=timezone.utc)
            else:
                dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)

            rows.append({
                'time':           dt,
                'symbol':         si.symbol.upper(),
                'timeframe':      _normalize_timeframe(si.time_frame),
                'open':           row_data.get('open', 0.0),
                'high':           row_data.get('high', 0.0),
                'low':            row_data.get('low', 0.0),
                'close':          row_data.get('close', 0.0),
                'volume':         row_data.get('volume', 0),
                'open_interest':  row_data.get('open_interest', 0),
                'source':         'metastock',
            })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df.sort_values('time', inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def _normalize_timeframe(tf_char: str) -> str:
    """
    تحويل رمز الإطار الزمني لـ MetaStock إلى الصيغة المستخدمة في النظام.
    D → 1d, W → 1w, M → 1M, I → 1m (intraday), Q → 15m, H → 1h
    """
    mapping = {
        'D': '1d',
        'W': '1w',
        'M': '1M',
        'I': '1m',
        'Q': '15m',
        'H': '1h',
        'T': '1m',   # tick → نعامله كـ 1m
    }
    return mapping.get(tf_char.upper(), '1d')


# ─────────────────────────────────────────────────────────────────────────────
# الواجهة الرئيسية: MetaStockParser
# ─────────────────────────────────────────────────────────────────────────────

class MetaStockParser:
    """
    محلل ملفات MetaStock الرئيسي.

    الاستخدام:
        parser = MetaStockParser('/path/to/metastock/dir')
        symbols = parser.list_symbols()
        df = parser.parse_symbol('TASI')
        all_df = parser.parse_all()
    """

    def __init__(self, data_dir: str | Path):
        self.data_dir = Path(data_dir)
        self._symbols: Optional[List[SymbolInfo]] = None

    # ── اكتشاف ملف الفهرس ──────────────────────────────────────────────────

    def _find_index_file(self) -> Tuple[Optional[Path], str]:
        """
        البحث عن ملف الفهرس بالأولوية: XMASTER > EMASTER > MASTER
        (غير حساس لحالة الأحرف)
        """
        candidates = {
            'XMASTER': None,
            'EMASTER': None,
            'MASTER':  None,
        }
        for f in self.data_dir.iterdir():
            upper = f.name.upper()
            if upper in candidates:
                candidates[upper] = f

        for name in ('XMASTER', 'EMASTER', 'MASTER'):
            if candidates[name]:
                return candidates[name], name

        return None, ''

    # ── قراءة الفهرس ───────────────────────────────────────────────────────

    def _load_symbols(self) -> List[SymbolInfo]:
        if self._symbols is not None:
            return self._symbols

        idx_path, idx_type = self._find_index_file()
        if idx_path is None:
            raise FileNotFoundError(
                f"لم يُعثر على ملف MASTER/EMASTER/XMASTER في: {self.data_dir}"
            )

        logger.info(f"قراءة فهرس MetaStock: {idx_path.name}")

        if idx_type == 'XMASTER':
            self._symbols = _read_xmaster(idx_path)
        elif idx_type == 'EMASTER':
            self._symbols = _read_emaster(idx_path)
        else:
            self._symbols = _read_master(idx_path)

        return self._symbols

    # ── واجهة عامة ─────────────────────────────────────────────────────────

    def list_symbols(self) -> List[Dict]:
        """إرجاع قائمة بجميع الرموز المتاحة."""
        symbols = self._load_symbols()
        return [
            {
                'symbol':     s.symbol,
                'name':       s.name,
                'timeframe':  _normalize_timeframe(s.time_frame),
                'file_num':   s.file_num,
                'first_date': str(s.first_date) if s.first_date else None,
                'last_date':  str(s.last_date) if s.last_date else None,
            }
            for s in symbols
        ]

    def parse_symbol(self, symbol: str) -> pd.DataFrame:
        """
        تحليل رمز واحد وإرجاع DataFrame.
        symbol: اسم الرمز (غير حساس لحالة الأحرف)
        """
        symbols = self._load_symbols()
        target = symbol.upper()
        si = next((s for s in symbols if s.symbol.upper() == target), None)
        if si is None:
            raise ValueError(f"الرمز '{symbol}' غير موجود في الفهرس")
        return self._parse_symbol_info(si)

    def parse_all(self) -> pd.DataFrame:
        """تحليل جميع الرموز وإرجاع DataFrame موحّد."""
        symbols = self._load_symbols()
        frames = []
        for si in symbols:
            try:
                df = self._parse_symbol_info(si)
                if not df.empty:
                    frames.append(df)
                    logger.success(
                        f"  ✓ {si.symbol}: {len(df)} شمعة "
                        f"({df['time'].min().date()} → {df['time'].max().date()})"
                    )
            except Exception as e:
                logger.error(f"  ✗ {si.symbol}: {e}")

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)
        logger.info(f"إجمالي الشموع المحللة: {len(combined):,}")
        return combined

    def _parse_symbol_info(self, si: SymbolInfo) -> pd.DataFrame:
        """تحليل رمز واحد بناءً على كائن SymbolInfo."""
        ext = 'DAT' if si.file_num <= 255 else 'MWD'
        dat_name = f'F{si.file_num}.{ext}'
        dop_name = f'F{si.file_num}.DOP'

        # البحث عن الملف بغض النظر عن حالة الأحرف
        dat_path = self._find_file(dat_name)
        dop_path = self._find_file(dop_name)

        if dat_path is None:
            logger.warning(f"ملف البيانات غير موجود: {dat_name}")
            return pd.DataFrame()

        # قراءة أسماء الأعمدة من DOP إن وُجد
        if dop_path:
            si.columns = _read_dop(dop_path, si.num_fields)

        return _read_dat_file(dat_path, si, dop_path)

    def _find_file(self, filename: str) -> Optional[Path]:
        """البحث عن ملف بغض النظر عن حالة الأحرف."""
        # محاولة مباشرة أولاً
        direct = self.data_dir / filename
        if direct.exists():
            return direct
        # بحث غير حساس لحالة الأحرف
        upper = filename.upper()
        lower = filename.lower()
        for f in self.data_dir.iterdir():
            if f.name.upper() == upper or f.name.lower() == lower:
                return f
        return None


# ─────────────────────────────────────────────────────────────────────────────
# دعم الملفات المضغوطة (ZIP)
# ─────────────────────────────────────────────────────────────────────────────

def extract_metastock_zip(zip_path: str | Path, extract_to: Optional[str | Path] = None) -> Path:
    """
    فك ضغط ملف ZIP يحتوي على بيانات MetaStock.
    يُرجع مسار المجلد المستخرج.

    يدعم:
      - ملف ZIP يحتوي على ملفات MetaStock مباشرة
      - ملف ZIP يحتوي على مجلد فرعي واحد
    """
    zip_path = Path(zip_path)
    if not zip_path.exists():
        raise FileNotFoundError(f"ملف ZIP غير موجود: {zip_path}")

    if extract_to is None:
        extract_to = Path(tempfile.mkdtemp(prefix='metastock_'))
    else:
        extract_to = Path(extract_to)
        extract_to.mkdir(parents=True, exist_ok=True)

    logger.info(f"فك ضغط {zip_path.name} → {extract_to}")

    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(extract_to)

    # إذا كان المحتوى في مجلد فرعي واحد، ننزل إليه
    contents = list(extract_to.iterdir())
    if len(contents) == 1 and contents[0].is_dir():
        return contents[0]

    return extract_to


def parse_metastock_zip(zip_path: str | Path) -> Tuple[List[Dict], pd.DataFrame]:
    """
    تحليل ملف ZIP يحتوي على بيانات MetaStock.
    يُرجع (قائمة الرموز, DataFrame الكامل).
    """
    with tempfile.TemporaryDirectory(prefix='metastock_') as tmp:
        data_dir = extract_metastock_zip(zip_path, tmp)
        parser = MetaStockParser(data_dir)
        symbols = parser.list_symbols()
        df = parser.parse_all()
    return symbols, df


def parse_metastock_dir(data_dir: str | Path) -> Tuple[List[Dict], pd.DataFrame]:
    """
    تحليل مجلد يحتوي على بيانات MetaStock.
    يُرجع (قائمة الرموز, DataFrame الكامل).
    """
    parser = MetaStockParser(data_dir)
    symbols = parser.list_symbols()
    df = parser.parse_all()
    return symbols, df

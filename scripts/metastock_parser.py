"""
MetaStock File Parser — Alpha-Engine2
======================================
يقرأ ملفات MetaStock الثنائية (MASTER / EMASTER / XMASTER + F{n}.DAT / F{n}.MWD)
ويحوّلها إلى DataFrame جاهز للإدخال في جدول market_data.ohlcv.

تنسيق MetaStock:
  - MASTER / EMASTER / XMASTER + F{n}.DAT / F{n}.MWD

ملاحظات Tadawul (2026-08):
  - رموز EMASTER بعد null؛ MASTER رمز@36؛ أسماء cp1256
  - Intraday غالباً 8 حقول. بعض الملفات: الحقل0 = padding صفر،
    ثم DATE, TIME, OPEN, HIGH, LOW, CLOSE, VOL
  - timeframe من مسار المجلد عند الحاجة
"""

from __future__ import annotations

import re
import struct
import zipfile
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger


def _mbf4_to_float(raw: bytes) -> float:
    if len(raw) < 4:
        return 0.0
    b0, b1, b2, b3 = raw[0], raw[1], raw[2], raw[3]
    exp = b3
    if exp == 0:
        return 0.0
    sign = (b2 & 0x80) >> 7
    mantissa = ((b2 & 0x7F) << 16) | (b1 << 8) | b0
    ieee_exp = exp - 2
    ieee_bits = (sign << 31) | (ieee_exp << 23) | mantissa
    try:
        return struct.unpack("f", struct.pack("I", ieee_bits & 0xFFFFFFFF))[0]
    except Exception:
        return 0.0


def _mbf_date_to_date(raw: bytes) -> Optional[date]:
    val = _mbf4_to_float(raw)
    if val == 0.0:
        return None
    try:
        d = int(val)
        year = 1900 + (d // 10000)
        month = (d % 10000) // 100
        day = d % 100
        if year < 1980 or year > 2100:
            return None
        if month < 1 or month > 12 or day < 1 or day > 31:
            return None
        return date(year, month, day)
    except (ValueError, OverflowError):
        return None


def _mbf_time_to_time(raw: bytes) -> Optional[Tuple[int, int]]:
    val = _mbf4_to_float(raw)
    if val == 0.0:
        return None
    try:
        t = int(round(val))
        if t >= 10000:  # HHMMSS
            hour = t // 10000
            minute = (t % 10000) // 100
        else:  # HHMM
            hour = t // 100
            minute = t % 100
        if hour > 23 or minute > 59:
            return None
        return (hour, minute)
    except (ValueError, OverflowError):
        return None


def _read_short(data: bytes) -> int:
    return struct.unpack("<H", data[:2])[0]


def _read_byte(data: bytes) -> int:
    return data[0] if data else 0


def _read_str(data: bytes, encoding: str = "latin-1") -> str:
    if not data:
        return ""
    i = 0
    while i < len(data) and data[i] == 0:
        i += 1
    data = data[i:]
    raw = data.split(b"\x00")[0]
    if not raw:
        return ""
    try:
        return raw.decode(encoding, errors="replace").strip()
    except Exception:
        return raw.decode("latin-1", errors="replace").strip()


def _read_name(data: bytes) -> str:
    if not data:
        return ""
    i = 0
    while i < len(data) and data[i] == 0:
        i += 1
    raw = data[i:].split(b"\x00")[0]
    if not raw:
        return ""
    for enc in ("cp1256", "utf-8", "latin-1"):
        try:
            s = raw.decode(enc).strip()
            if s:
                return s
        except Exception:
            continue
    return raw.decode("latin-1", errors="replace").strip()


class SymbolInfo:
    __slots__ = (
        "file_num",
        "num_fields",
        "symbol",
        "name",
        "time_frame",
        "first_date",
        "last_date",
        "columns",
    )

    def __init__(self):
        self.file_num: int = 0
        self.num_fields: int = 7
        self.symbol: str = ""
        self.name: str = ""
        self.time_frame: str = "D"
        self.first_date: Optional[date] = None
        self.last_date: Optional[date] = None
        self.columns: List[str] = ["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOL", "OI"]

    def __repr__(self) -> str:
        return (
            f"SymbolInfo(file_num={self.file_num}, symbol={self.symbol!r}, "
            f"name={self.name!r}, tf={self.time_frame}, fields={self.num_fields})"
        )


def _read_emaster(path: Path) -> List[SymbolInfo]:
    symbols: List[SymbolInfo] = []
    record_size = 192

    with open(path, "rb") as fh:
        header = fh.read(record_size)
        if len(header) < record_size:
            logger.warning(f"EMASTER header too short: {path}")
            return symbols

        num_records = _read_short(header[2:4])
        if num_records <= 0 or num_records > 255:
            alt = _read_short(header[0:2])
            if 0 < alt <= 255:
                num_records = alt
        logger.debug(f"EMASTER: {num_records} records expected")

        for _ in range(num_records):
            rec = fh.read(record_size)
            if len(rec) < record_size:
                break

            si = SymbolInfo()
            si.file_num = _read_byte(rec[2:3]) or _read_byte(rec[0:1])
            si.num_fields = _read_byte(rec[6:7]) or 7
            si.symbol = _read_str(rec[10:26])
            if not si.symbol:
                si.symbol = _read_str(rec[11:25])
            si.name = _read_name(rec[32:64])
            if not si.name:
                si.name = _read_name(rec[7:31])
            si.time_frame = chr(rec[59]) if rec[59] and 32 <= rec[59] < 127 else "D"
            si.first_date = _mbf_date_to_date(rec[63:67])
            si.last_date = _mbf_date_to_date(rec[71:75])

            if si.num_fields < 5 or si.num_fields > 16:
                si.num_fields = 7

            if si.symbol:
                symbols.append(si)

    logger.info(f"EMASTER: parsed {len(symbols)} symbols")
    return symbols


def _read_xmaster(path: Path) -> List[SymbolInfo]:
    symbols: List[SymbolInfo] = []
    record_size = 150

    with open(path, "rb") as fh:
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
            si.symbol = _read_str(rec[1:16])
            si.name = _read_name(rec[16:66])
            si.time_frame = chr(rec[66]) if rec[66] and 32 <= rec[66] < 127 else "D"
            si.num_fields = _read_byte(rec[67:68]) or 7
            si.file_num = _read_short(rec[108:110])
            si.first_date = _mbf_date_to_date(rec[70:74])
            si.last_date = _mbf_date_to_date(rec[78:82])

            if si.symbol:
                symbols.append(si)

    logger.info(f"XMASTER: parsed {len(symbols)} symbols")
    return symbols


def _read_master(path: Path) -> List[SymbolInfo]:
    symbols: List[SymbolInfo] = []
    record_size = 53

    with open(path, "rb") as fh:
        header = fh.read(record_size)
        if len(header) < record_size:
            return symbols

        num_records = _read_byte(header[2:3])
        if num_records <= 0:
            num_records = _read_short(header[0:2])
            if num_records <= 0 or num_records > 255:
                num_records = min(255, max(0, (path.stat().st_size // record_size) - 1))

        for _ in range(num_records):
            rec = fh.read(record_size)
            if len(rec) < record_size:
                break

            si = SymbolInfo()
            si.file_num = _read_byte(rec[0:1])
            si.time_frame = chr(rec[1]) if rec[1] and 32 <= rec[1] < 127 else "D"
            si.num_fields = _read_byte(rec[2:3]) or 7

            si.symbol = _read_str(rec[36:50])
            if not si.symbol:
                si.symbol = _read_str(rec[3:17])

            si.name = _read_name(rec[7:23])
            if not si.name:
                si.name = _read_name(rec[25:41])

            si.first_date = _mbf_date_to_date(rec[17:21])
            si.last_date = _mbf_date_to_date(rec[21:25])

            if si.num_fields < 5 or si.num_fields > 16:
                si.num_fields = 7

            if si.symbol:
                symbols.append(si)

    logger.info(f"MASTER: parsed {len(symbols)} symbols")
    return symbols


_DEFAULT_COLUMNS_7 = ["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOL", "OI"]
_DEFAULT_COLUMNS_8 = ["DATE", "TIME", "OPEN", "HIGH", "LOW", "CLOSE", "VOL", "OI"]
# بعض ملفات Tadawul Intraday: حقل أول صفر (padding)
_DEFAULT_COLUMNS_8_PADDED = [
    "PAD",
    "DATE",
    "TIME",
    "OPEN",
    "HIGH",
    "LOW",
    "CLOSE",
    "VOL",
]


def _default_columns(num_fields: int, padded: bool = False) -> List[str]:
    if num_fields >= 8:
        cols = list(_DEFAULT_COLUMNS_8_PADDED if padded else _DEFAULT_COLUMNS_8)
        while len(cols) < num_fields:
            cols.append(f"F{len(cols)}")
        return cols[:num_fields]
    cols = list(_DEFAULT_COLUMNS_7)
    while len(cols) < num_fields:
        cols.append(f"F{len(cols)}")
    return cols[:num_fields]


def _read_dop(dop_path: Path, num_fields: int) -> List[str]:
    if not dop_path.exists():
        return _default_columns(num_fields)

    try:
        content = dop_path.read_text(encoding="latin-1", errors="replace")
        cols = re.findall(r'"([^"]+)"', content)
        if cols:
            return [c.upper() for c in cols][:num_fields] or _default_columns(num_fields)
    except Exception as e:
        logger.warning(f"Cannot read DOP file {dop_path}: {e}")

    return _default_columns(num_fields)


def _infer_timeframe_from_path(path: Path) -> Optional[str]:
    text = str(path).lower().replace("\\", "/")
    rules = [
        (r"intraday[_-]?1\s*min|intraday_1min|/1min/", "1m"),
        (r"intraday[_-]?15\s*min|intraday_15min|/15min/", "15m"),
        (r"intraday[_-]?30\s*min|intraday_30min|/30min/", "30m"),
        (r"intraday[_-]?5\s*min|intraday_5min|/5min/", "5m"),
        (r"intraday[_-]?60|/1h/|intraday_1h", "1h"),
        (r"/daily/|\\daily\\", "1d"),
    ]
    for pattern, tf in rules:
        if re.search(pattern, text):
            return tf
    return None


def _normalize_timeframe(tf_char: str) -> str:
    mapping = {
        "D": "1d",
        "W": "1w",
        "M": "1M",
        "I": "1m",
        "Q": "15m",
        "H": "1h",
        "T": "1m",
    }
    return mapping.get((tf_char or "D").upper(), "1d")


def _detect_padded_layout(sample_rows: List[bytes], num_fields: int) -> bool:
    """
    إذا كان الحقل0 دائماً صفراً والحقل1 تاريخاً صالحاً → تخطيط padded.
    """
    if num_fields < 8 or not sample_rows:
        return False
    zero0 = 0
    date1 = 0
    for raw in sample_rows:
        if len(raw) < num_fields * 4:
            continue
        f0 = raw[0:4]
        f1 = raw[4:8]
        if _mbf4_to_float(f0) == 0.0:
            zero0 += 1
        if _mbf_date_to_date(f1) is not None:
            date1 += 1
    n = len(sample_rows)
    return n > 0 and zero0 >= max(1, n // 2) and date1 >= max(1, n // 2)


def _read_dat_file(
    dat_path: Path,
    si: SymbolInfo,
    dop_path: Optional[Path] = None,
    timeframe_override: Optional[str] = None,
) -> pd.DataFrame:
    if not dat_path.exists():
        logger.warning(f"DAT file not found: {dat_path}")
        return pd.DataFrame()

    file_size = dat_path.stat().st_size
    if file_size <= 28:
        logger.warning(f"DAT file too small (possibly corrupt): {dat_path}")
        return pd.DataFrame()

    field_size = 4
    rec_bytes = max(1, si.num_fields) * field_size
    tf = timeframe_override or _normalize_timeframe(si.time_frame)

    with open(dat_path, "rb") as fh:
        max_recs = _read_short(fh.read(2))
        last_rec = _read_short(fh.read(2))
        fh.read(24)

        num_candles = max(0, last_rec - 1)
        est = max(0, (file_size - 28) // rec_bytes)
        if num_candles <= 0 or num_candles > est + 5:
            num_candles = est

        # عيّنة لاكتشاف التخطيط
        sample: List[bytes] = []
        pos_after_header = fh.tell()
        for _ in range(min(20, num_candles)):
            raw = fh.read(si.num_fields * field_size)
            if len(raw) < si.num_fields * field_size:
                break
            sample.append(raw)
        fh.seek(pos_after_header)

        if dop_path and dop_path.exists():
            columns = _read_dop(dop_path, si.num_fields)
        else:
            columns = _default_columns(si.num_fields)

        padded = False
        if si.num_fields >= 8:
            padded = _detect_padded_layout(sample, si.num_fields)
            if padded:
                columns = _default_columns(si.num_fields, padded=True)
                logger.debug(f"DAT {dat_path.name}: detected padded DATE/TIME layout")
            elif "TIME" not in [c.upper() for c in columns]:
                columns = _default_columns(si.num_fields, padded=False)

        logger.debug(
            f"DAT {dat_path.name}: max_recs={max_recs}, last_rec={last_rec}, "
            f"candles={num_candles}, fields={si.num_fields}, cols={columns}, "
            f"tf={tf}, padded={padded}"
        )

        rows = []
        for _ in range(num_candles):
            raw_row = fh.read(si.num_fields * field_size)
            if len(raw_row) < si.num_fields * field_size:
                break

            row_data: Dict[str, object] = {}
            for i, col_name in enumerate(columns):
                if i >= si.num_fields:
                    break
                chunk = raw_row[i * field_size : (i + 1) * field_size]
                cu = col_name.upper()
                if cu in ("PAD", "F0", "UNUSED"):
                    continue
                if cu == "DATE":
                    row_data["date"] = _mbf_date_to_date(chunk)
                elif cu == "TIME":
                    row_data["time_hm"] = _mbf_time_to_time(chunk)
                elif cu == "OPEN":
                    row_data["open"] = round(_mbf4_to_float(chunk), 4)
                elif cu == "HIGH":
                    row_data["high"] = round(_mbf4_to_float(chunk), 4)
                elif cu == "LOW":
                    row_data["low"] = round(_mbf4_to_float(chunk), 4)
                elif cu == "CLOSE":
                    row_data["close"] = round(_mbf4_to_float(chunk), 4)
                elif cu in ("VOL", "VOLUME"):
                    row_data["volume"] = int(_mbf4_to_float(chunk))
                elif cu in ("OI", "OPENINTEREST", "OPEN_INTEREST"):
                    row_data["open_interest"] = int(_mbf4_to_float(chunk))

            if "date" not in row_data or row_data["date"] is None:
                continue

            d: date = row_data["date"]  # type: ignore
            hm = row_data.get("time_hm")
            if hm:
                dt = datetime(d.year, d.month, d.day, hm[0], hm[1], tzinfo=timezone.utc)
            else:
                dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)

            o = float(row_data.get("open", 0.0) or 0.0)
            h = float(row_data.get("high", 0.0) or 0.0)
            low = float(row_data.get("low", 0.0) or 0.0)
            c = float(row_data.get("close", 0.0) or 0.0)
            # تخطّي صفوف بلا سعر منطقي
            if o <= 0 and h <= 0 and low <= 0 and c <= 0:
                continue

            rows.append(
                {
                    "time": dt,
                    "symbol": si.symbol.upper(),
                    "timeframe": tf,
                    "open": o,
                    "high": h,
                    "low": low,
                    "close": c,
                    "volume": int(row_data.get("volume", 0) or 0),
                    "open_interest": int(row_data.get("open_interest", 0) or 0),
                    "source": "metastock",
                }
            )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df.sort_values("time", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


class MetaStockParser:
    def __init__(self, data_dir: str | Path, timeframe_override: Optional[str] = None):
        self.data_dir = Path(data_dir)
        self._symbols: Optional[List[SymbolInfo]] = None
        self.timeframe_override = timeframe_override or _infer_timeframe_from_path(
            self.data_dir
        )

    def _find_index_file(self) -> Tuple[Optional[Path], str]:
        candidates = {"XMASTER": None, "EMASTER": None, "MASTER": None}
        for f in self.data_dir.iterdir():
            upper = f.name.upper()
            if upper in candidates:
                candidates[upper] = f
        for name in ("XMASTER", "EMASTER", "MASTER"):
            if candidates[name]:
                return candidates[name], name
        return None, ""

    def _load_symbols(self) -> List[SymbolInfo]:
        if self._symbols is not None:
            return self._symbols

        idx_path, idx_type = self._find_index_file()
        if idx_path is None:
            raise FileNotFoundError(
                f"لم يُعثر على ملف MASTER/EMASTER/XMASTER في: {self.data_dir}"
            )

        logger.info(f"قراءة فهرس MetaStock: {idx_path.name}")

        if idx_type == "XMASTER":
            self._symbols = _read_xmaster(idx_path)
        elif idx_type == "EMASTER":
            self._symbols = _read_emaster(idx_path)
            if not self._symbols:
                master_path = None
                for f in self.data_dir.iterdir():
                    if f.name.upper() == "MASTER":
                        master_path = f
                        break
                if master_path:
                    logger.warning("EMASTER أعاد 0 رموز — محاولة MASTER كاحتياط")
                    self._symbols = _read_master(master_path)
        else:
            self._symbols = _read_master(idx_path)

        return self._symbols

    def list_symbols(self) -> List[Dict]:
        symbols = self._load_symbols()
        tf_default = self.timeframe_override
        return [
            {
                "symbol": s.symbol,
                "name": s.name,
                "timeframe": tf_default or _normalize_timeframe(s.time_frame),
                "file_num": s.file_num,
                "first_date": str(s.first_date) if s.first_date else None,
                "last_date": str(s.last_date) if s.last_date else None,
            }
            for s in symbols
        ]

    def parse_symbol(self, symbol: str) -> pd.DataFrame:
        symbols = self._load_symbols()
        target = symbol.upper()
        si = next((s for s in symbols if s.symbol.upper() == target), None)
        if si is None:
            raise ValueError(f"الرمز '{symbol}' غير موجود في الفهرس")
        return self._parse_symbol_info(si)

    def parse_all(self) -> pd.DataFrame:
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
        ext = "DAT" if si.file_num <= 255 else "MWD"
        dat_name = f"F{si.file_num}.{ext}"
        dop_name = f"F{si.file_num}.DOP"
        dat_path = self._find_file(dat_name)
        dop_path = self._find_file(dop_name)
        if dat_path is None:
            logger.warning(f"ملف البيانات غير موجود: {dat_name}")
            return pd.DataFrame()
        if dop_path:
            si.columns = _read_dop(dop_path, si.num_fields)
        return _read_dat_file(
            dat_path,
            si,
            dop_path,
            timeframe_override=self.timeframe_override,
        )

    def _find_file(self, filename: str) -> Optional[Path]:
        direct = self.data_dir / filename
        if direct.exists():
            return direct
        upper = filename.upper()
        for f in self.data_dir.iterdir():
            if f.name.upper() == upper:
                return f
        return None


def extract_metastock_zip(
    zip_path: str | Path, extract_to: Optional[str | Path] = None
) -> Path:
    zip_path = Path(zip_path)
    if not zip_path.exists():
        raise FileNotFoundError(f"ملف ZIP غير موجود: {zip_path}")
    if extract_to is None:
        extract_to = Path(tempfile.mkdtemp(prefix="metastock_"))
    else:
        extract_to = Path(extract_to)
        extract_to.mkdir(parents=True, exist_ok=True)
    logger.info(f"فك ضغط {zip_path.name} → {extract_to}")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_to)
    contents = list(extract_to.iterdir())
    if len(contents) == 1 and contents[0].is_dir():
        return contents[0]
    return extract_to


def parse_metastock_zip(zip_path: str | Path) -> Tuple[List[Dict], pd.DataFrame]:
    with tempfile.TemporaryDirectory(prefix="metastock_") as tmp:
        data_dir = extract_metastock_zip(zip_path, tmp)
        parser = MetaStockParser(data_dir)
        symbols = parser.list_symbols()
        df = parser.parse_all()
    return symbols, df


def parse_metastock_dir(data_dir: str | Path) -> Tuple[List[Dict], pd.DataFrame]:
    parser = MetaStockParser(data_dir)
    symbols = parser.list_symbols()
    df = parser.parse_all()
    return symbols, df

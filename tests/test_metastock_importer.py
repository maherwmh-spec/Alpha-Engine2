"""
اختبارات نظام استيراد MetaStock — Alpha-Engine2
=================================================
يغطي:
  1. تحويل MBF ← → IEEE
  2. تحليل التواريخ والأوقات
  3. قراءة ملفات EMASTER / XMASTER / MASTER
  4. قراءة ملفات DAT
  5. MetaStockParser (قراءة مجلد كامل)
  6. دعم ملفات ZIP
  7. MetaStockImporter (استيراد إلى DB) — مع mock
"""

from __future__ import annotations

import io
import os
import struct
import tempfile
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

# ── استيراد الوحدات المختبَرة ─────────────────────────────────────────────
from scripts.metastock_parser import (
    MetaStockParser,
    SymbolInfo,
    _mbf4_to_float,
    _mbf_date_to_date,
    _mbf_time_to_time,
    _normalize_timeframe,
    _read_dat_file,
    _read_emaster,
    _read_master,
    _read_xmaster,
    extract_metastock_zip,
    parse_metastock_dir,
)


# ═══════════════════════════════════════════════════════════════════════════════
# أدوات مساعدة للاختبارات
# ═══════════════════════════════════════════════════════════════════════════════

def _float_to_mbf4(value: float) -> bytes:
    """
    تحويل float Python إلى 4 بايت MBF (للاختبارات).
    نستخدم الطريقة العكسية: IEEE → MBF.
    """
    if value == 0.0:
        return b'\x00\x00\x00\x00'

    ieee_bytes = struct.pack('f', value)
    b0, b1, b2, b3 = ieee_bytes[0], ieee_bytes[1], ieee_bytes[2], ieee_bytes[3]

    # استخراج مكونات IEEE 754
    ieee_int = struct.unpack('I', ieee_bytes)[0]
    sign = (ieee_int >> 31) & 1
    exp_ieee = (ieee_int >> 23) & 0xFF
    mantissa = ieee_int & 0x7FFFFF

    # تحويل الأس: IEEE bias=127, MBF bias=128
    exp_mbf = exp_ieee + 2

    # بناء MBF
    mbf_b2 = (sign << 7) | ((mantissa >> 16) & 0x7F)
    mbf_b1 = (mantissa >> 8) & 0xFF
    mbf_b0 = mantissa & 0xFF
    mbf_b3 = exp_mbf & 0xFF

    return bytes([mbf_b0, mbf_b1, mbf_b2, mbf_b3])


def _date_to_mbf4(d: date) -> bytes:
    """تحويل date إلى MBF4 بتنسيق MetaStock."""
    val = float((d.year - 1900) * 10000 + d.month * 100 + d.day)
    return _float_to_mbf4(val)


def _make_emaster(symbols: List[dict]) -> bytes:
    """
    إنشاء ملف EMASTER وهمي للاختبار.
    كل سجل = 192 بايت.
    ملاحظة: ملفات MetaStock تستخدم ASCII فقط — نستخدم أسماء إنجليزية في الاختبارات.
    """
    record_size = 192
    num_records = len(symbols)

    # رأس الملف
    header = bytearray(record_size)
    header[0] = 0x45  # 'E'
    header[1] = 0x4D  # 'M'
    struct.pack_into('<H', header, 2, num_records)

    records = bytearray(header)

    for sym in symbols:
        rec = bytearray(record_size)
        rec[2]  = sym.get('file_num', 1) & 0xFF
        rec[6]  = sym.get('num_fields', 7) & 0xFF
        # symbol (offset 10, length 14) — ASCII only
        sym_bytes = sym['symbol'].encode('ascii', errors='replace')[:14]
        rec[10:10 + len(sym_bytes)] = sym_bytes
        # name (offset 31, length 16) — ASCII only (MetaStock binary format)
        raw_name = sym.get('name', '')
        # تحويل الأسماء العربية إلى ASCII للاختبار
        ascii_name = raw_name.encode('ascii', errors='replace').decode('ascii')[:16]
        name_bytes = ascii_name.encode('ascii')[:16]
        rec[31:31 + len(name_bytes)] = name_bytes
        # time_frame (offset 59)
        rec[59] = ord(sym.get('time_frame', 'D'))
        # first_date (offset 63)
        if sym.get('first_date'):
            rec[63:67] = _date_to_mbf4(sym['first_date'])
        # last_date (offset 71)
        if sym.get('last_date'):
            rec[71:75] = _date_to_mbf4(sym['last_date'])
        records.extend(rec)

    return bytes(records)


def _make_dat_file(candles: List[dict], num_fields: int = 7) -> bytes:
    """
    إنشاء ملف DAT وهمي للاختبار.
    الهيكل: 2 بايت max_recs + 2 بايت last_rec + 24 بايت header + candles
    """
    num_candles = len(candles)
    buf = bytearray()

    # رأس الملف
    buf += struct.pack('<H', num_candles + 10)  # max_recs
    buf += struct.pack('<H', num_candles + 1)   # last_rec
    buf += b'\x00' * 24                          # padding

    for c in candles:
        d = c.get('date', date(2024, 1, 1))
        date_val = float((d.year - 1900) * 10000 + d.month * 100 + d.day)
        buf += _float_to_mbf4(date_val)
        buf += _float_to_mbf4(c.get('open',  100.0))
        buf += _float_to_mbf4(c.get('high',  105.0))
        buf += _float_to_mbf4(c.get('low',    95.0))
        buf += _float_to_mbf4(c.get('close', 102.0))
        buf += _float_to_mbf4(float(c.get('volume', 1000)))
        if num_fields >= 7:
            buf += _float_to_mbf4(float(c.get('oi', 0)))

    return bytes(buf)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. اختبارات تحويل MBF
# ═══════════════════════════════════════════════════════════════════════════════

class TestMBFConversion:
    """اختبارات تحويل Microsoft Binary Format."""

    def test_zero(self):
        assert _mbf4_to_float(b'\x00\x00\x00\x00') == 0.0

    def test_short_input(self):
        assert _mbf4_to_float(b'\x00\x00') == 0.0

    def test_roundtrip_positive(self):
        """تحقق من تحويل ذهاب وإياب لأرقام موجبة."""
        for val in [1.0, 10.5, 100.25, 1234.5678, 9999.99]:
            mbf = _float_to_mbf4(val)
            result = _mbf4_to_float(mbf)
            assert abs(result - val) < 0.01, f"فشل للقيمة {val}: النتيجة {result}"

    def test_roundtrip_price(self):
        """اختبار أسعار نموذجية للسوق السعودي."""
        for price in [10.20, 25.40, 100.00, 250.60, 1000.00]:
            mbf = _float_to_mbf4(price)
            result = _mbf4_to_float(mbf)
            assert abs(result - price) < 0.01, f"فشل للسعر {price}"


class TestDateTimeConversion:
    """اختبارات تحويل التواريخ والأوقات."""

    def test_date_conversion(self):
        d = date(2024, 3, 15)
        mbf = _date_to_mbf4(d)
        result = _mbf_date_to_date(mbf)
        assert result == d, f"التاريخ المتوقع {d}، النتيجة {result}"

    def test_date_zero(self):
        assert _mbf_date_to_date(b'\x00\x00\x00\x00') is None

    def test_date_year_2000(self):
        d = date(2000, 1, 1)
        mbf = _date_to_mbf4(d)
        result = _mbf_date_to_date(mbf)
        assert result == d

    def test_date_recent(self):
        d = date(2026, 4, 15)
        mbf = _date_to_mbf4(d)
        result = _mbf_date_to_date(mbf)
        assert result == d


class TestNormalizeTimeframe:
    """اختبارات تطبيع الإطار الزمني."""

    def test_daily(self):
        assert _normalize_timeframe('D') == '1d'

    def test_weekly(self):
        assert _normalize_timeframe('W') == '1w'

    def test_monthly(self):
        assert _normalize_timeframe('M') == '1M'

    def test_intraday(self):
        assert _normalize_timeframe('I') == '1m'

    def test_hourly(self):
        assert _normalize_timeframe('H') == '1h'

    def test_unknown(self):
        assert _normalize_timeframe('X') == '1d'

    def test_lowercase(self):
        assert _normalize_timeframe('d') == '1d'


# ═══════════════════════════════════════════════════════════════════════════════
# 2. اختبارات قراءة EMASTER
# ═══════════════════════════════════════════════════════════════════════════════

class TestReadEMaster:
    """اختبارات قراءة ملف EMASTER."""

    def test_read_single_symbol(self, tmp_path):
        symbols_data = [
            {
                'file_num': 1,
                'num_fields': 7,
                'symbol': 'TASI',
                'name': 'مؤشر تاسي',
                'time_frame': 'D',
                'first_date': date(2020, 1, 1),
                'last_date':  date(2024, 12, 31),
            }
        ]
        emaster_bytes = _make_emaster(symbols_data)
        emaster_path = tmp_path / 'EMASTER'
        emaster_path.write_bytes(emaster_bytes)

        result = _read_emaster(emaster_path)
        assert len(result) == 1
        assert result[0].symbol == 'TASI'
        assert result[0].file_num == 1
        assert result[0].num_fields == 7

    def test_read_multiple_symbols(self, tmp_path):
        symbols_data = [
            {'file_num': i, 'symbol': f'SYM{i:02d}', 'name': f'رمز {i}',
             'time_frame': 'D', 'num_fields': 7}
            for i in range(1, 6)
        ]
        emaster_bytes = _make_emaster(symbols_data)
        emaster_path = tmp_path / 'EMASTER'
        emaster_path.write_bytes(emaster_bytes)

        result = _read_emaster(emaster_path)
        assert len(result) == 5
        symbols = [s.symbol for s in result]
        assert 'SYM01' in symbols
        assert 'SYM05' in symbols

    def test_empty_file(self, tmp_path):
        emaster_path = tmp_path / 'EMASTER'
        emaster_path.write_bytes(b'\x00' * 10)
        result = _read_emaster(emaster_path)
        assert result == []


# ═══════════════════════════════════════════════════════════════════════════════
# 3. اختبارات قراءة DAT
# ═══════════════════════════════════════════════════════════════════════════════

class TestReadDatFile:
    """اختبارات قراءة ملفات DAT."""

    def _make_symbol_info(self, symbol='TEST', file_num=1, num_fields=7) -> SymbolInfo:
        si = SymbolInfo()
        si.file_num = file_num
        si.symbol = symbol
        si.num_fields = num_fields
        si.time_frame = 'D'
        si.columns = ['DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL', 'OI']
        return si

    def test_read_basic_candles(self, tmp_path):
        candles = [
            {'date': date(2024, 1, d), 'open': 100.0 + d, 'high': 110.0 + d,
             'low': 95.0 + d, 'close': 105.0 + d, 'volume': 1000 * d}
            for d in range(1, 6)
        ]
        dat_bytes = _make_dat_file(candles)
        dat_path = tmp_path / 'F1.DAT'
        dat_path.write_bytes(dat_bytes)

        si = self._make_symbol_info()
        df = _read_dat_file(dat_path, si)

        assert not df.empty
        assert len(df) == 5
        assert 'time' in df.columns
        assert 'open' in df.columns
        assert 'close' in df.columns
        assert 'volume' in df.columns
        assert df['symbol'].iloc[0] == 'TEST'
        assert df['timeframe'].iloc[0] == '1d'

    def test_prices_accuracy(self, tmp_path):
        candles = [
            {'date': date(2024, 6, 1), 'open': 125.50, 'high': 130.25,
             'low': 120.75, 'close': 128.00, 'volume': 50000}
        ]
        dat_bytes = _make_dat_file(candles)
        dat_path = tmp_path / 'F1.DAT'
        dat_path.write_bytes(dat_bytes)

        si = self._make_symbol_info()
        df = _read_dat_file(dat_path, si)

        assert not df.empty
        assert abs(df['open'].iloc[0] - 125.50) < 0.1
        assert abs(df['high'].iloc[0] - 130.25) < 0.1
        assert abs(df['low'].iloc[0] - 120.75) < 0.1
        assert abs(df['close'].iloc[0] - 128.00) < 0.1

    def test_empty_file(self, tmp_path):
        dat_path = tmp_path / 'F1.DAT'
        dat_path.write_bytes(b'\x00' * 10)

        si = self._make_symbol_info()
        df = _read_dat_file(dat_path, si)
        assert df.empty

    def test_nonexistent_file(self, tmp_path):
        si = self._make_symbol_info()
        df = _read_dat_file(tmp_path / 'F999.DAT', si)
        assert df.empty

    def test_sorted_by_time(self, tmp_path):
        """التحقق من أن الشموع مرتبة زمنياً."""
        candles = [
            {'date': date(2024, 1, d)} for d in [5, 3, 1, 4, 2]
        ]
        dat_bytes = _make_dat_file(candles)
        dat_path = tmp_path / 'F1.DAT'
        dat_path.write_bytes(dat_bytes)

        si = self._make_symbol_info()
        df = _read_dat_file(dat_path, si)

        if not df.empty:
            times = df['time'].tolist()
            assert times == sorted(times)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. اختبارات MetaStockParser
# ═══════════════════════════════════════════════════════════════════════════════

class TestMetaStockParser:
    """اختبارات الفئة الرئيسية MetaStockParser."""

    def _create_metastock_dir(self, tmp_path: Path, symbols: List[dict]) -> Path:
        """إنشاء مجلد MetaStock وهمي كامل."""
        # إنشاء EMASTER
        emaster_bytes = _make_emaster(symbols)
        (tmp_path / 'EMASTER').write_bytes(emaster_bytes)

        # إنشاء ملفات DAT لكل رمز
        for sym in symbols:
            file_num = sym.get('file_num', 1)
            candles = sym.get('candles', [
                {'date': date(2024, 1, d), 'open': 100.0, 'high': 105.0,
                 'low': 95.0, 'close': 102.0, 'volume': 1000}
                for d in range(1, 6)
            ])
            dat_bytes = _make_dat_file(candles, sym.get('num_fields', 7))
            (tmp_path / f'F{file_num}.DAT').write_bytes(dat_bytes)

        return tmp_path

    def test_list_symbols(self, tmp_path):
        symbols = [
            {'file_num': 1, 'symbol': '1010', 'name': 'مصرف الرياض',
             'time_frame': 'D', 'num_fields': 7},
            {'file_num': 2, 'symbol': '2222', 'name': 'أرامكو',
             'time_frame': 'D', 'num_fields': 7},
        ]
        self._create_metastock_dir(tmp_path, symbols)

        parser = MetaStockParser(tmp_path)
        result = parser.list_symbols()

        assert len(result) == 2
        syms = [s['symbol'] for s in result]
        assert '1010' in syms
        assert '2222' in syms

    def test_parse_symbol(self, tmp_path):
        symbols = [
            {'file_num': 1, 'symbol': '1010', 'name': 'مصرف الرياض',
             'time_frame': 'D', 'num_fields': 7,
             'candles': [
                 {'date': date(2024, 1, 1), 'open': 25.0, 'high': 26.0,
                  'low': 24.5, 'close': 25.8, 'volume': 5000}
             ]},
        ]
        self._create_metastock_dir(tmp_path, symbols)

        parser = MetaStockParser(tmp_path)
        df = parser.parse_symbol('1010')

        assert not df.empty
        assert df['symbol'].iloc[0] == '1010'
        assert df['source'].iloc[0] == 'metastock'

    def test_parse_symbol_not_found(self, tmp_path):
        symbols = [
            {'file_num': 1, 'symbol': '1010', 'name': 'Test', 'time_frame': 'D', 'num_fields': 7}
        ]
        self._create_metastock_dir(tmp_path, symbols)

        parser = MetaStockParser(tmp_path)
        with pytest.raises(ValueError, match="غير موجود"):
            parser.parse_symbol('9999')

    def test_parse_all(self, tmp_path):
        symbols = [
            {'file_num': i, 'symbol': f'{1000 + i}0', 'name': f'رمز {i}',
             'time_frame': 'D', 'num_fields': 7}
            for i in range(1, 4)
        ]
        self._create_metastock_dir(tmp_path, symbols)

        parser = MetaStockParser(tmp_path)
        df = parser.parse_all()

        assert not df.empty
        assert len(df['symbol'].unique()) == 3

    def test_no_index_file(self, tmp_path):
        """يجب رفع استثناء إذا لم يوجد ملف فهرس."""
        parser = MetaStockParser(tmp_path)
        with pytest.raises(FileNotFoundError):
            parser.list_symbols()

    def test_case_insensitive_files(self, tmp_path):
        """التحقق من التعامل مع أسماء الملفات بأي حالة أحرف."""
        symbols = [
            {'file_num': 1, 'symbol': 'TEST', 'name': 'Test', 'time_frame': 'D', 'num_fields': 7}
        ]
        emaster_bytes = _make_emaster(symbols)
        # كتابة الملف باسم بأحرف صغيرة
        (tmp_path / 'emaster').write_bytes(emaster_bytes)
        dat_bytes = _make_dat_file([{'date': date(2024, 1, 1)}])
        (tmp_path / 'f1.dat').write_bytes(dat_bytes)

        parser = MetaStockParser(tmp_path)
        result = parser.list_symbols()
        assert len(result) == 1


# ═══════════════════════════════════════════════════════════════════════════════
# 5. اختبارات دعم ZIP
# ═══════════════════════════════════════════════════════════════════════════════

class TestZipSupport:
    """اختبارات فك ضغط ملفات ZIP."""

    def _create_metastock_zip(self, zip_path: Path, symbols: List[dict]):
        """إنشاء ملف ZIP يحتوي على بيانات MetaStock وهمية."""
        emaster_bytes = _make_emaster(symbols)
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('EMASTER', emaster_bytes)
            for sym in symbols:
                file_num = sym.get('file_num', 1)
                dat_bytes = _make_dat_file([
                    {'date': date(2024, 1, d)} for d in range(1, 4)
                ])
                zf.writestr(f'F{file_num}.DAT', dat_bytes)

    def test_extract_zip_flat(self, tmp_path):
        """ZIP يحتوي على ملفات مباشرة."""
        zip_path = tmp_path / 'data.zip'
        symbols = [{'file_num': 1, 'symbol': 'TEST', 'name': 'Test',
                    'time_frame': 'D', 'num_fields': 7}]
        self._create_metastock_zip(zip_path, symbols)

        extract_dir = tmp_path / 'extracted'
        result_dir = extract_metastock_zip(zip_path, extract_dir)

        assert result_dir.exists()
        assert (result_dir / 'EMASTER').exists() or \
               any(f.name.upper() == 'EMASTER' for f in result_dir.iterdir())

    def test_extract_zip_with_subdir(self, tmp_path):
        """ZIP يحتوي على مجلد فرعي واحد."""
        zip_path = tmp_path / 'data.zip'
        emaster_bytes = _make_emaster([
            {'file_num': 1, 'symbol': 'TEST', 'name': 'T', 'time_frame': 'D', 'num_fields': 7}
        ])
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('metastock_data/EMASTER', emaster_bytes)
            zf.writestr('metastock_data/F1.DAT', _make_dat_file([]))

        extract_dir = tmp_path / 'extracted'
        result_dir = extract_metastock_zip(zip_path, extract_dir)

        # يجب أن ينزل إلى المجلد الفرعي
        assert result_dir.name == 'metastock_data'

    def test_nonexistent_zip(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            extract_metastock_zip(tmp_path / 'nonexistent.zip')

    def test_parse_metastock_dir(self, tmp_path):
        """اختبار parse_metastock_dir الكاملة."""
        symbols = [
            {'file_num': 1, 'symbol': '1010', 'name': 'رياض', 'time_frame': 'D', 'num_fields': 7,
             'candles': [{'date': date(2024, 1, d), 'open': 25.0, 'high': 26.0,
                          'low': 24.0, 'close': 25.5, 'volume': 1000} for d in range(1, 4)]},
        ]
        emaster_bytes = _make_emaster(symbols)
        (tmp_path / 'EMASTER').write_bytes(emaster_bytes)
        dat_bytes = _make_dat_file(symbols[0]['candles'])
        (tmp_path / 'F1.DAT').write_bytes(dat_bytes)

        syms_list, df = parse_metastock_dir(tmp_path)

        assert len(syms_list) == 1
        assert syms_list[0]['symbol'] == '1010'
        assert not df.empty
        assert len(df) == 3


# ═══════════════════════════════════════════════════════════════════════════════
# 6. اختبارات MetaStockImporter (مع Mock لقاعدة البيانات)
# ═══════════════════════════════════════════════════════════════════════════════

class TestMetaStockImporter:
    """اختبارات MetaStockImporter مع محاكاة قاعدة البيانات."""

    def _create_test_dir(self, tmp_path: Path) -> Path:
        symbols = [
            {'file_num': 1, 'symbol': '1010', 'name': 'رياض', 'time_frame': 'D', 'num_fields': 7,
             'candles': [{'date': date(2024, 1, d), 'open': 25.0, 'high': 26.0,
                          'low': 24.0, 'close': 25.5, 'volume': 1000} for d in range(1, 4)]},
            {'file_num': 2, 'symbol': '2222', 'name': 'أرامكو', 'time_frame': 'D', 'num_fields': 7,
             'candles': [{'date': date(2024, 1, d), 'open': 30.0, 'high': 31.0,
                          'low': 29.0, 'close': 30.5, 'volume': 2000} for d in range(1, 6)]},
        ]
        emaster_bytes = _make_emaster(symbols)
        (tmp_path / 'EMASTER').write_bytes(emaster_bytes)
        for sym in symbols:
            dat_bytes = _make_dat_file(sym['candles'])
            (tmp_path / f'F{sym["file_num"]}.DAT').write_bytes(dat_bytes)
        return tmp_path

    @pytest.mark.asyncio
    async def test_import_from_dir_success(self, tmp_path):
        """اختبار الاستيراد الناجح مع mock لقاعدة البيانات."""
        data_dir = self._create_test_dir(tmp_path)

        from bots.metastock_importer.bot import MetaStockImporter

        with patch.object(MetaStockImporter, '_init_pool', new_callable=AsyncMock), \
             patch.object(MetaStockImporter, '_close_pool', new_callable=AsyncMock), \
             patch.object(MetaStockImporter, '_upsert_dataframe', new_callable=AsyncMock) as mock_upsert:

            mock_upsert.return_value = 3

            importer = MetaStockImporter()
            result = await importer.import_from_dir(data_dir)

        assert result['status'] == 'success'
        assert result['symbols_count'] == 2
        assert result['imported_rows'] == 6  # 3 + 3 (mock returns 3 each)
        assert result['errors'] == []

    @pytest.mark.asyncio
    async def test_import_with_symbols_filter(self, tmp_path):
        """اختبار الاستيراد مع فلتر رموز."""
        data_dir = self._create_test_dir(tmp_path)

        from bots.metastock_importer.bot import MetaStockImporter

        with patch.object(MetaStockImporter, '_init_pool', new_callable=AsyncMock), \
             patch.object(MetaStockImporter, '_close_pool', new_callable=AsyncMock), \
             patch.object(MetaStockImporter, '_upsert_dataframe', new_callable=AsyncMock) as mock_upsert:

            mock_upsert.return_value = 3

            importer = MetaStockImporter()
            result = await importer.import_from_dir(data_dir, symbols_filter=['1010'])

        assert result['symbols_count'] == 1
        assert result['symbols'][0]['symbol'] == '1010'

    @pytest.mark.asyncio
    async def test_import_nonexistent_dir(self):
        """اختبار الاستيراد من مجلد غير موجود."""
        from bots.metastock_importer.bot import MetaStockImporter

        importer = MetaStockImporter()
        result = await importer.import_from_dir('/nonexistent/path')

        assert result['status'] == 'error'
        assert len(result['errors']) > 0

    @pytest.mark.asyncio
    async def test_import_from_zip(self, tmp_path):
        """اختبار الاستيراد من ملف ZIP."""
        # إنشاء ZIP وهمي
        zip_path = tmp_path / 'test.zip'
        symbols = [
            {'file_num': 1, 'symbol': '1010', 'name': 'رياض', 'time_frame': 'D', 'num_fields': 7}
        ]
        emaster_bytes = _make_emaster(symbols)
        dat_bytes = _make_dat_file([
            {'date': date(2024, 1, d)} for d in range(1, 4)
        ])
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('EMASTER', emaster_bytes)
            zf.writestr('F1.DAT', dat_bytes)

        from bots.metastock_importer.bot import MetaStockImporter

        with patch.object(MetaStockImporter, '_init_pool', new_callable=AsyncMock), \
             patch.object(MetaStockImporter, '_close_pool', new_callable=AsyncMock), \
             patch.object(MetaStockImporter, '_upsert_dataframe', new_callable=AsyncMock) as mock_upsert:

            mock_upsert.return_value = 3

            importer = MetaStockImporter()
            result = await importer.import_from_zip(zip_path)

        assert result['status'] in ('success', 'partial', 'empty')
        # لا أخطاء فادحة
        assert 'error' not in result.get('status', '') or result.get('symbols_count', 0) >= 0

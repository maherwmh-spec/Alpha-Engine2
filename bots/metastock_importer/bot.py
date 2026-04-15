"""
MetaStock Importer Bot — Alpha-Engine2
========================================
يستورد بيانات MetaStock (ثنائية أو ZIP) إلى جدول market_data.ohlcv
في TimescaleDB.

يدعم:
  - مجلد MetaStock مباشرة
  - ملف ZIP يحتوي على بيانات MetaStock
  - استيراد رمز واحد أو جميع الرموز
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import asyncpg
import pandas as pd
from loguru import logger

from config.config_manager import config
from scripts.metastock_parser import (
    MetaStockParser,
    extract_metastock_zip,
    parse_metastock_dir,
    parse_metastock_zip,
)


class MetaStockImporter:
    """
    يستورد بيانات MetaStock إلى TimescaleDB.

    الاستخدام:
        importer = MetaStockImporter()
        result = await importer.import_from_zip('/path/to/data.zip')
        result = await importer.import_from_dir('/path/to/metastock/')
    """

    # حجم الدفعة عند الإدراج الجماعي
    BATCH_SIZE = 500

    def __init__(self):
        self.dsn  = config.get_asyncpg_dsn()
        self.pool: Optional[asyncpg.Pool] = None

    # ── إدارة الاتصال ───────────────────────────────────────────────────────

    async def _init_pool(self):
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                self.dsn, min_size=1, max_size=5
            )

    async def _close_pool(self):
        if self.pool:
            await self.pool.close()
            self.pool = None

    # ── الواجهة العامة ──────────────────────────────────────────────────────

    async def import_from_zip(
        self,
        zip_path: str | Path,
        symbols_filter: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        استيراد بيانات MetaStock من ملف ZIP.

        Parameters
        ----------
        zip_path : مسار ملف ZIP
        symbols_filter : قائمة رموز للاستيراد (None = الكل)

        Returns
        -------
        dict: {status, symbols_count, imported_rows, skipped_rows, errors}
        """
        with tempfile.TemporaryDirectory(prefix='ms_import_') as tmp:
            try:
                data_dir = extract_metastock_zip(zip_path, tmp)
                return await self._import_from_dir_internal(
                    data_dir, symbols_filter
                )
            except Exception as e:
                logger.error(f"خطأ في استيراد ZIP: {e}")
                return self._error_result(str(e))

    async def import_from_dir(
        self,
        data_dir: str | Path,
        symbols_filter: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        استيراد بيانات MetaStock من مجلد.

        Parameters
        ----------
        data_dir : مسار مجلد MetaStock
        symbols_filter : قائمة رموز للاستيراد (None = الكل)
        """
        return await self._import_from_dir_internal(
            Path(data_dir), symbols_filter
        )

    # ── المنطق الداخلي ──────────────────────────────────────────────────────

    async def _import_from_dir_internal(
        self,
        data_dir: Path,
        symbols_filter: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """منطق الاستيراد الفعلي من مجلد."""
        result: Dict[str, Any] = {
            'status':         'success',
            'symbols_count':  0,
            'imported_rows':  0,
            'skipped_rows':   0,
            'errors':         [],
            'symbols':        [],
        }

        try:
            parser = MetaStockParser(data_dir)
            symbols_info = parser.list_symbols()

            if not symbols_info:
                result['status'] = 'empty'
                result['errors'].append('لم يُعثر على رموز في ملفات MetaStock')
                return result

            # تطبيق الفلتر إن وُجد
            if symbols_filter:
                upper_filter = {s.upper() for s in symbols_filter}
                symbols_info = [
                    s for s in symbols_info
                    if s['symbol'].upper() in upper_filter
                ]

            result['symbols_count'] = len(symbols_info)
            logger.info(f"MetaStock: سيتم استيراد {len(symbols_info)} رمز")

            await self._init_pool()

            for sym_info in symbols_info:
                symbol = sym_info['symbol']
                try:
                    df = parser.parse_symbol(symbol)
                    if df.empty:
                        logger.warning(f"  ⚠ {symbol}: لا توجد بيانات")
                        result['skipped_rows'] += 0
                        continue

                    rows_imported = await self._upsert_dataframe(df)
                    result['imported_rows'] += rows_imported
                    result['symbols'].append({
                        'symbol':     symbol,
                        'name':       sym_info.get('name', ''),
                        'rows':       rows_imported,
                        'timeframe':  sym_info.get('timeframe', '1d'),
                        'first_date': sym_info.get('first_date'),
                        'last_date':  sym_info.get('last_date'),
                    })
                    logger.success(
                        f"  ✓ {symbol}: {rows_imported:,} شمعة مستوردة"
                    )

                except Exception as e:
                    logger.error(f"  ✗ {symbol}: {e}")
                    result['errors'].append(f"{symbol}: {str(e)}")

        except FileNotFoundError as e:
            result['status'] = 'error'
            result['errors'].append(str(e))
            return result
        except Exception as e:
            logger.error(f"خطأ عام في الاستيراد: {e}")
            result['status'] = 'error'
            result['errors'].append(str(e))
        finally:
            await self._close_pool()

        # تحديد الحالة النهائية
        if result['errors'] and result['imported_rows'] > 0:
            result['status'] = 'partial'
        elif result['errors']:
            result['status'] = 'error'

        return result

    async def _upsert_dataframe(self, df: pd.DataFrame) -> int:
        """
        إدراج أو تحديث DataFrame في جدول market_data.ohlcv.
        يستخدم ON CONFLICT DO UPDATE للتعامل مع التكرار.
        """
        if df.empty:
            return 0

        upsert_sql = """
            INSERT INTO market_data.ohlcv
                (time, symbol, timeframe, open, high, low, close,
                 volume, open_interest, source)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (time, symbol, timeframe)
            DO UPDATE SET
                open          = EXCLUDED.open,
                high          = EXCLUDED.high,
                low           = EXCLUDED.low,
                close         = EXCLUDED.close,
                volume        = EXCLUDED.volume,
                open_interest = EXCLUDED.open_interest,
                source        = EXCLUDED.source
        """

        records = [
            (
                row['time'],
                row['symbol'],
                row['timeframe'],
                float(row['open']),
                float(row['high']),
                float(row['low']),
                float(row['close']),
                int(row['volume']),
                int(row.get('open_interest', 0)),
                'metastock',
            )
            for _, row in df.iterrows()
        ]

        total = 0
        async with self.pool.acquire() as conn:
            # إدراج على دفعات لتجنب الضغط على الذاكرة
            for i in range(0, len(records), self.BATCH_SIZE):
                batch = records[i: i + self.BATCH_SIZE]
                await conn.executemany(upsert_sql, batch)
                total += len(batch)

        return total

    @staticmethod
    def _error_result(message: str) -> Dict[str, Any]:
        return {
            'status':        'error',
            'symbols_count': 0,
            'imported_rows': 0,
            'skipped_rows':  0,
            'errors':        [message],
            'symbols':       [],
        }

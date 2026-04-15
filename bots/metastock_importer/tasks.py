"""
Celery Tasks — MetaStock Importer
====================================
مهام Celery لاستيراد ملفات MetaStock في الخلفية.
"""

import asyncio
from pathlib import Path
from typing import List, Optional

from loguru import logger

from scripts.celery_app import app
from bots.metastock_importer.bot import MetaStockImporter


@app.task(
    name='bots.metastock_importer.tasks.import_metastock_zip',
    bind=True,
    max_retries=2,
    default_retry_delay=30,
)
def import_metastock_zip(
    self,
    zip_path: str,
    symbols_filter: Optional[List[str]] = None,
) -> dict:
    """
    مهمة Celery: استيراد ملف ZIP يحتوي على بيانات MetaStock.

    Parameters
    ----------
    zip_path : مسار ملف ZIP
    symbols_filter : قائمة رموز للاستيراد (None = الكل)
    """
    logger.info(f"[Celery] بدء استيراد MetaStock ZIP: {zip_path}")
    try:
        importer = MetaStockImporter()
        result = asyncio.run(
            importer.import_from_zip(zip_path, symbols_filter)
        )
        logger.info(
            f"[Celery] اكتمل الاستيراد: "
            f"{result.get('imported_rows', 0):,} صف، "
            f"الحالة: {result.get('status')}"
        )
        return result
    except Exception as exc:
        logger.error(f"[Celery] خطأ في import_metastock_zip: {exc}")
        raise self.retry(exc=exc)


@app.task(
    name='bots.metastock_importer.tasks.import_metastock_dir',
    bind=True,
    max_retries=2,
    default_retry_delay=30,
)
def import_metastock_dir(
    self,
    data_dir: str,
    symbols_filter: Optional[List[str]] = None,
) -> dict:
    """
    مهمة Celery: استيراد مجلد يحتوي على بيانات MetaStock.

    Parameters
    ----------
    data_dir : مسار مجلد MetaStock
    symbols_filter : قائمة رموز للاستيراد (None = الكل)
    """
    logger.info(f"[Celery] بدء استيراد MetaStock DIR: {data_dir}")
    try:
        importer = MetaStockImporter()
        result = asyncio.run(
            importer.import_from_dir(data_dir, symbols_filter)
        )
        logger.info(
            f"[Celery] اكتمل الاستيراد: "
            f"{result.get('imported_rows', 0):,} صف، "
            f"الحالة: {result.get('status')}"
        )
        return result
    except Exception as exc:
        logger.error(f"[Celery] خطأ في import_metastock_dir: {exc}")
        raise self.retry(exc=exc)

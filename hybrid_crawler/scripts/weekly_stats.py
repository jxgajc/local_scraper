import os
import sys
import logging
from datetime import datetime

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

env_path = os.path.join(project_root, ".env")
try:
    from dotenv import load_dotenv
    load_dotenv(env_path)
except Exception:
    pass

from sqlalchemy import inspect, text

from hybrid_crawler.models import Base, engine
from hybrid_crawler.models import crawl_status
from hybrid_crawler.models import spider_progress
from hybrid_crawler.models import crawl_data
from hybrid_crawler.models import fujian_drug
from hybrid_crawler.models import guangdong_drug
from hybrid_crawler.models import hainan_drug
from hybrid_crawler.models import hebei_drug
from hybrid_crawler.models import liaoning_drug
from hybrid_crawler.models import nhsa_drug
from hybrid_crawler.models import ningxia_drug
from hybrid_crawler.models import shandong_drug
from hybrid_crawler.models import tianjin_drug
from hybrid_crawler.recrawl.manager import RecrawlManager
from hybrid_crawler.recrawl.registry import get_adapter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("weekly_stats")

EXCLUDE_TABLES = {"crawl_status", "spider_progress", "crawl_data"}


def parse_week_key(table_name: str) -> str:
    parts = table_name.rsplit("_w", 1)
    if len(parts) == 2:
        return parts[1]
    return ""


def get_adapter_meta() -> dict[str, dict[str, str]]:
    adapter_meta = {}
    RecrawlManager.list_spiders()
    for spider_name in RecrawlManager.list_spiders():
        adapter = get_adapter(spider_name)
        if adapter.table_name:
            adapter_meta[adapter.table_name] = {
                "unique_id": adapter.unique_id or ""
            }
    return adapter_meta


def fetch_count(conn, table_name: str) -> int:
    result = conn.execute(text(f"SELECT COUNT(1) FROM `{table_name}`")).scalar()
    return int(result or 0)


def fetch_distinct_count(conn, table_name: str, column_name: str) -> int:
    result = conn.execute(text(f"SELECT COUNT(DISTINCT `{column_name}`) FROM `{table_name}`")).scalar()
    return int(result or 0)


def run_stats() -> None:
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    base_tables = [t for t in Base.metadata.tables.keys() if t not in EXCLUDE_TABLES]
    adapter_meta = get_adapter_meta()
    with engine.begin() as conn:
        for base in base_tables:
            prefix = f"{base}_w"
            week_tables = [t for t in existing_tables if t.startswith(prefix)]
            if not week_tables:
                continue
            week_tables = sorted(week_tables, key=parse_week_key)
            logger.info(f"统计表: {base}")
            prev_count = None
            for week_table in week_tables:
                count = fetch_count(conn, week_table)
                delta = count - prev_count if prev_count is not None else 0
                unique_id = adapter_meta.get(base, {}).get("unique_id")
                if unique_id:
                    distinct_count = fetch_distinct_count(conn, week_table, unique_id)
                    logger.info(f"{week_table} rows={count} diff={delta} distinct_{unique_id}={distinct_count}")
                else:
                    logger.info(f"{week_table} rows={count} diff={delta}")
                prev_count = count
    logger.info(f"统计完成 {datetime.now()}")


if __name__ == "__main__":
    run_stats()

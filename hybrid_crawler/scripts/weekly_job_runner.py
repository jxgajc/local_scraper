import os
import sys
import time
import asyncio
import logging
import subprocess
from datetime import datetime, timedelta

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

from hybrid_crawler.models import Base, engine, init_db
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("weekly_job_runner")

EXCLUDE_TABLES = {"crawl_status", "spider_progress", "crawl_data"}


def get_week_suffix(run_dt: datetime) -> str:
    iso_year, iso_week, _ = run_dt.isocalendar()
    return f"_w{iso_year}{iso_week:02d}"


def ensure_tables() -> None:
    init_db()


def run_full_crawl() -> None:
    run_script = os.path.join(project_root, "run.py")
    subprocess.run([sys.executable, run_script], cwd=project_root, check=True)


async def run_recrawl() -> None:
    await RecrawlManager.recrawl_all()


def rename_tables(suffix: str) -> None:
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    base_tables = set(Base.metadata.tables.keys())
    targets = [t for t in base_tables if t in existing_tables and t not in EXCLUDE_TABLES]
    if not targets:
        logger.info("未找到需要重命名的表")
        return
    with engine.begin() as conn:
        for table_name in targets:
            new_name = f"{table_name}{suffix}"
            if new_name in existing_tables:
                logger.warning(f"目标表已存在，跳过: {new_name}")
                continue
            conn.execute(text(f"RENAME TABLE `{table_name}` TO `{new_name}`"))
            logger.info(f"已重命名表 {table_name} -> {new_name}")


def run_once() -> None:
    logger.info("开始执行周度采集任务")
    ensure_tables()
    logger.info("已确认数据库表存在")
    run_full_crawl()
    logger.info("全量采集完成")
    asyncio.run(run_recrawl())
    logger.info("补充采集完成")
    suffix = get_week_suffix(datetime.now())
    rename_tables(suffix)
    ensure_tables()
    logger.info("本周任务完成")


def compute_schedule(start_dt: datetime, window_days: int, run_hour: int, run_minute: int) -> list[datetime]:
    end_dt = start_dt + timedelta(days=window_days)
    scheduled = []
    cursor = start_dt
    while cursor <= end_dt:
        if cursor.weekday() == 4:
            scheduled_dt = cursor.replace(hour=run_hour, minute=run_minute, second=0, microsecond=0)
            scheduled.append(scheduled_dt)
        cursor = cursor + timedelta(days=1)
    return scheduled


def run_scheduler() -> None:
    run_hour = int(os.getenv("FRIDAY_RUN_HOUR", "20"))
    run_minute = int(os.getenv("FRIDAY_RUN_MINUTE", "0"))
    window_days = int(os.getenv("FRIDAY_RUN_WINDOW_DAYS", "30"))
    schedule = compute_schedule(datetime.now(), window_days, run_hour, run_minute)
    if not schedule:
        logger.info("未生成任何计划执行时间")
        return
    for run_dt in schedule:
        now = datetime.now()
        if run_dt <= now:
            continue
        wait_seconds = (run_dt - now).total_seconds()
        logger.info(f"等待下次执行时间: {run_dt}")
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        run_once()
    logger.info("调度窗口结束")


if __name__ == "__main__":
    run_scheduler()

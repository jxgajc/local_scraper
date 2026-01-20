import sys
import os
import argparse
import asyncio
import time
import logging
from typing import Callable

# Add project root to sys.path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

env_path = os.path.join(project_root, '.env')
try:
    from dotenv import load_dotenv
    load_dotenv(env_path)
except Exception:
    pass

from hybrid_crawler.recrawl.manager import RecrawlManager
from hybrid_crawler.recrawl.registry import get_adapter
from hybrid_crawler.models import SessionLocal
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def run_test(spider_name: str, db_sample: int = 0, timeout_find_missing: int = 60, timeout_recrawl: int = 120):
    logger.info(f"=== Starting Recrawl Debug for {spider_name} ===")

    start_time = time.time()

    def stop_check() -> bool:
        if time.time() - start_time > 120:
            logger.warning("⏰ Timeout reached (120s)!")
            return True
        return False

    available_spiders = RecrawlManager.list_spiders()
    if spider_name not in available_spiders:
        logger.error(f"Adapter not found for {spider_name}. Registered: {available_spiders}")
        return

    if db_sample > 0:
        adapter = get_adapter(spider_name, stop_check=stop_check, update_only=True)

        session = SessionLocal()
        try:
            sample_ids_sql = text(
                f"SELECT {adapter.unique_id} FROM {adapter.table_name} "
                f"WHERE {adapter.unique_id} IS NOT NULL "
                f"GROUP BY {adapter.unique_id} ORDER BY MAX(id) DESC LIMIT :limit"
            )
            rows = session.execute(sample_ids_sql, {"limit": db_sample}).fetchall()
            picked_ids = [str(r[0]) for r in rows if r and r[0] is not None]

            if not picked_ids:
                logger.info("⚠️ DB sample returned no ids")
                return

            missing_data = {}
            for uid in picked_ids:
                row = session.execute(
                    text(
                        f"SELECT * FROM {adapter.table_name} WHERE {adapter.unique_id}=:uid "
                        f"ORDER BY id DESC LIMIT 1"
                    ),
                    {"uid": uid},
                ).fetchone()
                base_info = dict(row._mapping) if row else {adapter.unique_id: uid}
                base_info.setdefault(adapter.unique_id, uid)
                missing_data[uid] = base_info

        finally:
            session.close()

        try:
            count = await asyncio.wait_for(adapter.recrawl(missing_ids=missing_data), timeout=timeout_recrawl)
            logger.info(f"✅ Recrawl finished (update_only). Processed: {count}")
        except asyncio.TimeoutError:
            logger.error("❌ Recrawl timed out!")
        except Exception as e:
            logger.error(f"❌ Recrawl failed: {e}")

        logger.info(f"=== Finished in {time.time() - start_time:.2f}s ===")
        return

    logger.info("🔍 Step 1: Finding missing data...")
    try:
        missing = await asyncio.wait_for(RecrawlManager.find_missing(spider_name, stop_check), timeout=timeout_find_missing)
        logger.info(f"Found {len(missing)} missing items.")
    except asyncio.TimeoutError:
        logger.error("Find missing timed out!")
        missing = {}
    except Exception as e:
        logger.error(f"Find missing failed: {e}")
        missing = {}

    target_ids = list(missing.keys())[:5]
    subset = {}

    if target_ids:
        logger.info(f"🎯 Step 2: Triggering Recrawl for {len(target_ids)} items (Subset)...")
        subset = {k: missing[k] for k in target_ids}
    else:
        logger.info("⚠️ No missing data found.")
        logger.info("🛠️ Force Check: Attempting to verify DB connectivity and existence...")
        session = SessionLocal()
        try:
            session.execute(text("SELECT 1"))
            logger.info("✅ DB Connection OK")
            logger.info("Please verify database records manually.")
        except Exception as e:
            logger.error(f"❌ DB Check Failed: {e}")
        finally:
            session.close()

        logger.info("Skipping Recrawl execution as no valid IDs available.")

    if subset:
        try:
            count = await asyncio.wait_for(
                RecrawlManager.recrawl(spider_name, missing_ids=subset, stop_check=stop_check),
                timeout=timeout_recrawl
            )
            logger.info(f"✅ Recrawl finished. Processed: {count}")

        except asyncio.TimeoutError:
            logger.error("❌ Recrawl timed out!")
        except Exception as e:
            logger.error(f"❌ Recrawl failed: {e}")

    logger.info(f"=== Finished in {time.time() - start_time:.2f}s ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("spider_name")
    parser.add_argument("--db-sample", type=int, default=0)
    parser.add_argument("--timeout-find-missing", type=int, default=60)
    parser.add_argument("--timeout-recrawl", type=int, default=120)
    args = parser.parse_args()

    asyncio.run(
        run_test(
            args.spider_name,
            db_sample=args.db_sample,
            timeout_find_missing=args.timeout_find_missing,
            timeout_recrawl=args.timeout_recrawl,
        )
    )

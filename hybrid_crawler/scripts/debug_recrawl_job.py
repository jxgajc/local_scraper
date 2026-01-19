import sys
import os
import asyncio
import time
import logging
from typing import Callable

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hybrid_crawler.recrawl.manager import RecrawlManager
from hybrid_crawler.recrawl.registry import get_adapter
from hybrid_crawler.models import SessionLocal
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def run_test(spider_name: str):
    logger.info(f"=== Starting Recrawl Debug for {spider_name} ===")
    
    # 1. Setup Timeout Check
    start_time = time.time()
    def stop_check() -> bool:
        if time.time() - start_time > 120: # 2 minutes
            logger.warning("⏰ Timeout reached (120s)!")
            return True
        return False

    # 2. Get Adapter
    adapter = get_adapter(spider_name)
    if not adapter:
        logger.error(f"Adapter not found for {spider_name}")
        return

    # 3. Find Missing
    logger.info("🔍 Step 1: Finding missing data...")
    try:
        # 使用 wait_for 增加一层超时保障
        missing = await asyncio.wait_for(RecrawlManager.find_missing(spider_name, stop_check), timeout=60)
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
        # 尝试查询数据库中该省份的表是否存在数据
        # 由于无法直接获取 Model，我们尝试猜测表名或跳过
        session = SessionLocal()
        try:
            # 简单验证 DB 连接
            session.execute(text("SELECT 1"))
            logger.info("✅ DB Connection OK")
            
            # 如果能知道表名最好，不知道则提示用户手动检查
            logger.info("Please verify database records manually.")
            
        except Exception as e:
            logger.error(f"❌ DB Check Failed: {e}")
        finally:
            session.close()
            
        # 构造 Mock Data 进行补采测试 (如果支持)
        # 这里的 Mock 需要真实的 ID 格式，比较困难，故仅在有真实缺失时执行 Recrawl
        logger.info("Skipping Recrawl execution as no valid IDs available.")

    if subset:
        try:
            count = await asyncio.wait_for(
                RecrawlManager.recrawl(spider_name, missing_ids=subset, stop_check=stop_check),
                timeout=120
            )
            logger.info(f"✅ Recrawl finished. Processed: {count}")
            
        except asyncio.TimeoutError:
            logger.error("❌ Recrawl timed out!")
        except Exception as e:
            logger.error(f"❌ Recrawl failed: {e}")

    logger.info(f"=== Finished in {time.time() - start_time:.2f}s ===")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python debug_recrawl_job.py <spider_name>")
        sys.exit(1)
        
    spider = sys.argv[1]
    asyncio.run(run_test(spider))

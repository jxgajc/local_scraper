#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化_test表脚本 (优化版)
功能：检查各个爬虫对应的_test表是否为空，如果为空，则从正式表同步数据。
优化点：采用分批次同步 (Batch Processing)，避免一次性全量插入导致数据库锁死。
"""

import os
import sys
import time
import logging
from sqlalchemy import text, func

# 添加项目路径
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

# 尝试导入 recrawl_checker
try:
    from recrawl_checker import SPIDER_MAPPING
except ImportError:
    sys.path.append(os.path.dirname(current_dir))
    from recrawl_checker import SPIDER_MAPPING

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("init_test_tables")

BATCH_SIZE = 5000  # 每次同步的行数
SLEEP_INTERVAL = 0.1  # 每批次间隔时间(秒)

def init_tables():
    logger.info("🚀 开始检查并初始化_test表 (Batch Mode)...")
    
    for spider_name, crawler_class in SPIDER_MAPPING.items():
        crawler = None
        try:
            crawler = crawler_class()
            test_table = crawler.table_name
            
            if not test_table.endswith('_test'):
                logger.warning(f"[{spider_name}] 表名 {test_table} 不以 _test 结尾，跳过初始化")
                continue
                
            prod_table = test_table.replace('_test', '')
            
            # 1. 检查_test表是否为空
            count_sql = text(f"SELECT COUNT(1) FROM {test_table}")
            test_count = crawler.db_session.execute(count_sql).scalar()
            
            logger.info(f"[{spider_name}] {test_table} 当前数据量: {test_count}")
            
            if test_count == 0:
                logger.info(f"[{spider_name}] _test表为空，准备从 {prod_table} 同步数据...")
                
                # 2. 获取源表 ID 范围
                range_sql = text(f"SELECT MIN(id), MAX(id), COUNT(1) FROM {prod_table}")
                min_id, max_id, total_rows = crawler.db_session.execute(range_sql).fetchone()
                
                if not total_rows or total_rows == 0:
                    logger.info(f"[{spider_name}] 源表 {prod_table} 为空，无需同步")
                    continue
                    
                if min_id is None or max_id is None:
                    logger.warning(f"[{spider_name}] 源表 {prod_table} 没有有效的 ID 范围，尝试全量同步...")
                    # 降级方案：全量同步 (针对无 ID 的表，虽然不太可能)
                    sync_sql = text(f"INSERT IGNORE INTO {test_table} SELECT * FROM {prod_table}")
                    crawler.db_session.execute(sync_sql)
                    crawler.db_session.commit()
                    continue

                logger.info(f"[{spider_name}] 源数据总量: {total_rows}, ID范围: {min_id} -> {max_id}")
                
                # 3. 分批次同步
                processed_count = 0
                current_id = min_id
                
                while current_id <= max_id:
                    next_id = current_id + BATCH_SIZE
                    
                    # 构造批次插入 SQL
                    batch_sql = text(f"""
                        INSERT IGNORE INTO {test_table} 
                        SELECT * FROM {prod_table} 
                        WHERE id >= :start_id AND id < :end_id
                    """)
                    
                    result = crawler.db_session.execute(batch_sql, {"start_id": current_id, "end_id": next_id})
                    crawler.db_session.commit() # 每次提交，释放锁
                    
                    rows_affected = result.rowcount
                    processed_count += rows_affected
                    
                    # 进度日志
                    progress = min(100, int((current_id - min_id) / (max_id - min_id) * 100))
                    if current_id % (BATCH_SIZE * 5) == 0 or rows_affected > 0:
                         logger.info(f"[{spider_name}] 进度 {progress}% | 同步批次 {current_id}-{next_id} | 本次写入: {rows_affected}")
                    
                    current_id = next_id
                    time.sleep(SLEEP_INTERVAL) # 休息一下，防止数据库高负载
                
                logger.info(f"[{spider_name}] ✅ 同步完成，共写入: {processed_count} 条")
                
                # 最终确认
                final_count = crawler.db_session.execute(count_sql).scalar()
                logger.info(f"[{spider_name}] {test_table} 最终数据量: {final_count}")
                
            else:
                logger.info(f"[{spider_name}] _test表已有数据，跳过同步")
                
        except Exception as e:
            logger.error(f"[{spider_name}] ❌ 初始化失败: {e}")
            if crawler:
                crawler.db_session.rollback()
        finally:
            if crawler:
                crawler.close()

if __name__ == "__main__":
    init_tables()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化数据库表脚本
功能：在 spider_once 数据库中创建所有模型对应的表
"""

import os
import sys
import logging

# 添加项目路径
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("init_tables")


def init_tables():
    """初始化所有数据库表"""
    logger.info("🚀 开始初始化数据库表...")

    try:
        # 导入所有模型以确保它们被注册到 Base.metadata
        from hybrid_crawler.models import Base, engine, init_db
        from hybrid_crawler.models.crawl_status import CrawlStatus
        from hybrid_crawler.models.spider_progress import SpiderProgress
        from hybrid_crawler.models.fujian_drug import FujianDrug
        from hybrid_crawler.models.guangdong_drug import GuangdongDrug
        from hybrid_crawler.models.hainan_drug import HainanDrug
        from hybrid_crawler.models.hebei_drug import HebeiDrug
        from hybrid_crawler.models.liaoning_drug import LiaoningDrug
        from hybrid_crawler.models.ningxia_drug import NingxiaDrug
        from hybrid_crawler.models.tianjin_drug import TianjinDrug

        # 创建所有表
        logger.info("正在创建表...")
        Base.metadata.create_all(bind=engine)

        logger.info("✅ 数据库表初始化完成!")

        # 列出已创建的表
        from sqlalchemy import inspect
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        logger.info(f"已创建的表: {tables}")

    except Exception as e:
        logger.error(f"❌ 初始化失败: {e}")
        raise


if __name__ == "__main__":
    init_tables()

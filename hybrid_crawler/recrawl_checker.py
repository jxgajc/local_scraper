#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬虫数据补采脚本（简化版）
功能：通过官网API确定缺失数据并进行补采
补采逻辑已迁移到各爬虫类中
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from sqlalchemy import text

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('recrawl.log'),
        logging.StreamHandler()
    ]
)

try:
    from hybrid_crawler.models import SessionLocal
except ImportError:
    sys.path.insert(0, os.path.join(project_root, 'hybrid_crawler'))
    from hybrid_crawler.models import SessionLocal

# 导入爬虫类
from hybrid_crawler.spiders.fujian_drug_store import FujianDrugSpider
from hybrid_crawler.spiders.guangdong_drug_store import GuangdongDrugSpider
from hybrid_crawler.spiders.hainan_drug_store import HainanDrugSpider
from hybrid_crawler.spiders.ningxia_drug_store import NingxiaDrugSpider
from hybrid_crawler.spiders.hebei_drug_store import HebeiDrugSpider
from hybrid_crawler.spiders.liaoning_drug_store import LiaoningDrugSpider
from hybrid_crawler.spiders.tianjin_drug_store import TianjinDrugSpider

# 爬虫映射表
SPIDER_MAP = {
    "fujian": FujianDrugSpider,
    "guangdong": GuangdongDrugSpider,
    "hainan": HainanDrugSpider,
    "ningxia": NingxiaDrugSpider,
    "hebei": HebeiDrugSpider,
    "liaoning": LiaoningDrugSpider,
    "tianjin": TianjinDrugSpider,
}


def get_existing_ids(db_session, table_name, unique_id):
    """从数据库获取已采集的唯一标识"""
    sql = text(f"SELECT DISTINCT {unique_id} FROM {table_name}")
    result = db_session.execute(sql)
    return {str(row[0]) for row in result if row[0] is not None}


def find_missing(spider_cls, db_session, logger, stop_check=None):
    """找出缺失的数据"""
    config = spider_cls.recrawl_config
    table_name = config['table_name']
    unique_id = config['unique_id']

    logger.info(f"从数据库获取 {table_name} 表中的现有 {unique_id}...")
    existing_ids = get_existing_ids(db_session, table_name, unique_id)
    logger.info(f"数据库中已有 {len(existing_ids)} 条记录")

    logger.info(f"从官网API获取所有 {unique_id}...")
    api_data = spider_cls.fetch_all_ids_from_api(logger=logger, stop_check=stop_check)
    logger.info(f"官网API共有 {len(api_data)} 条记录")

    # 找出缺失的
    missing_data = {k: v for k, v in api_data.items() if k not in existing_ids}
    logger.info(f"发现 {len(missing_data)} 条缺失数据")

    return missing_data


def recrawl_spider(spider_name, logger=None):
    """执行特定爬虫的补采"""
    if logger is None:
        logger = logging.getLogger("recrawl_spider")

    if spider_name not in SPIDER_MAP:
        logger.error(f"未知的爬虫名称: {spider_name}")
        logger.info(f"可用的爬虫名称: {list(SPIDER_MAP.keys())}")
        return False

    spider_cls = SPIDER_MAP[spider_name]
    db_session = SessionLocal()

    try:
        logger.info(f"开始执行 {spider_name} 爬虫的补采...")

        # 1. 找出缺失数据
        missing_data = find_missing(spider_cls, db_session, logger)

        if not missing_data:
            logger.info("没有缺失数据，无需补采")
            return True

        # 2. 执行补采
        logger.info(f"开始补采 {len(missing_data)} 条数据...")
        success_count = spider_cls.recrawl_by_ids(missing_data, db_session, logger)
        logger.info(f"{spider_name} 爬虫补采完成，成功补采 {success_count} 条数据")

        return True
    except Exception as e:
        logger.error(f"执行 {spider_name} 爬虫补采失败: {e}")
        return False
    finally:
        db_session.close()


def check_spider(spider_name, logger=None):
    """检查特定爬虫的缺失情况"""
    if logger is None:
        logger = logging.getLogger("check_spider")

    if spider_name not in SPIDER_MAP:
        logger.error(f"未知的爬虫名称: {spider_name}")
        return None

    spider_cls = SPIDER_MAP[spider_name]
    db_session = SessionLocal()

    try:
        missing_data = find_missing(spider_cls, db_session, logger)
        return {
            "spider_name": spider_name,
            "missing_count": len(missing_data),
            "missing_ids": list(missing_data.keys())[:10]  # 只返回前10个示例
        }
    except Exception as e:
        logger.error(f"检查 {spider_name} 爬虫失败: {e}")
        return {"spider_name": spider_name, "error": str(e)}
    finally:
        db_session.close()


def check_all_spiders():
    """检查所有爬虫的缺失情况"""
    logger = logging.getLogger("check_all_spiders")
    logger.info("开始检查所有爬虫的缺失情况...")

    report = {}
    for spider_name in SPIDER_MAP.keys():
        logger.info(f"\n=== 检查 {spider_name} 爬虫 ===")
        result = check_spider(spider_name, logger)
        if result:
            report[spider_name] = result

    # 生成报告
    logger.info("\n=== 补采检查报告 ===")
    for spider_name, result in report.items():
        if "error" in result:
            logger.info(f"{spider_name}: 错误 - {result['error']}")
        else:
            logger.info(f"{spider_name}: 缺失 {result['missing_count']} 条数据")
            if result['missing_ids']:
                logger.info(f"  示例: {result['missing_ids']}")

    return report


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="爬虫数据补采工具")
    parser.add_argument("--check", action="store_true", help="检查所有爬虫缺失情况")
    parser.add_argument("--check-spider", type=str, help="检查特定爬虫缺失情况")
    parser.add_argument("--recrawl", type=str, help="执行特定爬虫补采")
    args = parser.parse_args()

    if args.check:
        check_all_spiders()
    elif args.check_spider:
        check_spider(args.check_spider)
    elif args.recrawl:
        recrawl_spider(args.recrawl)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

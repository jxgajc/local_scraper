"""
Recrawl 模块 - 统一的补充采集架构

使用方式:
    from hybrid_crawler.recrawl import RecrawlManager

    # 查找缺失数据
    missing = RecrawlManager.find_missing('fujian_drug_spider')

    # 执行补充采集
    count = RecrawlManager.recrawl('fujian_drug_spider', missing)

    # 或一键执行
    count = RecrawlManager.full_recrawl('fujian_drug_spider')
"""

from .manager import RecrawlManager

__all__ = ['RecrawlManager']

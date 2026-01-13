"""
RecrawlManager - 补充采集统一管理器（异步版本）
"""
import asyncio
import logging
from typing import Dict, Any, Optional, List, Callable

from .registry import get_adapter, list_adapters, is_registered

logger = logging.getLogger(__name__)


class RecrawlManager:
    """补充采集统一管理器 - 异步版本"""

    @staticmethod
    async def find_missing(spider_name: str, stop_check: Callable = None) -> Dict[str, Any]:
        """查找指定爬虫的缺失数据"""
        _ensure_adapters_loaded()

        if not is_registered(spider_name):
            logger.warning(f"未找到 spider '{spider_name}' 的 Adapter")
            return {}

        adapter = get_adapter(spider_name, stop_check=stop_check)
        return await adapter.find_missing()

    @staticmethod
    async def recrawl(spider_name: str, missing_ids=None, stop_check: Callable = None) -> int:
        """执行指定爬虫的补充采集"""
        _ensure_adapters_loaded()

        if not is_registered(spider_name):
            logger.warning(f"未找到 spider '{spider_name}' 的 Adapter")
            return 0

        adapter = get_adapter(spider_name, stop_check=stop_check)
        return await adapter.recrawl(missing_ids)

    @staticmethod
    async def full_recrawl(spider_name: str, stop_check: Callable = None) -> int:
        """执行完整的补采流程：查找缺失 -> 补采"""
        _ensure_adapters_loaded()

        if not is_registered(spider_name):
            logger.warning(f"未找到 spider '{spider_name}' 的 Adapter")
            return 0

        adapter = get_adapter(spider_name, stop_check=stop_check)
        return await adapter.recrawl()

    @staticmethod
    def list_spiders() -> List[str]:
        """返回所有支持补采的爬虫名称"""
        _ensure_adapters_loaded()
        return list(list_adapters().keys())

    @staticmethod
    async def check_all(stop_check: Callable = None) -> Dict[str, Dict[str, Any]]:
        """检查所有爬虫的缺失数据"""
        _ensure_adapters_loaded()

        results = {}
        for spider_name in list_adapters().keys():
            if stop_check and stop_check():
                break
            try:
                missing = await RecrawlManager.find_missing(spider_name, stop_check)
                results[spider_name] = {
                    'missing_count': len(missing),
                    'missing_data': missing
                }
            except Exception as e:
                logger.error(f"检查 {spider_name} 失败: {e}")
                results[spider_name] = {
                    'missing_count': -1,
                    'error': str(e)
                }
        return results

    @staticmethod
    async def recrawl_all(stop_check: Callable = None) -> Dict[str, int]:
        """对所有爬虫执行补采"""
        _ensure_adapters_loaded()

        results = {}
        for spider_name in list_adapters().keys():
            if stop_check and stop_check():
                break
            try:
                count = await RecrawlManager.full_recrawl(spider_name, stop_check)
                results[spider_name] = count
            except Exception as e:
                logger.error(f"补采 {spider_name} 失败: {e}")
                results[spider_name] = -1
        return results


_adapters_loaded = False


def _ensure_adapters_loaded():
    """确保所有 Adapter 已加载"""
    global _adapters_loaded
    if not _adapters_loaded:
        from . import adapters  # noqa: F401
        _adapters_loaded = True

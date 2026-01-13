"""
RecrawlManager - 补充采集统一管理器
"""
import logging
from typing import Dict, Any, Optional, List, Callable

from .registry import get_adapter, list_adapters, is_registered

logger = logging.getLogger(__name__)


class RecrawlManager:
    """
    补充采集统一管理器

    提供静态方法作为统一入口，内部委托给对应的 Adapter
    """

    @staticmethod
    def find_missing(spider_name: str, logger_instance=None, stop_check: Callable = None) -> Dict[str, Any]:
        """
        查找指定爬虫的缺失数据

        Args:
            spider_name: 爬虫名称
            logger_instance: 可选的日志实例
            stop_check: 可选的停止检查函数

        Returns:
            {unique_id: base_info} 缺失数据字典
        """
        # 延迟导入以避免循环依赖
        _ensure_adapters_loaded()

        if not is_registered(spider_name):
            logger.warning(f"未找到 spider '{spider_name}' 的 Adapter")
            return {}

        adapter = get_adapter(spider_name, logger_instance=logger_instance, stop_check=stop_check)
        return adapter.find_missing()

    @staticmethod
    def recrawl(spider_name: str, missing_ids=None, logger_instance=None, stop_check: Callable = None) -> int:
        """
        执行指定爬虫的补充采集

        Args:
            spider_name: 爬虫名称
            missing_ids: 可选，指定要补采的数据。如果为None则自动查找
            logger_instance: 可选的日志实例
            stop_check: 可选的停止检查函数

        Returns:
            成功补采的记录数
        """
        _ensure_adapters_loaded()

        if not is_registered(spider_name):
            logger.warning(f"未找到 spider '{spider_name}' 的 Adapter")
            return 0

        adapter = get_adapter(spider_name, logger_instance=logger_instance, stop_check=stop_check)
        return adapter.recrawl(missing_ids)

    @staticmethod
    def full_recrawl(spider_name: str, logger_instance=None, stop_check: Callable = None) -> int:
        """
        执行完整的补采流程：查找缺失 -> 补采

        Args:
            spider_name: 爬虫名称
            logger_instance: 可选的日志实例
            stop_check: 可选的停止检查函数

        Returns:
            成功补采的记录数
        """
        _ensure_adapters_loaded()

        if not is_registered(spider_name):
            logger.warning(f"未找到 spider '{spider_name}' 的 Adapter")
            return 0

        adapter = get_adapter(spider_name, logger_instance=logger_instance, stop_check=stop_check)
        missing = adapter.find_missing()
        if missing:
            return adapter.recrawl(missing)
        return 0

    @staticmethod
    def list_spiders() -> List[str]:
        """返回所有支持补采的爬虫名称"""
        _ensure_adapters_loaded()
        return list(list_adapters().keys())

    @staticmethod
    def check_all(logger_instance=None, stop_check: Callable = None) -> Dict[str, Dict[str, Any]]:
        """
        检查所有爬虫的缺失数据

        Returns:
            {spider_name: {missing_count: int, missing_data: dict}}
        """
        _ensure_adapters_loaded()

        results = {}
        for spider_name in list_adapters().keys():
            if stop_check and stop_check():
                break
            try:
                missing = RecrawlManager.find_missing(spider_name, logger_instance, stop_check)
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
    def recrawl_all(logger_instance=None, stop_check: Callable = None) -> Dict[str, int]:
        """
        对所有爬虫执行补采

        Returns:
            {spider_name: success_count}
        """
        _ensure_adapters_loaded()

        results = {}
        for spider_name in list_adapters().keys():
            if stop_check and stop_check():
                break
            try:
                count = RecrawlManager.full_recrawl(spider_name, logger_instance, stop_check)
                results[spider_name] = count
            except Exception as e:
                logger.error(f"补采 {spider_name} 失败: {e}")
                results[spider_name] = -1
        return results


_adapters_loaded = False


def _ensure_adapters_loaded():
    """确保所有 Adapter 已加载（触发注册）"""
    global _adapters_loaded
    if not _adapters_loaded:
        from . import adapters  # noqa: F401
        _adapters_loaded = True

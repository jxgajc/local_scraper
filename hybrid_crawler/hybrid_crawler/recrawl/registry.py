"""
Adapter 注册表 - 管理 spider_name 到 Adapter 的映射
"""
from typing import Dict, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from .base_adapter import BaseRecrawlAdapter

# 注册表：spider_name -> Adapter 类
_ADAPTER_REGISTRY: Dict[str, Type['BaseRecrawlAdapter']] = {}


def register_adapter(spider_name: str):
    """
    装饰器：注册 Adapter 类

    Usage:
        @register_adapter('fujian_drug_spider')
        class FujianRecrawlAdapter(BaseRecrawlAdapter):
            ...
    """
    def decorator(cls):
        _ADAPTER_REGISTRY[spider_name] = cls
        return cls
    return decorator


def get_adapter(spider_name: str, **kwargs) -> 'BaseRecrawlAdapter':
    """
    获取指定 spider 的 Adapter 实例

    Args:
        spider_name: 爬虫名称
        **kwargs: 传递给 Adapter 构造函数的参数

    Returns:
        Adapter 实例

    Raises:
        ValueError: 如果 spider_name 未注册
    """
    if spider_name not in _ADAPTER_REGISTRY:
        raise ValueError(f"未找到 spider '{spider_name}' 的 Adapter，已注册: {list(_ADAPTER_REGISTRY.keys())}")

    adapter_cls = _ADAPTER_REGISTRY[spider_name]
    return adapter_cls(**kwargs)


def list_adapters() -> Dict[str, Type['BaseRecrawlAdapter']]:
    """返回所有已注册的 Adapter"""
    return _ADAPTER_REGISTRY.copy()


def is_registered(spider_name: str) -> bool:
    """检查 spider 是否已注册"""
    return spider_name in _ADAPTER_REGISTRY

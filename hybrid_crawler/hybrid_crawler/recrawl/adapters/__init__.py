"""
省份适配器模块
"""
from .fujian import FujianRecrawlAdapter
from .guangdong import GuangdongRecrawlAdapter
from .hainan import HainanRecrawlAdapter
from .liaoning import LiaoningRecrawlAdapter
from .ningxia import NingxiaRecrawlAdapter
from .hebei import HebeiRecrawlAdapter
from .tianjin import TianjinRecrawlAdapter

__all__ = [
    'FujianRecrawlAdapter',
    'GuangdongRecrawlAdapter',
    'HainanRecrawlAdapter',
    'LiaoningRecrawlAdapter',
    'NingxiaRecrawlAdapter',
    'HebeiRecrawlAdapter',
    'TianjinRecrawlAdapter',
]

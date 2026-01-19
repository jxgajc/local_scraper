from abc import ABC, abstractmethod
from typing import List, Set, Any
import logging

logger = logging.getLogger(__name__)

class StorageBackend(ABC):
    """
    存储后端抽象基类
    """
    
    @abstractmethod
    def check_existence(self, ids: List[str]) -> Set[str]:
        """
        批量检查ID是否存在
        :param ids: 待检查的ID列表
        :return: 已存在的ID集合
        """
        pass

    @abstractmethod
    def save_batch(self, items: List[Any]) -> int:
        """
        批量保存数据 (仅新增，不更新)
        :param items: Item对象列表
        :return: 成功写入的数量
        """
        pass
        
    def close(self):
        """
        关闭连接 (可选)
        """
        pass

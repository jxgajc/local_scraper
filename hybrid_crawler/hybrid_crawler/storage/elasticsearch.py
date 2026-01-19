from typing import List, Set, Any
import logging
import os

try:
    from elasticsearch import Elasticsearch, helpers
except ImportError:
    Elasticsearch = None

from itemadapter import ItemAdapter
from .base import StorageBackend

logger = logging.getLogger(__name__)

class ElasticsearchStorage(StorageBackend):
    def __init__(self, hosts, user=None, password=None, index_prefix='drug_store'):
        if not Elasticsearch:
            raise ImportError("elasticsearch library is not installed. Please run 'pip install elasticsearch'")
        
        # 处理认证
        http_auth = (user, password) if user and password else None
        
        # 初始化客户端
        self.client = Elasticsearch(
            hosts=hosts, 
            http_auth=http_auth,
            timeout=30
        )
        self.index_prefix = index_prefix
        logger.info(f"Elasticsearch Storage initialized. Hosts: {hosts}, Prefix: {index_prefix}")

    def _get_index_name(self, item: Any) -> str:
        """
        根据 Item 内容确定索引名称
        格式: prefix_spidername
        """
        adapter = ItemAdapter(item)
        spider_name = adapter.get('spider_name', 'default')
        # 替换不支持的字符 (如大写转小写)
        return f"{self.index_prefix}_{spider_name}".lower()

    def check_existence(self, ids: List[str]) -> Set[str]:
        # 在 Elasticsearch 中，直接利用 _op_type='create' 即可实现"仅新增"，
        # 无需额外的 check_existence 查询。
        return set()

    def save_batch(self, items: List[Any]) -> int:
        if not items: return 0
        
        actions = []
        for item in items:
            adapter = ItemAdapter(item)
            # 转为字典
            doc = adapter.asdict()
            
            # 移除非序列化对象 (如果有)
            # ...
            
            # 使用 md5_id 作为文档 _id
            doc_id = doc.get('md5_id')
            if not doc_id:
                # 如果没有 md5_id，尝试生成? 或者跳过?
                # 假设 Phase 3 保证了 md5_id 存在
                logger.debug("Item missing md5_id, skipping ES indexing")
                continue
                
            index_name = self._get_index_name(item)
            
            # 构建 Bulk Action
            action = {
                "_index": index_name,
                "_id": doc_id,
                "_source": doc,
                "_op_type": "create"  # 关键: 仅在 ID 不存在时创建
            }
            actions.append(action)
            
        if not actions:
            return 0
            
        success_count = 0
        try:
            # 执行 Bulk 操作
            # raise_on_error=False: 忽略部分失败 (如已存在)
            # stats_only=True: 仅返回成功数量 (旧版) / 
            # bulk 返回 (success_count, errors_list)
            success, errors = helpers.bulk(
                self.client, 
                actions, 
                raise_on_error=False,
                refresh=False # 提高写入性能
            )
            success_count = success
            
            # 如果需要统计有多少是"已存在"导致的失败，可以分析 errors
            # 但这里我们只关心成功写入的新增数量
            
        except Exception as e:
            logger.error(f"ES Bulk Index error: {e}")
            
        return success_count

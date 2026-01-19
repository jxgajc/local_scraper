from typing import List, Set, Any, Type, Dict
import logging
from sqlalchemy.exc import IntegrityError
from itemadapter import ItemAdapter

from .base import StorageBackend
from ..models import SessionLocal
from ..models.crawl_data import CrawlData

logger = logging.getLogger(__name__)

class MySQLStorage(StorageBackend):
    def __init__(self):
        self.session_maker = SessionLocal

    def _get_model_class(self, item: Any) -> Type:
        """
        动态获取 Item 对应的 SQLAlchemy Model 类。
        """
        if hasattr(item, 'get_model_class'):
            return item.get_model_class()
        
        # 兼容旧逻辑或字典类型的 item
        if isinstance(item, dict) and 'model_class' in item:
            return item['model_class']
            
        return CrawlData

    def _create_orm_object(self, item: Any, model_class: Type) -> Any:
        if not item: return None
        # 自动映射 Item 字段到 Model 字段
        model_fields = [c.key for c in model_class.__table__.columns]
        
        # ItemAdapter 统一处理 Item 对象和字典
        adapter = ItemAdapter(item)
        item_data = {k: v for k, v in adapter.items() if k in model_fields}
        
        return model_class(**item_data)

    def check_existence(self, ids: List[str]) -> Set[str]:
        # 由于 MySQL 需要 Model Class 才能查询表，此接口在 MySQL 实现中难以独立使用
        # 逻辑已集成在 save_batch 中
        return set()

    def save_batch(self, items: List[Any]) -> int:
        session = self.session_maker()
        try:
            # 1. 按模型类分组 (Group items by Model Class)
            items_by_model: Dict[Type, List[Any]] = {}
            for item in items:
                model_cls = self._get_model_class(item)
                if model_cls not in items_by_model:
                    items_by_model[model_cls] = []
                items_by_model[model_cls].append(item)
            
            total_saved = 0
            
            for model_cls, model_items in items_by_model.items():
                # 转换为 ORM 对象
                orm_objects = []
                ids_map = {} # md5_id -> obj
                
                for item in model_items:
                    obj = self._create_orm_object(item, model_cls)
                    if not obj: continue
                    
                    # 假设所有 Model 都有 md5_id 字段
                    if hasattr(obj, 'md5_id') and obj.md5_id:
                        orm_objects.append(obj)
                        ids_map[obj.md5_id] = obj
                    else:
                        # 无指纹对象，直接当作新对象
                        orm_objects.append(obj)

                if not orm_objects:
                    continue

                # 2. 批量查重 (Check Existence)
                existing_ids = set()
                if hasattr(model_cls, 'md5_id') and ids_map:
                    id_list = list(ids_map.keys())
                    # 分块查询，防止 SQL 过长
                    chunk_size = 1000
                    for i in range(0, len(id_list), chunk_size):
                        chunk = id_list[i:i+chunk_size]
                        try:
                            # SELECT md5_id FROM table WHERE md5_id IN (...)
                            existing = session.query(model_cls.md5_id).filter(model_cls.md5_id.in_(chunk)).all()
                            existing_ids.update(row[0] for row in existing)
                        except Exception as e:
                            logger.error(f"查重查询失败: {e}")

                # 3. 内存过滤 (Memory Filter)
                new_objects = []
                for obj in orm_objects:
                    if hasattr(obj, 'md5_id') and obj.md5_id in existing_ids:
                        continue
                    new_objects.append(obj)
                
                if not new_objects:
                    continue

                # 4. 批量插入 (Insert Batch)
                try:
                    session.add_all(new_objects)
                    session.commit()
                    total_saved += len(new_objects)
                except IntegrityError:
                    session.rollback()
                    logger.warning(f"批量写入 {model_cls.__tablename__} 遇到冲突，降级为逐条写入...")
                    
                    # 降级：逐条写入 (Insert Only)
                    count = 0
                    for obj in new_objects:
                        try:
                            session.add(obj)
                            session.commit()
                            count += 1
                        except IntegrityError:
                            session.rollback()
                            # 忽略重复
                        except Exception as e:
                            session.rollback()
                            logger.error(f"单条写入失败: {e}")
                    total_saved += count
                    
                except Exception as e:
                    session.rollback()
                    logger.error(f"批量写入未知错误: {e}")
            
            return total_saved

        finally:
            session.close()

import logging
import time
from twisted.internet import threads, defer
from itemadapter import ItemAdapter
from sqlalchemy.sql import func
from .models import SessionLocal
from .models.crawl_status import CrawlStatus
from .models.spider_progress import SpiderProgress
from .models.crawl_data import CrawlData # Fallback
from .exceptions import DataValidationError

logger = logging.getLogger(__name__)

class DataCleaningPipeline:
    """数据清洗与校验层"""
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        # 基础清洗 (去除首尾空格)
        for k, v in adapter.items():
            if isinstance(v, str):
                adapter[k] = v.strip()
                
        return item

class UniversalBatchWritePipeline:
    """
    【通用异步批量写入管道】
    替代原有的 *DrugPipeline，根据 Item 动态识别 Model 并写入。
    """
    
    def __init__(self, buffer_size=50, timeout=2):
        self.buffer = []
        self.buffer_size = buffer_size
        self.timeout = timeout
        self.last_flush_time = time.time()
        
        # 数据库 Session 工厂
        self.session_maker = SessionLocal 
        
        # 使用 set 仅存储当前活跃的异步任务
        self.active_tasks = set()

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(
            buffer_size=settings.getint('BUFFER_THRESHOLD', 100),
            timeout=settings.getfloat('BUFFER_TIMEOUT_SEC', 2.0)
        )

    def process_item(self, item, spider):
        # 1. 过滤 None 或 状态 Item
        if item is None or isinstance(item, dict):
            # 如果是字典（通常是状态Item），交由下一个Pipeline处理
            return item

        # 2. 添加到 Buffer
        self.buffer.append(item)

        # 3. 检查是否满足写入条件
        if self._should_flush():
            self._trigger_flush()

        return item

    def _should_flush(self):
        """判断是否需要刷新"""
        has_data = len(self.buffer) > 0
        time_expired = (time.time() - self.last_flush_time) >= self.timeout
        size_reached = len(self.buffer) >= self.buffer_size
        return size_reached or (has_data and time_expired)

    def _trigger_flush(self):
        """触发异步写入任务"""
        items_to_write = self.buffer
        self.buffer = [] # 指向新列表
        self.last_flush_time = time.time()

        if not items_to_write:
            return

        logger.debug(f"🚀 触发异步写入: {len(items_to_write)} 条")
        df = threads.deferToThread(self._flush_buffer, items_to_write)
        
        self.active_tasks.add(df)
        df.addBoth(self._cleanup_task, df)
        df.addErrback(self._log_error)

    def _cleanup_task(self, result, df):
        """任务完成后的清理回调"""
        self.active_tasks.discard(df)
        return result

    def _log_error(self, failure):
        """错误日志回调"""
        logger.error(f"🔥 异步写入严重异常: {failure.getErrorMessage()}")
        return failure

    @defer.inlineCallbacks
    def close_spider(self, spider):
        """优雅关闭"""
        logger.info(f"⏳ 爬虫关闭中... 剩余 Buffer: {len(self.buffer)} | 进行中任务: {len(self.active_tasks)}")
        
        if self.buffer:
            self._trigger_flush()
        
        if self.active_tasks:
            yield defer.DeferredList(list(self.active_tasks))
            
        logger.info("✅ Pipeline 关闭完成：所有数据已安全落库。")

    def _get_model_class(self, item):
        """
        动态获取 Item 对应的 SQLAlchemy Model 类。
        优先调用 item.get_model_class()，其次查找 item['model_class']，最后回退到 CrawlData。
        """
        if hasattr(item, 'get_model_class'):
            return item.get_model_class()
        
        # 兼容旧逻辑或字典类型的 item
        if isinstance(item, dict) and 'model_class' in item:
            return item['model_class']
            
        return CrawlData

    def _create_orm_object(self, item, model_class):
        if not item: return None
        # 自动映射 Item 字段到 Model 字段
        model_fields = [c.key for c in model_class.__table__.columns]
        
        # ItemAdapter 统一处理 Item 对象和字典
        adapter = ItemAdapter(item)
        item_data = {k: v for k, v in adapter.items() if k in model_fields}
        
        return model_class(**item_data)

    def _flush_buffer(self, items):
        """执行数据库写入（运行在线程池中）"""
        session = self.session_maker()
        try:
            orm_objects = []
            for item in items:
                if item:
                    model = self._get_model_class(item)
                    obj = self._create_orm_object(item, model)
                    if obj:
                        orm_objects.append(obj)
            
            if not orm_objects: return

            session.add_all(orm_objects)
            session.commit()
            logger.info(f"💾 批量写入成功: {len(orm_objects)} 条")
            
        except Exception as e:
            session.rollback()
            logger.error(f"⚠️ 批量写入失败: {e}，正在降级为逐条写入...")
            self._fallback_single_write(session, orm_objects)
        finally:
            session.close()

    def _fallback_single_write(self, session, objects):
        count = 0
        for obj in objects:
            try:
                session.merge(obj)
                session.commit()
                count += 1
            except Exception as e:
                session.rollback()
                logger.error(f"❌ 单条写入丢弃: {e}")
        logger.info(f"🆗 降级处理完成: 挽回 {count}/{len(objects)} 条")


class CrawlStatusPipeline:
    """
    爬虫状态记录管道
    """
    
    def process_item(self, item, spider):
        # 检查是否为状态记录item
        if isinstance(item, dict) and item.get('_status_'):
            return threads.deferToThread(self._save_status, item, spider)
        return item
    
    def _save_status(self, status_item, spider):
        """保存采集状态 (运行在线程池中)"""
        session = SessionLocal()
        try:
            status = CrawlStatus(
                spider_name=status_item.get('spider_name', spider.name),
                crawl_id=status_item.get('crawl_id'),
                stage=status_item.get('stage'),
                page_no=status_item.get('page_no', 1),
                total_pages=status_item.get('total_pages', 0),
                page_size=status_item.get('page_size', 0),
                items_found=status_item.get('items_found', 0),
                items_stored=status_item.get('items_stored', 0),
                params=status_item.get('params'),
                api_url=status_item.get('api_url'),
                success=status_item.get('success', True),
                error_message=status_item.get('error_message'),
                parent_crawl_id=status_item.get('parent_crawl_id'),
                reference_id=status_item.get('reference_id')
            )
            session.add(status)
            self._update_progress(session, status_item, spider)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"❌ 保存采集状态失败: {e}")
        finally:
            session.close()
        return status_item
        
    def _update_progress(self, session, item, spider):
        """更新爬虫实时进度表"""
        try:
            spider_name = item.get('spider_name', spider.name)
            progress = session.query(SpiderProgress).filter_by(spider_name=spider_name).first()
            if not progress:
                progress = SpiderProgress(spider_name=spider_name)
                session.add(progress)
            
            progress.run_id = item.get('crawl_id', 'unknown')
            progress.status = 'running' if item.get('success', True) else 'error'
            
            page = item.get('page_no', 1)
            total = item.get('total_pages', 0)
            
            progress.total_tasks = total
            progress.completed_tasks = page
            if total > 0:
                progress.progress_percent = round((page / total) * 100, 2)
                
            progress.current_stage = item.get('stage', 'unknown')
            progress.items_scraped = session.query(CrawlStatus).filter_by(spider_name=spider_name).with_entities(func.sum(CrawlStatus.items_stored)).scalar() or 0
            
            desc = f"Stage: {progress.current_stage}"
            if total > 0:
                desc += f" | Page {page}/{total}"
            if item.get('error_message'):
                desc += f" | Error: {item.get('error_message')}"
            progress.current_item = desc
            
        except Exception as e:
            logger.error(f"⚠️ 更新实时进度失败: {e}")

    def close_spider(self, spider):
        pass

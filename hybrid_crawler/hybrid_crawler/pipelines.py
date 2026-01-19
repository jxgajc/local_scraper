import logging
import time
from twisted.internet import threads, defer
from itemadapter import ItemAdapter
from sqlalchemy.dialects.mysql import insert
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
    根据配置选择存储后端 (MySQL/ES)，并执行批量写入。
    """
    
    def __init__(self, settings):
        self.buffer = []
        self.buffer_size = settings.getint('BUFFER_THRESHOLD', 500)
        self.timeout = settings.getfloat('BUFFER_TIMEOUT_SEC', 1.5)
        self.last_flush_time = time.time()
        
        # 使用 set 仅存储当前活跃的异步任务
        self.active_tasks = set()
        
        # 初始化存储后端
        backend_type = settings.get('STORAGE_BACKEND', 'mysql').lower()
        logger.info(f"Initializing Storage Backend: {backend_type}")
        
        if backend_type == 'elasticsearch':
            from .storage.elasticsearch import ElasticsearchStorage
            self.storage = ElasticsearchStorage(
                hosts=settings.get('ES_HOSTS', ['http://localhost:9200']),
                user=settings.get('ES_USER'),
                password=settings.get('ES_PASSWORD'),
                index_prefix=settings.get('ES_INDEX_PREFIX', 'drug_store')
            )
        else:
            from .storage.mysql import MySQLStorage
            self.storage = MySQLStorage()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(settings=crawler.settings)

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

    def _flush_buffer(self, items):
        """执行数据库写入（运行在线程池中）"""
        try:
            count = self.storage.save_batch(items)
            logger.info(f"💾 批量写入成功: {count} 条 (新增)")
        except Exception as e:
            logger.error(f"⚠️ 批量写入失败: {e}")


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
        """
        更新爬虫实时进度表
        使用 MySQL 原生 Upsert (INSERT ... ON DUPLICATE KEY UPDATE) 
        彻底解决并发下的唯一键冲突和事务回滚问题
        """
        try:
            spider_name = item.get('spider_name', spider.name)
            run_id = item.get('crawl_id', 'unknown')
            
            # 1. 准备数据字典
            data = {
                'spider_name': spider_name,
                'run_id': run_id,
                'status': 'running' if item.get('success', True) else 'error',
                'completed_tasks': item.get('page_no', 1),
                'total_tasks': item.get('total_pages', 0),
                'current_stage': item.get('stage', 'unknown'),
                'updated_at': func.now()
            }

            # 2. 计算进度百分比
            if data['total_tasks'] > 0:
                data['progress_percent'] = round((data['completed_tasks'] / data['total_tasks']) * 100, 2)
            else:
                data['progress_percent'] = 0.0

            # 3. 计算 items_scraped (仍然需要查询一次，但这是读操作，不会锁表太久)
            # 注意：如果对性能要求极高，可以改为 Redis 计数或增量更新
            total_items = session.query(func.sum(CrawlStatus.items_stored))\
                .filter(CrawlStatus.spider_name == spider_name).scalar() or 0
            data['items_scraped'] = total_items

            # 4. 构建描述信息
            desc = f"Stage: {data['current_stage']}"
            if data['total_tasks'] > 0:
                desc += f" | Page {data['completed_tasks']}/{data['total_tasks']}"
            if item.get('error_message'):
                desc += f" | Error: {item.get('error_message')}"
            data['current_item'] = desc

            # 5. 执行原子 Upsert
            stmt = insert(SpiderProgress).values(data)
            
            # 指定发生冲突时需要更新的字段
            # 注意：spider_name 是唯一键，作为冲突判断依据
            update_dict = {
                'run_id': stmt.inserted.run_id,
                'status': stmt.inserted.status,
                'current_stage': stmt.inserted.current_stage,
                'items_scraped': stmt.inserted.items_scraped,
                'current_item': stmt.inserted.current_item,
                'updated_at': func.now()
            }

            # 关键修改：只有 list_page 阶段才更新主进度
            # 这样可以避免 detail_page 的进度 (如 1/1) 覆盖了 list_page 的总进度 (如 8/33)
            current_stage = item.get('stage', '')
            if 'list' in current_stage or current_stage == 'start_requests':
                 update_dict.update({
                    'completed_tasks': stmt.inserted.completed_tasks,
                    'total_tasks': stmt.inserted.total_tasks,
                    'progress_percent': stmt.inserted.progress_percent,
                 })
            
            upsert_stmt = stmt.on_duplicate_key_update(**update_dict)
            
            # 使用 execute 直接执行，绕过 ORM 对象缓存
            session.execute(upsert_stmt)
            
        except Exception as e:
            logger.error(f"⚠️ 更新实时进度失败: {e}")
            # 不抛出异常，保证主流程继续

    def close_spider(self, spider):
        pass

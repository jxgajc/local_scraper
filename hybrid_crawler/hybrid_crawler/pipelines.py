import logging
import time
import hashlib
from twisted.internet import threads
from itemadapter import ItemAdapter
from sqlalchemy.sql import func # 新增
from .models import SessionLocal, init_db
from .models.crawl_data import CrawlData
from .models.crawl_status import CrawlStatus
from .models.spider_progress import SpiderProgress # 新增
from .models.fujian_drug import FujianDrug
from .models.hainan_drug import HainanDrug
from .models.hebei_drug import HebeiDrug
from .models.liaoning_drug import LiaoningDrug
from .models.ningxia_drug import NingxiaDrug
from .models.guangdong_drug import GuangdongDrug
from .models.tianjin_drug import TianjinDrug
from .models.shandong_drug import ShandongDrug

from .models.nhsa_drug import NhsaDrug
from .exceptions import DataValidationError

logger = logging.getLogger(__name__)

class DataCleaningPipeline:
    """数据清洗与校验层"""
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        # 1. 必填项校验
        # if not adapter.get('url'):
        #     raise DataValidationError("Drop item: Missing URL")
            
        # # 2. 生成指纹
        # if not adapter.get('url_hash'):
        #     url = adapter.get('url')
        #     adapter['url_hash'] = hashlib.md5(url.encode('utf-8')).hexdigest()
            
        # 3. 基础清洗 (去除首尾空格)
        for k, v in adapter.items():
            if isinstance(v, str):
                adapter[k] = v.strip()
                
        return item

import logging
import time
from twisted.internet import threads, defer
from sqlalchemy.orm import sessionmaker
# from my_project.models import CrawlData, engine  # 导入你的模型

logger = logging.getLogger(__name__)

class AsyncBatchWritePipeline:
    """
    【最终改良版 - 异步批量写入层】
    特性：
    1. 无锁设计：利用 Twisted 线程池管理并发，避免 Buffer 爆仓。
    2. 自动清理：动态追踪活跃任务，无内存泄漏。
    3. 优雅退出：close_spider 使用 DeferredList 原生等待，彻底告别 time.sleep。
    """
    
    def __init__(self, buffer_size=50, timeout=2):
        self.buffer = []
        self.buffer_size = buffer_size
        self.timeout = timeout
        self.last_flush_time = time.time()
        
        # 数据库 Session 工厂
        self.session_maker = SessionLocal 
        
        # 【关键改良】使用 set 仅存储当前活跃的异步任务
        # 任务完成后会自动从中移除
        self.active_tasks = set()

    @classmethod
    def from_crawler(cls, crawler):
        # 建议：init_db() 最好放在 Spider 的 start_requests 或 main 中，而不是这里
        # init_db() 
        settings = crawler.settings
        return cls(
            buffer_size=settings.getint('BUFFER_THRESHOLD', 100),
            timeout=settings.getfloat('BUFFER_TIMEOUT_SEC', 2.0)
        )

    def process_item(self, item, spider):
        # 1. 如果 item 为 None，通常无需处理，直接返回
        if item is None:
            return item

        # 2. 添加到 Buffer
        self.buffer.append(item)

        # 3. 检查是否满足写入条件 (数量阈值 或 时间阈值)
        # 注意：这里去掉了 is_flushing 的判断。
        # 原因：如果写入慢而爬虫快，阻塞 flush 会导致 buffer 无限膨胀撑爆内存。
        # Twisted 的线程池会自动排队处理 flush 任务，比我们在内存囤积数据更安全。
        if self._should_flush():
            self._trigger_flush()

        return item

    def _should_flush(self):
        """判断是否需要刷新"""
        # 只有当 buffer 有数据时才检查时间
        has_data = len(self.buffer) > 0
        time_expired = (time.time() - self.last_flush_time) >= self.timeout
        size_reached = len(self.buffer) >= self.buffer_size
        
        return size_reached or (has_data and time_expired)

    def _trigger_flush(self):
        """触发异步写入任务"""
        # 1. 立即切片取出数据，清空 Buffer (原子操作)
        items_to_write = self.buffer
        self.buffer = [] # 指向新列表
        self.last_flush_time = time.time()

        if not items_to_write:
            return

        # 2. 发起异步任务
        logger.debug(f"🚀 触发异步写入: {len(items_to_write)} 条")
        df = threads.deferToThread(self._flush_buffer, items_to_write)
        
        # 3. 【关键】追踪任务
        self.active_tasks.add(df)
        
        # 4. 【关键】添加回调：任务结束(无论成功失败)后，从集合中移除自己
        # 使用 addBoth 确保即使报错也能清理
        df.addBoth(self._cleanup_task, df)
        
        # 5. 添加错误日志回调
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
        """
        【最终改良版关闭逻辑】
        """
        logger.info(f"⏳ 爬虫关闭中... 剩余 Buffer: {len(self.buffer)} | 进行中任务: {len(self.active_tasks)}")
        
        # 1. 如果 Buffer 里还有没写完的，发起最后一次异步写入
        if self.buffer:
            self._trigger_flush()
        
        # 2. 【核心】等待所有活跃任务完成
        # DeferredList 会等待列表里所有的 Deferred 变为 called 状态
        if self.active_tasks:
            yield defer.DeferredList(list(self.active_tasks))
            
        logger.info("✅ Pipeline 关闭完成：所有数据已安全落库。")

    # --- 以下业务逻辑保持不变 ---

    def _get_model_class(self, item):
        return CrawlData

    def _create_orm_object(self, item, model_class):
        if not item: return None
        model_fields = [c.key for c in model_class.__table__.columns]
        item_data = {k: v for k, v in item.items() if k in model_fields}
        return model_class(**item_data)

    def _flush_buffer(self, items):
        """执行数据库写入（运行在线程池中）"""
        session = self.session_maker()
        try:
            orm_objects = []
            for item in items:
                # 再次过滤，确保安全
                if item:
                    model = self._get_model_class(item)
                    obj = self._create_orm_object(item, model)
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

    # 如果需要自定义字段映射，可以重写 _create_orm_object 方法
    # def _create_orm_object(self, item, model_class):
    #     # 自定义映射逻辑
    #     return model_class(
    #         store_name=item.get('name'),
    #         address=item.get('addr'),
    #         contact=item.get('phone'),
    #         # 其他字段映射
    #     )

class HainanDrugPipeline(AsyncBatchWritePipeline):
    """海南药店数据写入管道"""
    def _get_model_class(self, item):
        return HainanDrug

class FujianDrugPipeline(AsyncBatchWritePipeline):
    """福建药品数据写入管道"""
    def _get_model_class(self, item):
        return FujianDrug

    # 字段映射已经在爬虫的_create_item方法中完成，这里可以使用默认的映射
    # 如果需要额外的字段转换，可以重写 _create_orm_object 方法
class HebeiDrugPipeline(AsyncBatchWritePipeline):
    """河北药品数据写入管道"""
    def _get_model_class(self, item):
        return HebeiDrug

class LiaoningDrugPipeline(AsyncBatchWritePipeline):
    """辽宁药品数据写入管道"""
    def _get_model_class(self, item):
        return LiaoningDrug

class NingxiaDrugPipeline(AsyncBatchWritePipeline):
    """福建药品数据写入管道"""
    def _get_model_class(self, item):
        return NingxiaDrug

class GuangdongDrugPipeline(AsyncBatchWritePipeline):
    """广东药品数据写入管道"""
    def _get_model_class(self, item):
        return GuangdongDrug

class TianjinDrugPipeline(AsyncBatchWritePipeline):
    """广东药品数据写入管道"""
    def _get_model_class(self, item):
        return TianjinDrug

class NhsaDrugPipeline(AsyncBatchWritePipeline):
    """国家医保药品数据写入管道"""
    def _get_model_class(self, item):
        return NhsaDrug

class ShandongDrugPipeline(AsyncBatchWritePipeline):
    """国家医保药品数据写入管道"""
    def _get_model_class(self, item):
        return ShandongDrug
    # 字段映射已经在爬虫的_create_item方法中完成，这里可以使用默认的映射
    # 如果需要额外的字段转换，可以重写 _create_orm_object 方法


class CrawlStatusPipeline:
    """
    爬虫状态记录管道
    用于记录每个爬虫的采集过程和参数，用于数据完整性验证
    接收特殊的状态item，格式为 {'_status_': True, ...}
    
    【改良版】：使用 deferToThread 异步写入，避免阻塞 Reactor
    """
    
    def process_item(self, item, spider):
        # 检查是否为状态记录item
        if isinstance(item, dict) and item.get('_status_'):
            # 返回 Deferred，Scrapy 会等待其完成
            return threads.deferToThread(self._save_status, item, spider)
        return item
    
    def _save_status(self, status_item, spider):
        """保存采集状态 (运行在线程池中)"""
        # 每个线程独立的 Session
        session = SessionLocal()
        try:
            # 1. 保存历史审计日志 (Append Only)
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
            
            # 2. 更新实时进度 (Upsert)
            self._update_progress(session, status_item, spider)
            
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"❌ 保存采集状态失败: {e}")
        finally:
            session.close()
            
        # 必须返回 item 以供后续 Pipeline 使用
        return status_item
        
    def _update_progress(self, session, item, spider):
        """更新爬虫实时进度表"""
        try:
            spider_name = item.get('spider_name', spider.name)
            
            # 尝试查询现有记录
            progress = session.query(SpiderProgress).filter_by(spider_name=spider_name).first()
            if not progress:
                progress = SpiderProgress(spider_name=spider_name)
                session.add(progress)
            
            # 更新字段
            progress.run_id = item.get('crawl_id', 'unknown')
            progress.status = 'running' if item.get('success', True) else 'error'
            
            # 计算进度
            page = item.get('page_no', 1)
            total = item.get('total_pages', 0)
            
            progress.total_tasks = total
            progress.completed_tasks = page
            if total > 0:
                progress.progress_percent = round((page / total) * 100, 2)
                
            progress.current_stage = item.get('stage', 'unknown')
            progress.items_scraped = session.query(CrawlStatus).filter_by(spider_name=spider_name).with_entities(func.sum(CrawlStatus.items_stored)).scalar() or 0
            
            # 构造分层描述信息
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
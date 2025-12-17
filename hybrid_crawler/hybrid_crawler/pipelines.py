import logging
import time
import hashlib
from twisted.internet import threads
from itemadapter import ItemAdapter
from .models import SessionLocal, init_db
from .models.crawl_data import CrawlData
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

class AsyncBatchWritePipeline:
    """
    【异步批量写入层】
    核心机制：
    1. Buffer: 内存中暂存 Item。
    2. DeferToThread: 将耗时的 DB 写入操作扔到线程池，避免阻塞 Scrapy 的 Reactor。
    3. Fallback: 批量失败时自动降级。
    
    子类可以通过重写 `_get_model_class` 和 `_create_orm_object` 方法来支持不同的模型
    """
    def __init__(self, buffer_size=50, timeout=2):
        self.buffer = []
        self.buffer_size = buffer_size
        self.timeout = timeout
        self.last_flush = time.time()
        self.session_maker = SessionLocal
        self.is_flushing = False  # 新增：跟踪是否正在执行flush操作

    @classmethod
    def from_crawler(cls, crawler):
        init_db() # 确保表存在
        settings = crawler.settings
        return cls(
            buffer_size=settings.getint('BUFFER_THRESHOLD', 100),
            timeout=settings.getfloat('BUFFER_TIMEOUT_SEC', 2.0)
        )

    def process_item(self, item, spider):
        # 防止None被添加到buffer中
        if item is None:
            # 如果buffer中有数据，手动触发flush
            if self._should_flush() and not self.is_flushing:
                self.is_flushing = True
                # 创建副本并清空buffer，确保数据一致性
                items_to_flush = self.buffer.copy()
                self.buffer.clear()
                self.last_flush = time.time()
                
                # 异步调用 _flush_buffer
                df = threads.deferToThread(self._flush_buffer, items_to_flush)
                df.addCallback(self._on_flush_complete)
                df.addErrback(self._on_flush_error)
            return item
        
        self.buffer.append(item)
        # 检查是否达到 数量阈值 或 时间阈值
        if self._should_flush() and not self.is_flushing:
            self.is_flushing = True
            # 创建副本并清空buffer，确保数据一致性
            items_to_flush = self.buffer.copy()
            self.buffer.clear()
            self.last_flush = time.time()
            
            # 异步调用 _flush_buffer
            df = threads.deferToThread(self._flush_buffer, items_to_flush)
            df.addCallback(self._on_flush_complete)
            df.addErrback(self._on_flush_error)
        return item

    def _should_flush(self):
        return (len(self.buffer) >= self.buffer_size) or (time.time() - self.last_flush >= self.timeout and self.buffer)

    def _get_model_class(self, item):
        """获取对应的模型类，子类可以重写此方法"""
        return CrawlData

    def _create_orm_object(self, item, model_class):
        """创建ORM对象，子类可以重写此方法进行自定义映射"""
        # 确保item不为None
        if item is None:
            raise ValueError("Cannot create ORM object from None item")
            
        # 默认实现：使用字典解包，自动将item中的字段映射到模型
        # 只包含模型中定义的字段
        model_fields = [c.key for c in model_class.__table__.columns]
        item_data = {k: v for k, v in item.items() if k in model_fields}
        return model_class(**item_data)

    def _flush_buffer(self, items):
        """在独立线程中执行"""
        session = self.session_maker()
        try:
            orm_objects = []
            valid_items = []
            
            # 过滤掉None值
            for item in items:
                if item is not None:
                    valid_items.append(item)
                    model_class = self._get_model_class(item)
                    orm_obj = self._create_orm_object(item, model_class)
                    orm_objects.append(orm_obj)
            
            # 如果没有有效数据，直接返回
            if not valid_items:
                logger.info("ℹ️ 没有有效数据需要写入")
                return
            
            # 尝试批量写入
            session.add_all(orm_objects)
            session.commit()
            logger.info(f"✅ 成功批量写入 {len(valid_items)} 条数据")
        except Exception as e:
            session.rollback()
            logger.error(f"⚠️ 批量写入失败: {e}，正在尝试降级为逐条写入...")
            self._fallback_single_write(session, orm_objects)
        finally:
            session.close()

    def _fallback_single_write(self, session, objects):
        """降级策略：逐条写入，隔离脏数据"""
        success = 0
        for obj in objects:
            try:
                session.merge(obj) # 使用 merge 避免主键重复报错
                session.commit()
                success += 1
            except Exception as e:
                session.rollback()
                # 尝试获取对象的标识信息
                obj_id = getattr(obj, 'url_hash', getattr(obj, 'id', 'Unknown'))
                logger.error(f"❌ 单条写入失败 (ID: {obj_id}): {e}")
        logger.info(f"🆗 降级写入完成: 成功 {success} / 总数 {len(objects)}")

    def _on_flush_complete(self, result):
        """异步写入完成后的回调"""
        self.is_flushing = False
        # 检查是否有新数据需要处理
        if self._should_flush():
            self.process_item(None, None)  # 触发下一次flush

    def _on_flush_error(self, failure):
        """异步写入失败后的回调"""
        logger.error(f"🔥 异步写入线程严重异常: {failure}")
        self.is_flushing = False

    def _handle_error(self, failure):
        """保留旧的错误处理方法，确保兼容性"""
        logger.error(f"🔥 异步写入线程严重异常: {failure}")

    def close_spider(self, spider):
        """爬虫关闭时，强制刷新剩余 Buffer"""
        # 确保所有数据都被处理
        if self.buffer:
            self._flush_buffer(self.buffer)
        # 等待可能正在进行的异步操作完成
        import time
        while self.is_flushing:
            time.sleep(0.1)

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
import uuid
import time
import logging
from sqlalchemy import text
from ..models import SessionLocal

class SpiderStatusMixin:
    """
    爬虫状态上报 Mixin
    提供标准化的方法来生成和提交状态数据，简化 Spider 代码。
    同时集成补充采集功能。
    """

    def report_status(self, stage, crawl_id=None, **kwargs):
        """
        通用状态上报方法
        """
        status_item = {
            '_status_': True,
            'crawl_id': crawl_id or str(uuid.uuid4()),
            'stage': stage,
            'spider_name': self.name,
            'success': True,
        }
        status_item.update(kwargs)
        return status_item

    def report_list_page(self, crawl_id, page_no, total_pages, items_found, params, api_url, **kwargs):
        """
        上报列表页采集状态
        """
        return self.report_status(
            stage='list_page',
            crawl_id=crawl_id,
            page_no=page_no,
            total_pages=total_pages,
            items_found=items_found,
            params=params,
            api_url=api_url,
            **kwargs
        )

    def report_detail_page(self, crawl_id, page_no, items_found, params, api_url, parent_crawl_id, reference_id=None, **kwargs):
        """
        上报详情页/嵌套页采集状态
        """
        return self.report_status(
            stage='detail_page',
            crawl_id=crawl_id,
            page_no=page_no,
            items_found=items_found,
            params=params,
            api_url=api_url,
            parent_crawl_id=parent_crawl_id,
            reference_id=reference_id,
            **kwargs
        )

    def report_error(self, stage, error_msg, crawl_id=None, params=None, api_url=None, **kwargs):
        """
        上报错误状态
        """
        return self.report_status(
            stage=stage,
            crawl_id=crawl_id or str(uuid.uuid4()),
            success=False,
            error_message=str(error_msg),
            params=params,
            api_url=api_url,
            **kwargs
        )

    # ==========================================
    # 补充采集相关方法 (Recrawl Logic)
    # ==========================================

    def get_logger(self):
        if hasattr(self, 'spider_log'):
            return self.spider_log
        if hasattr(self, 'logger'):
            return self.logger
        return logging.getLogger(self.name)

    def find_missing(self):
        """
        找出缺失的数据
        默认实现使用 self.recrawl_config 和 self.fetch_all_ids_from_api (classmethod)
        """
        if not hasattr(self, 'recrawl_config'):
            return {}
        
        logger = self.get_logger()
        config = self.recrawl_config
        table_name = config.get('table_name')
        unique_id = config.get('unique_id')
        
        if not table_name or not unique_id:
            logger.warning("recrawl_config incomplete")
            return {}

        db = SessionLocal()
        try:
             logger.info(f"从数据库获取 {table_name} 表中的现有 {unique_id}...")
             # Get existing IDs
             sql = text(f"SELECT DISTINCT {unique_id} FROM {table_name}")
             result = db.execute(sql)
             existing_ids = {str(row[0]) for row in result if row[0] is not None}
             logger.info(f"数据库中已有 {len(existing_ids)} 条记录")
             
             # Fetch all from API
             # Note: fetch_all_ids_from_api is expected to be a classmethod or staticmethod
             logger.info(f"从官网API获取所有 {unique_id}...")
             api_data = self.__class__.fetch_all_ids_from_api(logger=logger)
             logger.info(f"官网API共有 {len(api_data)} 条记录")
             
             missing_data = {k: v for k, v in api_data.items() if k not in existing_ids}
             logger.info(f"发现 {len(missing_data)} 条缺失数据")
             return missing_data
        except Exception as e:
            logger.error(f"检查缺失数据失败: {e}")
            return {}
        finally:
             db.close()

    def recrawl_with_ids(self, missing_ids):
        """
        根据 ID 或数据字典进行补采
        If missing_ids is a dict, passed directly.
        If missing_ids is a list/set, we refetch all data to recover base info.
        """
        if not missing_ids:
            return 0

        logger = self.get_logger()
        db = SessionLocal()
        try:
             if isinstance(missing_ids, (list, set, tuple)):
                 # Check if we can recover data or if spider supports ID-only recrawl
                 logger.info("收到ID列表，正在重新获取API数据以恢复完整信息...")
                 all_data = self.__class__.fetch_all_ids_from_api(logger=logger)
                 # Filter by the requested IDs
                 missing_data = {k: v for k, v in all_data.items() if k in missing_ids}
                 if len(missing_data) < len(missing_ids):
                     logger.warning(f"请求补采 {len(missing_ids)} 条，但只在API中找到 {len(missing_data)} 条")
                 
                 return self.__class__.recrawl_by_ids(missing_data, db, logger)
             else:
                 # Assume it is the dict returned by find_missing
                 return self.__class__.recrawl_by_ids(missing_ids, db, logger)
        except Exception as e:
            logger.error(f"补采执行失败: {e}")
            return 0
        finally:
             db.close()

    def recrawl(self):
        """
        执行完整的补采流程：检查 -> 补采
        """
        missing = self.find_missing()
        if missing:
            return self.recrawl_with_ids(missing)
        return 0

    def sync_to_production(self):
        """
        同步到生产环境 (占位符)
        """
        pass

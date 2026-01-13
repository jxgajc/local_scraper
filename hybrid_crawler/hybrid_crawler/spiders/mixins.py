import uuid
import logging
import asyncio

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
    # 补充采集相关方法 (委托给 RecrawlManager)
    # ==========================================

    def get_logger(self):
        if hasattr(self, 'spider_log'):
            return self.spider_log
        if hasattr(self, 'logger'):
            return self.logger
        return logging.getLogger(self.name)

    async def find_missing_async(self):
        """异步找出缺失的数据"""
        from ..recrawl.manager import RecrawlManager
        return await RecrawlManager.find_missing(self.name)

    async def recrawl_with_ids_async(self, missing_ids):
        """异步根据 ID 或数据字典进行补采"""
        from ..recrawl.manager import RecrawlManager
        return await RecrawlManager.recrawl(self.name, missing_ids)

    async def recrawl_async(self):
        """异步执行完整的补采流程"""
        from ..recrawl.manager import RecrawlManager
        return await RecrawlManager.full_recrawl(self.name)

    # 同步包装方法（用于非异步上下文）
    def find_missing(self):
        """同步找出缺失的数据"""
        return asyncio.get_event_loop().run_until_complete(self.find_missing_async())

    def recrawl_with_ids(self, missing_ids):
        """同步根据 ID 或数据字典进行补采"""
        return asyncio.get_event_loop().run_until_complete(self.recrawl_with_ids_async(missing_ids))

    def recrawl(self):
        """同步执行完整的补采流程"""
        return asyncio.get_event_loop().run_until_complete(self.recrawl_async())

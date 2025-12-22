from ast import Param
from .base_spiders import BaseRequestSpider
from ..models.ningxia_drug import NingxiaDrugItem
from urllib.parse import urlencode
from ..utils.logger_utils import get_spider_logger
import json
import scrapy
import time
import uuid

# http://ylbzj.hebei.gov.cn/category/162
class NingxiaDrugSpider(BaseRequestSpider):
    """
    宁夏医保局药品订单爬虫
    目标: 直接获取药品订单列表数据
    Target: https://nxyp.ylbz.nx.gov.cn
    """
    name = "ningxia_drug_spider"
    
    # 接口地址
    list_api_url = "https://nxyp.ylbz.nx.gov.cn/cms/recentPurchaseDetail/getRecentPurchaseDetailData.html" 
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = get_spider_logger(self.name)
        self.crawl_id = str(uuid.uuid4())
        self.logger.info(f"🚀 爬虫初始化完成，crawl_id: {self.crawl_id}")

    custom_settings = {
        'CONCURRENT_REQUESTS': 4, # 根据服务器压力适当调整
        'DOWNLOAD_DELAY': 5,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': '*/*',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'User-Agent': 'PostmanRuntime-ApipostRuntime/1.1.0',
            'prodType': '2'
        },
        # 使用数据管道
        'ITEM_PIPELINES': {
            'hybrid_crawler.pipelines.DataCleaningPipeline': 300,        # 清洗
            'hybrid_crawler.pipelines.CrawlStatusPipeline': 350,         # 状态监控 (新增)
            'hybrid_crawler.pipelines.NingxiaDrugPipeline': 400,         # 入库
        }
    }

    def start_requests(self):
        """构造初始的POST请求"""
        payload = {
            "_search": "false",
            "page": 1,
            "rows": 1000,
            "sidx": "",
            "sord": "asc"
        }
        
        self.logger.info(f"📋 开始采集药品订单列表，初始payload: {json.dumps(payload)}")
        
        # 上报开始采集状态
        yield {
            '_status_': True,
            'crawl_id': self.crawl_id,
            'stage': 'start_requests',
            'page_no': 1,
            'params': payload,
            'api_url': self.list_api_url,
            'success': True
        }

        form_data_str = {k: str(v) for k, v in payload.items()}
        yield scrapy.FormRequest(
            url=self.list_api_url,
            method='POST',
            formdata=form_data_str,
            callback=self.parse_logic,
            meta={'payload': payload, 'crawl_id': self.crawl_id}, # 传递 payload 和 crawl_id
            dont_filter=True
        )

    def parse_logic(self, response):
        """处理药品列表初始响应：处理第一页数据 + 生成后续页码请求"""
        page_crawl_id = str(uuid.uuid4())
        parent_crawl_id = response.meta['crawl_id']
        current_payload = response.meta['payload']
        
        try:
            res_json = json.loads(response.text)
            
            total_pages = int(res_json.get("total", 0))
            records = res_json.get("rows", [])
            current = int(res_json.get("page", 1))
            total_records = int(res_json.get("records", 0))

            self.logger.info(f"📄 订单列表页面 [{current}/{total_pages}] - 发现 {len(records)} 条记录 (总计: {total_records})")

            # 上报页面采集状态
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': current,
                'total_pages': total_pages,
                'items_found': len(records),
                'params': current_payload,
                'api_url': self.list_api_url,
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }

            item_count = 0
            # 1. 处理当前页的每一条药品订单数据 -> 直接创建Item
            for order_item in records:
                yield self._create_item(order_item, current)
                item_count += 1

            # 更新页面采集状态
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': current,
                'total_pages': total_pages,
                'items_found': len(records),
                'items_stored': item_count,
                'params': current_payload,
                'api_url': self.list_api_url,
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }

            # 2. 生成剩余页码请求 (从第2页开始)
            # 注意：此处一次性生成所有请求，并发量由 CONCURRENT_REQUESTS 控制
            if current < total_pages:
                self.logger.info(f"🔄 准备调度后续页面 (2-{total_pages})")
                
                for page in range(2, total_pages + 1):
                    next_payload = current_payload.copy()
                    next_payload['page'] = page
                    
                    form_data_str = {k: str(v) for k, v in next_payload.items()}
                    yield scrapy.FormRequest(
                        url=self.list_api_url,
                        method='POST',
                        formdata=form_data_str,
                        callback=self.parse_list_page,
                        meta={
                            'payload': next_payload, 
                            'parent_crawl_id': parent_crawl_id, # 列表页的父级是 Spider 的 crawl_id
                            'page_num': page
                        }, 
                        dont_filter=True
                    )

        except Exception as e:
            self.logger.error(f"❌ 列表页解析失败 (Page 1): {e}", exc_info=True)
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': 1,
                'params': current_payload,
                'api_url': self.list_api_url,
                'success': False,
                'error_message': str(e),
                'parent_crawl_id': parent_crawl_id
            }

    def parse_list_page(self, response):
        """处理后续药品列表页"""
        page_crawl_id = str(uuid.uuid4())
        parent_crawl_id = response.meta['parent_crawl_id']
        current_payload = response.meta['payload']
        page_num = response.meta['page_num']
        
        try:
            res_json = json.loads(response.text)
            records = res_json.get("rows", [])
            # 接口返回的 page 字段可能为字符串
            api_page = int(res_json.get('page', page_num))
            total_pages = int(res_json.get("total", 0))
            
            self.logger.info(f"📄 订单列表页面 [{api_page}/{total_pages}] - 发现 {len(records)} 条记录")
            
            # 上报页面采集状态
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': api_page,
                'total_pages': total_pages,
                'items_found': len(records),
                'params': current_payload,
                'api_url': self.list_api_url,
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }
            
            item_count = 0
            for order_item in records:
                yield self._create_item(order_item, api_page)
                item_count += 1
            
            # 更新页面采集状态
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': api_page,
                'total_pages': total_pages,
                'items_found': len(records),
                'items_stored': item_count,
                'params': current_payload,
                'api_url': self.list_api_url,
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }
                
        except Exception as e:
            self.logger.error(f"❌ 分页解析失败 Page {page_num}: {e}", exc_info=True)
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': page_num,
                'params': current_payload,
                'api_url': self.list_api_url,
                'success': False,
                'error_message': str(e),
                'parent_crawl_id': parent_crawl_id
            }

    def _create_item(self, order_item, page_num=1):
        """
        构建 NingxiaDrugItem
        :param order_item: 从API获取的订单信息 (Dict)
        :param page_num: 采集页码
        """
        item = NingxiaDrugItem()
        
        # 1. 设置订单信息字段
        for field_name in item.fields:
            if field_name in ['md5_id', 'collect_time', 'url', 'url_hash', 'page_num']:
                continue  # 跳过需要单独处理的字段
            if field_name in order_item:
                item[field_name] = order_item[field_name]
        
        # 2. 设置URL字段
        item['url'] = f"{self.list_api_url}?page={page_num}"
        
        # 3. 设置页码
        item['page_num'] = page_num
        
        # 4. 生成MD5唯一ID和采集时间
        item.generate_md5_id()
        
        return item
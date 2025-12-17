from ast import Param
from .base_spiders import BaseRequestSpider
from ..models.ningxia_drug import NingxiaDrugItem
from urllib.parse import urlencode
import json
import scrapy
import time

# http://ylbzj.hebei.gov.cn/category/162
class NingxiaDrugSpider(BaseRequestSpider):
    """
    宁夏医保局药品订单爬虫
    目标: 直接获取药品订单列表数据
    """
    name = "ningxia_drug_spider"
    
    # ⚠️ 请确认第一个 curl 对应的真实 URL，并替换下方地址
    list_api_url = "https://nxyp.ylbz.nx.gov.cn/cms/recentPurchaseDetail/getRecentPurchaseDetailData.html" 
    

    
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
            'hybrid_crawler.pipelines.NingxiaDrugPipeline': 400,           # 入库
        }
    }

    def start_requests(self):
        """构造初始的GET请求"""
        payload = {
            "_search": "false",
            "page": 1,
            "rows": 1000,  # 修改此处 size 为 100
            "sidx": "",
            "sord": "asc"
        }
        # query_string = urlencode(payload)
        # full_url = f"{self.list_api_url}?{query_string}"
        # 发起第一页请求（不需要cookie）
        form_data_str = {k: str(v) for k, v in payload.items()}
        yield scrapy.FormRequest(
            url=self.list_api_url,
            method='POST',
            formdata=form_data_str,
            callback=self.parse_logic,
            meta={'payload': payload}, # 传递 payload 以便后续翻页使用
            dont_filter=True
        )

    def parse_logic(self, response):
        """处理药品列表初始响应：处理第一页数据 + 生成后续页码请求"""
        try:
            res_json = json.loads(response.text)
            
            total_pages = int(res_json.get("total", 0))
            records = res_json.get("rows", [])
            current = res_json.get("page", 1)

            self.logger.info(f"总页数: {total_pages}, 当前页: {current}, 当前页记录数: {len(records)}")

            # 1. 处理当前页的每一条药品订单数据 -> 直接创建Item
            for order_item in records:
                yield self._create_item(order_item, current)

            # 2. 生成剩余页码请求 (从第2页开始)
            current_payload = response.meta['payload']
            for page in range(2, total_pages + 1):
                next_payload = current_payload.copy()
                next_payload['page'] = page
                
                form_data_str = {k: str(v) for k, v in next_payload.items()}
                yield scrapy.FormRequest(
                    url=self.list_api_url,
                    method='POST',
                    formdata=form_data_str,
                    callback=self.parse_list_page,
                    meta={'payload': next_payload}, # 传递 payload 以便后续翻页使用
                    dont_filter=True
                )

        except Exception as e:
            self.logger.error(f"列表页解析失败: {e} | Response: {response.text[:200]}")

    def parse_list_page(self, response):
        """处理后续药品列表页"""
        try:
            res_json = json.loads(response.text)
            records = res_json.get("rows", [])
            page_num = res_json.get('page', 1)
            
            for order_item in records:
                yield self._create_item(order_item, page_num)
                
        except Exception as e:
            self.logger.error(f"分页解析失败 Page {res_json.get('page', 'unknown')}: {e}")



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
        

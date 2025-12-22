from ast import Param
from .base_spiders import BaseRequestSpider
from ..models.hebei_drug import HebeiDrugItem
from ..utils.logger_utils import get_spider_logger
from urllib.parse import urlencode
import json
import scrapy
import time
import uuid

# http://ylbzj.hebei.gov.cn/category/162
class HebeiDrugSpider(BaseRequestSpider):
    """
    河北医保局药品及采购医院爬虫
    目标: 先获取药品列表，再根据 prodCode 获取采购该药品的医院信息
    """
    name = "hebei_drug_spider"
    
    # API Endpoints
    list_api_url = "https://ylbzj.hebei.gov.cn/templates/default_pc/syyypqxjzcg/queryPubonlnDrudInfoList" 
    hospital_api_url = "https://ylbzj.hebei.gov.cn/templates/default_pc/syyypqxjzcg/queryProcurementMedinsList"
    
    # 存储cookie
    cookies = {}
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spider_log = get_spider_logger(self.name)
        self.crawl_id = str(uuid.uuid4())
        self.spider_log.info(f"🚀 爬虫初始化完成，crawl_id: {self.crawl_id}")
    

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
        'ITEM_PIPELINES': {
            'hybrid_crawler.pipelines.DataCleaningPipeline': 300,        # 清洗
            'hybrid_crawler.pipelines.CrawlStatusPipeline': 350,         # 状态监控 (新增)
            'hybrid_crawler.pipelines.HebeiDrugPipeline': 400,           # 入库
        }
    }

    def start_requests(self):
        """构造初始的GET请求"""
        payload = {
            "pageNo": 1,
            "pageSize": 1000, 
            "prodName": "",
            "prodentpName": ""
        }
        query_string = urlencode(payload)
        full_url = f"{self.list_api_url}?{query_string}"
        
        self.spider_log.info(f"📋 开始采集药品列表，初始payload: {json.dumps(payload)}")
        
        # 上报开始采集状态
        # yield {
        #     '_status_': True,
        #     'crawl_id': self.crawl_id,
        #     'stage': 'start_requests',
        #     'page_no': 1,
        #     'params': payload,
        #     'api_url': self.list_api_url,
        #     'success': True
        # }
        
        # 发起第一页请求（不需要cookie）
        yield scrapy.Request(
            url=full_url,
            method='GET',
            callback=self.parse_logic,
            meta={'payload': payload, 'crawl_id': self.crawl_id},
            dont_filter=True
        )

    def parse_logic(self, response):
        """处理药品列表初始响应：处理第一页数据 + 生成后续页码请求"""
        page_crawl_id = str(uuid.uuid4())
        parent_crawl_id = response.meta['crawl_id']
        current_payload = response.meta['payload']
        
        try:
            # 更新cookies
            if response.headers.getlist('Set-Cookie'):
                self._update_cookies(response)
            
            res_json = json.loads(response.text)
            
            data_block = res_json.get("data", {})
            total_pages = int(data_block.get("pages", 0))
            records = data_block.get("list", [])
            current = data_block.get("pageNo", 1)
            page_size = data_block.get("pageSize", 1000)

            self.spider_log.info(f"📄 列表页面 [{current}/{total_pages}] - 发现 {len(records)} 条药品记录")
            
            # 上报页面采集状态
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': current,
                'total_pages': total_pages,
                'page_size': page_size,
                'items_found': len(records),
                'params': current_payload,
                'api_url': self.list_api_url,
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }

            item_count = 0
            # 1. 处理当前页的每一条药品数据 -> 发起详情请求
            for drug_item in records:
                # 传入 page_crawl_id 作为 parent_crawl_id
                for request in self._request_hospital_detail(drug_item, current, page_crawl_id):
                    yield request
                    item_count += 1

            # 更新页面采集状态，记录触发的详情页请求数量
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
            if current < total_pages:
                self.spider_log.info(f"🔄 准备采集下一页列表 [{current + 1}/{total_pages}]")
                for page in range(2, total_pages + 1):
                    next_payload = current_payload.copy()
                    next_payload['pageNo'] = page
                    
                    query_string = urlencode(next_payload)
                    full_url = f"{self.list_api_url}?{query_string}"
                    
                    yield scrapy.Request(
                        url=full_url,
                        method='GET',
                        callback=self.parse_list_page,
                        meta={
                            'page_num': page, 
                            'payload': next_payload, 
                            'parent_crawl_id': parent_crawl_id # 列表页的父级是 root crawl_id
                        },
                        dont_filter=True
                    )

        except Exception as e:
            self.spider_log.error(f"❌ 列表页面解析失败 (Page 1): {e}", exc_info=True)
            
            # 上报异常状态
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
        parent_crawl_id = response.meta.get('parent_crawl_id')
        page_num = response.meta.get('page_num', 1)
        current_payload = response.meta.get('payload', {})
        
        try:
            if response.headers.getlist('Set-Cookie'):
                self._update_cookies(response)
            
            res_json = json.loads(response.text)
            data_block = res_json.get("data", {})
            records = data_block.get("list", [])
            total_pages = data_block.get("pages", 0)
            page_size = data_block.get("pageSize", 1000)
            
            self.spider_log.info(f"📄 列表页面 [{page_num}/{total_pages}] - 发现 {len(records)} 条药品记录")
            
            # 上报页面采集状态
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': page_num,
                'total_pages': total_pages,
                'page_size': page_size,
                'items_found': len(records),
                'params': current_payload,
                'api_url': self.list_api_url,
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }
            
            item_count = 0
            for drug_item in records:
                for request in self._request_hospital_detail(drug_item, page_num, page_crawl_id):
                    yield request
                    item_count += 1
            
            # 更新页面采集状态
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': page_num,
                'total_pages': total_pages,
                'items_found': len(records),
                'items_stored': item_count,
                'params': current_payload,
                'api_url': self.list_api_url,
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }
                
        except Exception as e:
            self.spider_log.error(f"❌ 分页解析失败 Page {page_num}: {e}", exc_info=True)
            
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

    def _request_hospital_detail(self, drug_item, page_num, parent_crawl_id):
        """构造获取医院详情的请求，将 drug_item 传递下去"""
        prodentp_code = drug_item.get("prodentpCode")
        prod_code = drug_item.get("prodCode")
        
        if not prodentp_code:
            self.spider_log.warning(f"⚠️ 缺少 prodentpCode，跳过详情查询: {drug_item.get('prodName')}")
            return

        # 构造详情页 Payload
        payload = {
            "pageNo": 1,
            "pageSize": 1000,
            "prodCode": prod_code,
            "prodEntpCode": prodentp_code,
            "isPublicHospitals": ""
        }
        query_string = urlencode(payload)
        full_url = f"{self.hospital_api_url}?{query_string}"
        
        # 这里的 meta 非常重要，用来传递第一步获取的 info 和页码
        yield scrapy.Request(
            url=full_url,
            method='GET',
            callback=self.parse_detail,
            meta={
                'drug_info': drug_item, 
                'page_num': page_num,
                'parent_crawl_id': parent_crawl_id,
                'payload': payload
            }, 
            cookies=self.cookies if self.cookies else None,
            dont_filter=True
        )

    def parse_detail(self, response):
        """处理医院详情响应，合并数据并生成 Item"""
        drug_info = response.meta['drug_info']
        page_num = response.meta.get('page_num', 1)
        parent_crawl_id = response.meta['parent_crawl_id']
        current_payload = response.meta['payload']
        detail_crawl_id = str(uuid.uuid4())
        
        try:
            # 尝试更新cookies (部分站点详情页也会set-cookie)
            if response.headers.getlist('Set-Cookie'):
                self._update_cookies(response)
                
            res_json = json.loads(response.text)
            
            hospital_list = res_json.get("list", [])
            # 兼容可能的 null 或不同结构
            if hospital_list is None:
                hospital_list = []
            
            self.spider_log.info(f"🏥 药品 [{drug_info.get('prodName')}] 详情页 - 发现 {len(hospital_list)} 家医院记录")
            
            # 上报详情页采集状态
            yield {
                '_status_': True,
                'crawl_id': detail_crawl_id,
                'stage': 'detail_page',
                'page_no': page_num, # 详情页没有分页，沿用列表页码
                'items_found': len(hospital_list),
                'params': current_payload,
                'api_url': self.hospital_api_url,
                'reference_id': drug_info.get('prodCode'),
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }

            # 3. 创建合并后的数据 Item
            item = self._create_item(drug_info, hospital_list, page_num)
            yield item
            
            # 更新状态，确认入库 1 条 (聚合了所有医院信息)
            yield {
                '_status_': True,
                'crawl_id': detail_crawl_id,
                'stage': 'detail_page',
                'items_stored': 1,
                'params': current_payload,
                'api_url': self.hospital_api_url,
                'reference_id': drug_info.get('prodCode'),
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }

        except Exception as e:
            self.spider_log.error(f"❌ 详情页解析失败: {e} | URL: {response.url}", exc_info=True)
            
            yield {
                '_status_': True,
                'crawl_id': detail_crawl_id,
                'stage': 'detail_page',
                'params': current_payload,
                'api_url': self.hospital_api_url,
                'reference_id': drug_info.get('prodCode'),
                'success': False,
                'error_message': str(e),
                'parent_crawl_id': parent_crawl_id
            }

    def _create_item(self, drug_info, hospital_list, page_num=1):
        """
        构建 HebeiDrugItem
        """
        item = HebeiDrugItem()
        
        prodentp_code = drug_info.get("prodentpCode")
        prod_code = drug_info.get("prodCode")
        
        # 1. 设置药品基础信息
        for field_name in item.fields:
            if field_name in ['md5_id', 'collect_time', 'url', 'url_hash', 'hospital_purchases', 'page_num']:
                continue
            if field_name in drug_info:
                item[field_name] = drug_info[field_name]
        
        # 2. 设置医院采购信息
        item['hospital_purchases'] = hospital_list
        
        # 3. 设置URL字段
        item['url'] = f"{self.hospital_api_url}?pageNo=1&pageSize=1000&prodCode={prod_code}&prodEntpCode={prodentp_code}"
        
        # 4. 设置页码
        item['page_num'] = page_num
        
        # 5. 生成MD5唯一ID
        item.generate_md5_id()
        
        return item
        
    def _update_cookies(self, response):
        """
        从响应中提取并更新cookies
        """
        try:
            for cookie_header in response.headers.getlist('Set-Cookie'):
                cookie_str = cookie_header.decode('utf-8')
                if '=' in cookie_str:
                    cookie_parts = cookie_str.split(';')[0].split('=')
                    if len(cookie_parts) >= 2:
                        cookie_name = cookie_parts[0].strip()
                        cookie_value = '='.join(cookie_parts[1:]).strip()
                        self.cookies[cookie_name] = cookie_value
            # self.spider_log.debug(f"更新后的cookies: {self.cookies}")
        except Exception as e:
            self.spider_log.warning(f"Cookies 更新失败: {e}")
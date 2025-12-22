from .base_spiders import BaseRequestSpider
from ..models.ningxia_drug import NingxiaDrugItem
from ..utils.logger_utils import get_spider_logger
import json
import scrapy
import uuid

class NingxiaDrugSpider(BaseRequestSpider):
    """
    宁夏医保局药品及采购医院爬虫
    流程: 
    1. 请求药品列表 (getRecentPurchaseDetailData.html) -> 支持翻页
    2. 获取 procurecatalogId
    3. 请求医院明细 (getDrugDetailDate.html) -> 支持翻页
    """
    name = "ningxia_drug_store"
    
    # 药品列表接口
    list_api_url = "https://nxyp.ylbz.nx.gov.cn/cms/recentPurchaseDetail/getRecentPurchaseDetailData.html" 
    # 医院明细接口
    hospital_api_url = "https://nxyp.ylbz.nx.gov.cn/cms/recentPurchaseDetail/getDrugDetailDate.html"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spider_log = get_spider_logger(self.name)
        self.crawl_id = str(uuid.uuid4())
        self.spider_log.info(f"🚀 爬虫初始化完成，crawl_id: {self.crawl_id}")

    custom_settings = {
        'CONCURRENT_REQUESTS': 4,
        'DOWNLOAD_DELAY': 1,
        'DEFAULT_REQUEST_HEADERS': {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15',
            'Origin': 'https://nxyp.ylbz.nx.gov.cn',
            'Referer': 'https://nxyp.ylbz.nx.gov.cn/cms/showListYPXQ.html',
            'X-Requested-With': 'XMLHttpRequest'
        },
        'ITEM_PIPELINES': {
            'hybrid_crawler.pipelines.DataCleaningPipeline': 300,        # 清洗
            'hybrid_crawler.pipelines.CrawlStatusPipeline': 350,         # 状态监控 (新增)
            'hybrid_crawler.pipelines.NingxiaDrugPipeline': 400,         # 入库
        }
    }

    def start_requests(self):
        """Step 1: 构造初始的药品列表请求"""
        payload = {
            "_search": "false",
            "page": "1",
            "rows": "100", 
            "sidx": "",
            "sord": "asc"
        }
        
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
        
        yield scrapy.FormRequest(
            url=self.list_api_url,
            method='POST',
            formdata=payload,
            callback=self.parse_logic,
            meta={'payload': payload, 'crawl_id': self.crawl_id},
            dont_filter=True
        )

    def parse_logic(self, response):
        """
        Step 2: 处理药品列表，触发医院详情请求
        """
        page_crawl_id = str(uuid.uuid4())
        parent_crawl_id = response.meta['crawl_id']
        current_payload = response.meta['payload']
        
        try:
            res_json = json.loads(response.text)
            
            # 提取数据
            records = res_json.get("rows", [])
            total_pages = int(res_json.get("total", 0))
            current_page = int(res_json.get("page", 1))
            total_records = int(res_json.get("records", 0))

            self.spider_log.info(f"📄 药品列表页面 [{current_page}/{total_pages}] - 发现 {len(records)} 条药品记录 (总计: {total_records})")

            # 上报页面采集状态
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': current_page,
                'total_pages': total_pages,
                'items_found': len(records),
                'params': current_payload,
                'api_url': self.list_api_url,
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }

            item_count = 0
            # --- 核心逻辑：遍历药品，进入第二层详情 ---
            for drug_item in records:
                # 必须有 procurecatalogId 才能查详情
                if drug_item.get("procurecatalogId"):
                    # 传递 page_crawl_id 作为详情页的父ID
                    yield from self._request_hospital_detail(drug_item, page_crawl_id)
                    item_count += 1
                else:
                    self.spider_log.warning(f"⚠️ 药品缺少 procurecatalogId: {drug_item.get('productName')}")

            # 更新页面采集状态
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': current_page,
                'total_pages': total_pages,
                'items_found': len(records),
                'items_stored': item_count, # 触发了多少个详情请求
                'params': current_payload,
                'api_url': self.list_api_url,
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }

            # --- 列表页翻页逻辑 ---
            if current_page < total_pages:
                self.spider_log.info(f"🔄 准备采集下一页药品列表 [{current_page + 1}/{total_pages}]")
                next_page = current_page + 1
                next_payload = current_payload.copy()
                next_payload['page'] = str(next_page)
                
                yield scrapy.FormRequest(
                    url=self.list_api_url,
                    method='POST',
                    formdata=next_payload,
                    callback=self.parse_logic,
                    meta={'payload': next_payload, 'crawl_id': self.crawl_id},
                    dont_filter=True
                )

        except Exception as e:
            self.spider_log.error(f"❌ 药品列表解析失败: {e}", exc_info=True)
            
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': current_payload.get('page'),
                'params': current_payload,
                'api_url': self.list_api_url,
                'success': False,
                'error_message': str(e),
                'parent_crawl_id': parent_crawl_id
            }

    def _request_hospital_detail(self, drug_item, parent_crawl_id):
        """Step 3: 构造医院详情请求 (POST)"""
        procure_id = str(drug_item.get("procurecatalogId"))
        
        detail_payload = {
            "procurecatalogId": procure_id,
            "_search": "false",
            "rows": "100", 
            "page": "1",    
            "sidx": "",
            "sord": "asc"
        }

        yield scrapy.FormRequest(
            url=self.hospital_api_url,
            method='POST',
            formdata=detail_payload,
            callback=self.parse_hospital_detail,
            meta={
                'drug_info': drug_item,
                'procure_id': procure_id,
                'current_detail_page': 1,
                'payload': detail_payload,
                'parent_crawl_id': parent_crawl_id
            },
            dont_filter=True
        )

    def parse_hospital_detail(self, response):
        """Step 4: 解析医院列表并生成 Item"""
        drug_info = response.meta['drug_info']
        parent_crawl_id = response.meta['parent_crawl_id']
        current_payload = response.meta['payload']
        detail_crawl_id = str(uuid.uuid4())
        
        try:
            res_json = json.loads(response.text)
            
            # 提取医院数据
            hospitals = res_json.get("rows", [])
            total_detail_pages = int(res_json.get("total", 0))
            current_detail_page = int(response.meta['current_detail_page'])
            
            self.spider_log.info(f"🏥 药品 [{drug_info.get('productName')}] 详情页 [{current_detail_page}/{total_detail_pages}] - 发现 {len(hospitals)} 家医院")

            # 上报详情页采集状态
            yield {
                '_status_': True,
                'crawl_id': detail_crawl_id,
                'stage': 'detail_page',
                'page_no': current_detail_page,
                'total_pages': total_detail_pages,
                'items_found': len(hospitals),
                'params': current_payload,
                'api_url': self.hospital_api_url,
                'reference_id': response.meta['procure_id'],
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }

            item_count = 0
            # 遍历当前页的医院，生成最终数据
            for hosp_item in hospitals:
                yield self._create_item(drug_info, hosp_item, response)
                item_count += 1

            # 更新详情页采集状态
            yield {
                '_status_': True,
                'crawl_id': detail_crawl_id,
                'stage': 'detail_page',
                'page_no': current_detail_page,
                'total_pages': total_detail_pages,
                'items_found': len(hospitals),
                'items_stored': item_count,
                'params': current_payload,
                'api_url': self.hospital_api_url,
                'reference_id': response.meta['procure_id'],
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }

            # --- 详情页翻页逻辑 ---
            if current_detail_page < total_detail_pages:
                next_page = current_detail_page + 1
                next_payload = current_payload.copy()
                next_payload['page'] = str(next_page)
                
                yield scrapy.FormRequest(
                    url=self.hospital_api_url,
                    method='POST',
                    formdata=next_payload,
                    callback=self.parse_hospital_detail,
                    meta={
                        'drug_info': drug_info,
                        'procure_id': response.meta['procure_id'],
                        'current_detail_page': next_page,
                        'payload': next_payload,
                        'parent_crawl_id': parent_crawl_id # 保持同一个父ID (列表页ID)
                    },
                    dont_filter=True
                )

        except Exception as e:
            self.spider_log.error(f"❌ 医院详情解析失败: {e} | DrugID: {response.meta.get('procure_id')}", exc_info=True)
            
            yield {
                '_status_': True,
                'crawl_id': detail_crawl_id,
                'stage': 'detail_page',
                'page_no': current_payload.get('page'),
                'params': current_payload,
                'api_url': self.hospital_api_url,
                'reference_id': response.meta.get('procure_id'),
                'success': False,
                'error_message': str(e),
                'parent_crawl_id': parent_crawl_id
            }

    def _create_item(self, drug_info, hosp_item, response=None):
        """合并药品信息和医院信息"""
        item = NingxiaDrugItem()
        
        # 1. 填充药品基础信息
        for key, value in drug_info.items():
            if key in item.fields:
                item[key] = value
                
        # 2. 填充/覆盖医院特有信息
        if 'hospitalName' in hosp_item:
            item['hospitalName'] = hosp_item['hospitalName']
            
        if 'areaName' in hosp_item:
            item['areaName'] = hosp_item['areaName']
            
        # 3. 补充系统字段
        item['url'] = self.hospital_api_url
        item['page_num'] = response.meta.get('current_detail_page', 1) if response else 1
        
        # 生成唯一ID
        item.generate_md5_id()
        
        return item
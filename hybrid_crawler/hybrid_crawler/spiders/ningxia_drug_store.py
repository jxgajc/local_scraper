from .base_spiders import BaseRequestSpider
from ..models.ningxia_drug import NingxiaDrugItem
import json
import scrapy

class NingxiaDrugSpider(BaseRequestSpider):
    """
    宁夏医保局药品及采购医院爬虫
    流程: 
    1. 请求药品列表 (getRecentPurchaseDetailData.html)
    2. 获取 procurecatalogId
    3. 请求医院明细 (getDrugDetailDate.html) [POST]
    """
    name = "ningxia_drug_store"
    
    # 药品列表接口
    list_api_url = "https://nxyp.ylbz.nx.gov.cn/cms/recentPurchaseDetail/getRecentPurchaseDetailData.html" 
    
    # 医院明细接口
    hospital_api_url = "https://nxyp.ylbz.nx.gov.cn/cms/recentPurchaseDetail/getDrugDetailDate.html"

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
            'hybrid_crawler.pipelines.DataCleaningPipeline': 300,
            'hybrid_crawler.pipelines.NingxiaDrugPipeline': 400,
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
        
        yield scrapy.FormRequest(
            url=self.list_api_url,
            method='POST',
            formdata=payload,
            callback=self.parse_logic, # 必须使用 parse_logic 作为回调
            meta={'payload': payload},
            dont_filter=True
        )

    def parse_logic(self, response):
        """
        Step 2: 处理药品列表，触发医院详情请求
        (方法名必须是 parse_logic 以满足 BaseRequestSpider 的要求)
        """
        try:
            res_json = json.loads(response.text)
            
            # 提取数据
            records = res_json.get("rows", [])
            total_pages = int(res_json.get("total", 0))
            current_page = int(res_json.get("page", 1))

            self.logger.info(f"[DrugList] 当前页: {current_page}/{total_pages}, 药品数: {len(records)}")

            # --- 核心逻辑：遍历药品，进入第二层详情 ---
            for drug_item in records:
                # 必须有 procurecatalogId 才能查详情
                if drug_item.get("procurecatalogId"):
                    yield from self._request_hospital_detail(drug_item)
                else:
                    self.logger.warning(f"药品缺少 procurecatalogId: {drug_item.get('productName')}")

            # --- 列表页翻页逻辑 ---
            if current_page < total_pages:
                next_page = current_page + 1
                next_payload = response.meta['payload'].copy()
                next_payload['page'] = str(next_page)
                
                yield scrapy.FormRequest(
                    url=self.list_api_url,
                    method='POST',
                    formdata=next_payload,
                    callback=self.parse_logic,
                    meta={'payload': next_payload},
                    dont_filter=True
                )

        except Exception as e:
            self.logger.error(f"药品列表解析失败: {e} | Response: {response.text[:200]}")

    def _request_hospital_detail(self, drug_item):
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
                'current_detail_page': 1 
            },
            dont_filter=True
        )

    def parse_hospital_detail(self, response):
        """Step 4: 解析医院列表并生成 Item"""
        try:
            res_json = json.loads(response.text)
            drug_info = response.meta['drug_info']
            
            # 提取医院数据
            hospitals = res_json.get("rows", [])
            total_detail_pages = int(res_json.get("total", 0))
            current_detail_page = int(response.meta['current_detail_page'])
            
            self.logger.info(f"[HospitalDetail] 药品ID: {response.meta['procure_id']} | 页码: {current_detail_page}/{total_detail_pages} | 医院数: {len(hospitals)}")

            # 遍历当前页的医院，生成最终数据
            for hosp_item in hospitals:
                yield self._create_item(drug_info, hosp_item, response)

            # --- 详情页翻页逻辑 ---
            if current_detail_page < total_detail_pages:
                next_page = current_detail_page + 1
                next_payload = {
                    "procurecatalogId": response.meta['procure_id'],
                    "_search": "false",
                    "rows": "100",
                    "page": str(next_page),
                    "sidx": "",
                    "sord": "asc"
                }
                
                yield scrapy.FormRequest(
                    url=self.hospital_api_url,
                    method='POST',
                    formdata=next_payload,
                    callback=self.parse_hospital_detail,
                    meta={
                        'drug_info': drug_info,
                        'procure_id': response.meta['procure_id'],
                        'current_detail_page': next_page
                    },
                    dont_filter=True
                )

        except Exception as e:
            self.logger.error(f"医院详情解析失败: {e} | DrugID: {response.meta.get('procure_id')}")

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
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
    宁夏医保局药品及采购医院爬虫
    目标: 先获取药品列表，再根据 procurecatalogId 获取采购该药品的医院信息（聚合模式）
    """
    name = "ningxia_drug_spider"
    
    # 接口地址
    list_api_url = "https://nxyp.ylbz.nx.gov.cn/cms/recentPurchaseDetail/getRecentPurchaseDetailData.html" 
    hospital_api_url = "https://nxyp.ylbz.nx.gov.cn/cms/recentPurchaseDetail/getDrugDetailDate.html"
    
    # 存储cookie
    cookies = {}

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
        """构造初始的GET请求"""
        payload = {
            "_search": "false",
            "page": 1,
            "rows": 1000, 
            "sidx": "",
            "sord": "asc"
        }
        
        self.logger.info(f"📋 开始采集药品列表，初始payload: {json.dumps(payload)}")
        
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
            
            total_pages = int(res_json.get("total", 0))
            records = res_json.get("rows", [])
            current = int(res_json.get("page", 1))
            total_records = int(res_json.get("records", 0))

            self.logger.info(f"📄 列表页面 [{current}/{total_pages}] - 发现 {len(records)} 条药品记录 (总计: {total_records})")

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
            # 1. 处理当前页的每一条药品数据 -> 发起详情请求
            for drug_item in records:
                # 传递 page_crawl_id 作为 parent
                yield from self._request_hospital_detail(drug_item, current, page_crawl_id)
                item_count += 1

            # 更新页面采集状态 (记录触发了多少个详情请求)
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
                self.logger.info(f"🔄 准备采集后续列表页面 (2-{total_pages})")
                
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
                            'parent_crawl_id': parent_crawl_id, # 列表页的父级是 Root
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
            if response.headers.getlist('Set-Cookie'):
                self._update_cookies(response)
                
            res_json = json.loads(response.text)
            records = res_json.get("rows", [])
            api_page = int(res_json.get('page', page_num))
            total_pages = int(res_json.get("total", 0))
            
            self.logger.info(f"📄 列表页面 [{api_page}/{total_pages}] - 发现 {len(records)} 条药品记录")
            
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
            for drug_item in records:
                yield from self._request_hospital_detail(drug_item, api_page, page_crawl_id)
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

    def _request_hospital_detail(self, drug_item, page_num, parent_crawl_id):
        """构造获取医院详情的请求，将 drug_item 传递下去"""
        procurecatalog_id = drug_item.get("procurecatalogId")
        
        if not procurecatalog_id:
            self.logger.warning(f"⚠️ 缺少 procurecatalogId，跳过详情查询: {drug_item.get('productName')}")
            return

        # 构造详情页 Payload
        payload = {
            "page": 1,
            "rows": 1000,
            "procurecatalogId": procurecatalog_id,
            "_search": "false",
            "sidx": "",
            "sord": "asc"
        }
        
        # 注意：这里原代码使用的是 GET 请求带 formdata，在 Scrapy 中 FormRequest 默认是 POST。
        # 如果目标接口确实支持 GET 且参数在 URL 中，建议用 scrapy.Request(url=...urlencode(params))。
        # 如果目标是 POST，请将 method 改为 'POST'。
        # 这里为了兼容原逻辑，保持 FormRequest 但需注意 method。
        # 假设原意是 POST (因为有 rows/page 参数)，这里显式改为 POST 会更稳妥，
        # 但如果必须 GET，formdata 会被忽略（除非库有特殊处理）。
        # 此处保留 FormRequest 结构以便兼容 payload 传递。
        
        form_data_str = {k: str(v) for k, v in payload.items()}
        yield scrapy.FormRequest(
            url=self.hospital_api_url,
            method='GET', # 保持原代码的 GET，但需注意可能需要 urlencode 到 URL
            formdata=form_data_str,
            callback=self.parse_detail,
            meta={
                'drug_info': drug_item, 
                'page_num': page_num,
                'procurecatalog_id': procurecatalog_id,
                'current_hospital_page': 1,
                'hospital_list': [],
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
        procurecatalog_id = response.meta.get('procurecatalog_id')
        current_hospital_page = response.meta.get('current_hospital_page', 1)
        hospital_list = response.meta.get('hospital_list', [])
        parent_crawl_id = response.meta['parent_crawl_id']
        current_payload = response.meta['payload']
        
        # 为当前药品的详情抓取生成一个 ID (如果还没生成过)
        detail_crawl_id = response.meta.get('detail_crawl_id', str(uuid.uuid4()))

        try:
            res_json = json.loads(response.text)
            
            # 3. 提取当前页医院数据和分页信息
            current_page_hospitals = res_json.get("rows", [])
            total_pages = int(res_json.get("total", 0))
            
            self.logger.info(f"🏥 药品 [{procurecatalog_id}] 医院列表 [{current_hospital_page}/{total_pages}] - 发现 {len(current_page_hospitals)} 家医院")
            
            # 上报详情页采集状态
            yield {
                '_status_': True,
                'crawl_id': detail_crawl_id,
                'stage': 'detail_page',
                'page_no': current_hospital_page,
                'total_pages': total_pages,
                'items_found': len(current_page_hospitals),
                'params': current_payload,
                'api_url': self.hospital_api_url,
                'reference_id': procurecatalog_id,
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }

            # 4. 累加医院数据
            hospital_list.extend(current_page_hospitals)
            
            # 5. 如果还有下一页，继续请求
            if current_hospital_page < total_pages:
                next_page = current_hospital_page + 1
                
                # 构造下一页请求
                next_payload = current_payload.copy()
                next_payload['page'] = next_page
                
                form_data_str = {k: str(v) for k, v in next_payload.items()}
                yield scrapy.FormRequest(
                    url=self.hospital_api_url,
                    method='GET',
                    formdata=form_data_str,
                    callback=self.parse_detail,
                    meta={
                        'drug_info': drug_info,
                        'page_num': page_num,
                        'procurecatalog_id': procurecatalog_id,
                        'current_hospital_page': next_page,
                        'hospital_list': hospital_list,
                        'parent_crawl_id': parent_crawl_id,
                        'detail_crawl_id': detail_crawl_id, # 传递同一个详情ID
                        'payload': next_payload
                    },
                    cookies=self.cookies if self.cookies else None,
                    dont_filter=True
                )
            else:
                # 6. 所有页面处理完成，生成最终Item
                self.logger.info(f"✅ 药品 [{procurecatalog_id}] 所有医院数据获取完成，共 {len(hospital_list)} 家医院")
                yield self._create_item(drug_info, hospital_list, page_num)
                
                # 更新状态：确认存储了1条聚合数据
                yield {
                    '_status_': True,
                    'crawl_id': detail_crawl_id,
                    'stage': 'detail_page',
                    'items_stored': 1, # 聚合模式下，最终只入库1条记录
                    'params': current_payload,
                    'api_url': self.hospital_api_url,
                    'reference_id': procurecatalog_id,
                    'success': True,
                    'parent_crawl_id': parent_crawl_id
                }

        except Exception as e:
            self.logger.error(f"❌ 详情页解析失败: {e} | URL: {response.url}", exc_info=True)
            yield {
                '_status_': True,
                'crawl_id': detail_crawl_id,
                'stage': 'detail_page',
                'page_no': current_hospital_page,
                'params': current_payload,
                'api_url': self.hospital_api_url,
                'reference_id': procurecatalog_id,
                'success': False,
                'error_message': str(e),
                'parent_crawl_id': parent_crawl_id
            }

    def _create_item(self, drug_info, hospital_list, page_num=1):
        """
        构建 NingxiaDrugItem
        """
        item = NingxiaDrugItem()
        
        prodentp_code = drug_info.get("prodentpCode")
        prod_code = drug_info.get("prodCode")
        
        # 1. 设置药品基础信息
        for field_name in item.fields:
            if field_name in ['md5_id', 'collect_time', 'url', 'url_hash', 'hospital_purchases', 'page_num']:
                continue  # 跳过需要单独处理的字段
            if field_name in drug_info:
                item[field_name] = drug_info[field_name]
        
        # 2. 设置医院采购信息
        item['hospital_purchases'] = hospital_list
        
        # 3. 设置URL字段
        item['url'] = f"{self.hospital_api_url}?pageNo=1&pageSize=1000&prodCode={prod_code}&prodEntpCode={prodentp_code}&isPublicHospitals="
        
        # 4. 设置页码
        item['page_num'] = page_num
        
        # 5. 生成MD5唯一ID和采集时间
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
            # self.logger.debug(f"更新后的cookies: {self.cookies}")
        except Exception as e:
            self.logger.warning(f"Cookies 更新失败: {e}")
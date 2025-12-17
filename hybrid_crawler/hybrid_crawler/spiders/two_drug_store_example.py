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
    宁夏医保局药品及采购医院爬虫
    目标: 先获取药品列表，再根据 extCode 获取采购该药品的医院信息
    """
    name = "ningxia_drug_spider"
    
    # ⚠️ 请确认第一个 curl 对应的真实 URL，并替换下方地址
    list_api_url = "https://nxyp.ylbz.nx.gov.cn/cms/recentPurchaseDetail/getRecentPurchaseDetailData.html" 
    
    # 医院查询接口
    hospital_api_url = "https://nxyp.ylbz.nx.gov.cn/cms/recentPurchaseDetail/getDrugDetailDate.html"
    
    # 存储cookie
    cookies = {}

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
            # 更新cookies
            # if response.headers.getlist('Set-Cookie'):
            #     self._update_cookies(response)
            res_json = json.loads(response.text)
            
            # data_block = res_json.get("data", {})
            total_pages = int(res_json.get("total", 0))
            records = res_json.get("rows", [])
            current = res_json.get("page", 1)

            self.logger.info(f"总页数: {total_pages}, 当前页: {current}, 当前页记录数: {len(records)}")

            # 1. 处理当前页的每一条药品数据 -> 发起详情请求
            for drug_item in records:
                yield from self._request_hospital_detail(drug_item, current)

            # 2. 生成剩余页码请求 (从第2页开始)
            current_payload = response.meta['payload']
            for page in range(2, total_pages + 1):
                next_payload = current_payload.copy()
                next_payload['page'] = page
                
                # query_string = urlencode(next_payload)
                # full_url = f"{self.list_api_url}?{query_string}"
                # 发起列表页请求（不需要cookie）
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
            # 更新cookies
            if response.headers.getlist('Set-Cookie'):
                self._update_cookies(response)
            res_json = json.loads(response.text)
            records = res_json.get("rows", [])
            page_num = res_json.get('page', 1)
            
            for drug_item in records:
                yield from self._request_hospital_detail(drug_item, page_num)
                
        except Exception as e:
            self.logger.error(f"分页解析失败 Page {res_json.get('page', 'unknown')}: {e}")

    def _request_hospital_detail(self, drug_item, page_num):
        """构造获取医院详情的请求，将 drug_item 传递下去"""
        procurecatalog_id = drug_item.get("procurecatalogId")

        
        if not procurecatalog_id:
            self.logger.warning(f"缺少 procurecatalogId，跳过详情查询: {drug_item};")
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
        # query_string = urlencode(payload)
        # full_url = f"{self.hospital_api_url}?{query_string}"
        # 这里的 meta 非常重要，用来传递第一步获取的 info 和页码
        # 获取医院信息需要使用之前收集的cookie
        form_data_str = {k: str(v) for k, v in payload.items()}
        yield scrapy.FormRequest(
            url=self.hospital_api_url,
            method='GET',
            formdata=form_data_str,
            # body=json.dumps(payload),
            callback=self.parse_detail,
            meta={
                'drug_info': drug_item, 
                'page_num': page_num,
                'procurecatalog_id': procurecatalog_id,
                'current_hospital_page': 1,
                'hospital_list': []
            }, 
            cookies=self.cookies if self.cookies else None,
            dont_filter=True
        )

    def parse_detail(self, response):
        """处理医院详情响应，合并数据并生成 Item"""
        try:
            # 1. 获取传递下来的药品基础信息和分页数据
            drug_info = response.meta['drug_info']
            page_num = response.meta.get('page_num', 1)
            procurecatalog_id = response.meta.get('procurecatalog_id')
            current_hospital_page = response.meta.get('current_hospital_page', 1)
            hospital_list = response.meta.get('hospital_list', [])
            
            # 2. 解析响应
            res_json = json.loads(response.text)
            
            # 3. 提取当前页医院数据和分页信息
            current_page_hospitals = res_json.get("rows", [])
            total_pages = int(res_json.get("total", 0))
            
            self.logger.info(f"药品 {procurecatalog_id} 医院列表 - 总页数: {total_pages}, 当前页: {current_hospital_page}, 当前页记录数: {len(current_page_hospitals)}")
            
            # 4. 累加医院数据
            hospital_list.extend(current_page_hospitals)
            
            # 5. 如果还有下一页，继续请求
            if current_hospital_page < total_pages:
                next_page = current_hospital_page + 1
                
                # 构造下一页请求
                payload = {
                    "page": next_page,
                    "rows": 10,
                    "procurecatalogId": procurecatalog_id,
                    "_search": "false",
                    "sidx": "",
                    "sord": "asc"
                }
                # query_string = urlencode(payload)
                # full_url = f"{self.hospital_api_url}?{query_string}"
                form_data_str = {k: str(v) for k, v in payload.items()}
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
                        'hospital_list': hospital_list
                    },
                    cookies=self.cookies if self.cookies else None,
                    dont_filter=True
                )
            else:
                # 6. 所有页面处理完成，生成最终Item
                self.logger.info(f"药品 {procurecatalog_id} 所有医院数据获取完成，共 {len(hospital_list)} 家医院")
                yield self._create_item(drug_info, hospital_list, page_num)

        except Exception as e:
            self.logger.error(f"详情页解析失败: {e} | URL: {response.url} | Response: {response.text[:200]}")

    def _create_item(self, drug_info, hospital_list, page_num=1):
        """
        构建 NingxiaDrugItem
        :param drug_info: 第一次请求获取的药品信息 (Dict)
        :param hospital_list: 第二次请求获取的医院列表 (List)
        :param page_num: 采集页码
        :param page_num: 采集页码
        """
        item = NingxiaDrugItem()
        
        # 使用 extCode 作为唯一标识的一部分
        # ext_code = drug_info.get("extCode", "")
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
        :param response: scrapy.Response对象
        """
        for cookie_header in response.headers.getlist('Set-Cookie'):
            cookie_str = cookie_header.decode('utf-8')
            if '=' in cookie_str:
                cookie_parts = cookie_str.split(';')[0].split('=')
                if len(cookie_parts) >= 2:
                    cookie_name = cookie_parts[0].strip()
                    cookie_value = '='.join(cookie_parts[1:]).strip()
                    self.cookies[cookie_name] = cookie_value
        self.logger.debug(f"更新后的cookies: {self.cookies}")
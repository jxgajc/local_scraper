from .base_spiders import BaseRequestSpider
from ..items import HybridCrawlerItem
import json
import scrapy

class FujianDrugSpider(BaseRequestSpider):
    """
    福建医保局药品及采购医院爬虫
    目标: 先获取药品列表，再根据 extCode 获取采购该药品的医院信息
    """
    name = "fujian_drug_spider"
    
    # ⚠️ 请确认第一个 curl 对应的真实 URL，并替换下方地址
    list_api_url = "https://open.ybj.fujian.gov.cn:10013/tps-local/web/tender/plus/item-cfg-info/list" 
    
    # 医院查询接口
    hospital_api_url = "https://open.ybj.fujian.gov.cn:10013/tps-local/web/trans/api/open/v2/queryHospital"

    custom_settings = {
        'CONCURRENT_REQUESTS': 8, # 根据服务器压力适当调整
        'DOWNLOAD_DELAY': 3,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': '*/*',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'User-Agent': 'PostmanRuntime-ApipostRuntime/1.1.0',
            'prodType': '2'
        }
    }

    def start_requests(self):
        """构造初始的POST请求"""
        payload = {
            "druglistName": "",
            "druglistCode": "",
            "drugName": "",
            "ruteName": "",
            "dosformName": "",
            "specName": "",
            "pac": "",
            "prodentpName": "",
            "current": 1,
            "size": 100,  # 修改此处 size 为 100
            "tenditmType": ""
        }
        
        # 发起第一页请求
        yield scrapy.Request(
            url=self.list_api_url,
            method='POST',
            body=json.dumps(payload),
            callback=self.parse_logic,
            meta={'payload': payload}, # 传递 payload 以便后续翻页使用
            dont_filter=True
        )

    def parse_logic(self, response):
        """处理药品列表初始响应：处理第一页数据 + 生成后续页码请求"""
        try:
            res_json = json.loads(response.text)
            
            if res_json.get("code") != 0:
                self.logger.error(f"API Error: {res_json.get('message')}")
                return

            data_block = res_json.get("data", {})
            total_pages = int(data_block.get("pages", 0))
            records = data_block.get("records", [])

            self.logger.info(f"总页数: {total_pages}, 当前页记录数: {len(records)}")

            # 1. 处理当前页的每一条药品数据 -> 发起详情请求
            for drug_item in records:
                yield from self._request_hospital_detail(drug_item)

            # 2. 生成剩余页码请求 (从第2页开始)
            current_payload = response.meta['payload']
            for page in range(2, total_pages + 1):
                next_payload = current_payload.copy()
                next_payload['current'] = page
                
                yield scrapy.Request(
                    url=self.list_api_url,
                    method='POST',
                    body=json.dumps(next_payload),
                    callback=self.parse_list_page,
                    meta={'page_num': page},
                    dont_filter=True
                )

        except Exception as e:
            self.logger.error(f"列表页解析失败: {e} | Response: {response.text[:200]}")

    def parse_list_page(self, response):
        """处理后续药品列表页"""
        try:
            res_json = json.loads(response.text)
            records = res_json.get("data", {}).get("records", [])
            
            for drug_item in records:
                yield from self._request_hospital_detail(drug_item)
                
        except Exception as e:
            self.logger.error(f"分页解析失败 Page {response.meta.get('page_num')}: {e}")

    def _request_hospital_detail(self, drug_item):
        """构造获取医院详情的请求，将 drug_item 传递下去"""
        ext_code = drug_item.get("extCode")
        
        if not ext_code:
            self.logger.warning(f"缺少 extCode，跳过详情查询: {drug_item.get('drugName')}")
            return

        # 构造详情页 Payload
        payload = {
            "area": "",
            "hospitalName": "",
            "pageNo": 1,
            "pageSize": 10, # 详情页可以保持默认，或者也由用户指定
            "productId": ext_code, # 关键映射：extCode -> productId
            "tenditmType": ""
        }

        # 这里的 meta 非常重要，用来传递第一步获取的 info
        yield scrapy.Request(
            url=self.hospital_api_url,
            method='POST',
            body=json.dumps(payload),
            callback=self.parse_detail,
            meta={'drug_info': drug_item}, 
            dont_filter=True
        )

    def parse_detail(self, response):
        """处理医院详情响应，合并数据并生成 Item"""
        try:
            # 1. 获取第一步传递下来的药品基础信息
            drug_info = response.meta['drug_info']
            
            # 2. 解析第二步的响应
            res_json = json.loads(response.text)
            
            hospital_list = []
            
            # 注意：根据提供的第二个 curl 结果，data 字段是一个 "JSON 字符串"，需要二次解析
            # "data": "{\"msg\":\"调用...\",\"total\":12,\"data\":[...]}"
            raw_data_str = res_json.get("data")
            
            if isinstance(raw_data_str, str):
                try:
                    inner_data = json.loads(raw_data_str)
                    hospital_list = inner_data.get("data", [])
                except json.JSONDecodeError:
                    self.logger.warning(f"内部JSON解析失败: {raw_data_str[:100]}")
            elif isinstance(raw_data_str, dict):
                # 兼容情况：如果 API 偶尔返回直接的 dict
                hospital_list = raw_data_str.get("data", [])

            # 3. 创建合并后的数据 Item
            yield self._create_item(drug_info, hospital_list)

        except Exception as e:
            self.logger.error(f"详情页解析失败: {e} | URL: {response.url}")

    def _create_item(self, drug_info, hospital_list):
        """
        构建 HybridCrawlerItem
        :param drug_info: 第一次请求获取的药品信息 (Dict)
        :param hospital_list: 第二次请求获取的医院列表 (List)
        """
        item = HybridCrawlerItem()
        
        # 使用 extCode 作为唯一标识的一部分
        ext_code = drug_info.get("extCode", "")
        drug_name = drug_info.get("drugName", "")
        
        # 构造一个伪造的唯一 URL 或 API 调用特征，用于去重
        item["url"] = f"{self.hospital_api_url}?productId={ext_code}&unique_flag=combined"
        
        item["title"] = drug_name
        
        # 构造最终的结构化数据，直接合并字典
        final_data = drug_info.copy()
        final_data["hospital_purchases"] = hospital_list
        
        item["content"] = json.dumps(final_data, ensure_ascii=False)
        item["status_code"] = 200
        item["request_type"] = "http"
        item["source"] = "fujian_drug_spider"
        
        # 将结构化字典放入 meta_info，方便入库处理
        item["meta_info"] = final_data
        
        return item
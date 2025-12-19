import scrapy
import json
from ..models.fujian_drug import FujianDrugItem
from scrapy.http import JsonRequest

class FujianDrugSpider(scrapy.Spider):
    """
    福建省医疗保障局 - 药品挂网及采购医院查询
    Target: https://open.ybj.fujian.gov.cn:10013/tps-local/#/external/product-publicity
    """
    name = "fujian_drug_spider"
    
    # API Endpoints
    list_api_url = "https://open.ybj.fujian.gov.cn:10013/tps-local/web/tender/plus/item-cfg-info/list"
    hospital_api_url = "https://open.ybj.fujian.gov.cn:10013/tps-local/web/trans/api/open/v2/queryHospital"

    custom_settings = {
        'CONCURRENT_REQUESTS': 5,
        'DOWNLOAD_DELAY': 3,
        'DEFAULT_REQUEST_HEADERS': {
            'Content-Type': 'application/json;charset=utf-8',
            'Accept': 'application/json, text/plain, */*',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15',
            'Origin': 'https://open.ybj.fujian.gov.cn:10013',
            'Referer': 'https://open.ybj.fujian.gov.cn:10013/tps-local/',
            # 注意: 这里的Cookie可能有时效性，实际部署时可能需要动态获取或定期更新
            # 'Cookie': 'A-pool-ui-5=16412.56937.19855.0000; _gscu_1203915485=64553900x15k8025',
            'prodType': '2',
            'Priority': 'u=3, i'
        },
        'ITEM_PIPELINES': {
            'hybrid_crawler.pipelines.DataCleaningPipeline': 300,        # 清洗
            'hybrid_crawler.pipelines.FujianDrugPipeline': 400,           # 入库
        }
    }

    def start_requests(self):
        """开始请求药品列表"""
        # 初始Payload，size设为100以提高效率
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
            "size": 1000,
            "tenditmType": ""
        }
        
        yield JsonRequest(
            url=self.list_api_url,
            method='POST',
            data=payload,
            callback=self.parse_drug_list,
            meta={'payload': payload},
            dont_filter=True
        )

    def parse_drug_list(self, response):
        """解析药品列表"""
        try:
            res_json = json.loads(response.text)
            if res_json.get("code") != 0:
                self.logger.error(f"List API Error: {res_json.get('message')}")
                return

            data_block = res_json.get("data", {})
            records = data_block.get("records", [])
            current_page = data_block.get("current", 1)
            total_pages = data_block.get("pages", 0)

            self.logger.info(f"[Page {current_page}/{total_pages}] Found {len(records)} drugs")

            for record in records:
                # 1. 提取基础信息
                base_info = {
                    'ext_code': record.get('extCode'),
                    'drug_list_code': record.get('druglistCode'),
                    'drug_name': record.get('drugName'),
                    'drug_list_name': record.get('druglistName'),
                    'dosform': record.get('dosformName'),
                    'spec': record.get('specName'),
                    'pac': record.get('pac'),
                    'rute_name': record.get('ruteName'),
                    'prod_entp': record.get('prodentpName'),
                    'source_data': json.dumps(record, ensure_ascii=False)
                }

                # 2. 查询医院采购信息
                # 只有 extCode 存在时才能查询
                ext_code = record.get('extCode')
                if ext_code:
                    hospital_payload = {
                        "area": "",
                        "hospitalName": "",
                        "pageNo": 1,
                        "pageSize": 100,
                        "productId": ext_code,
                        "tenditmType": ""
                    }
                    
                    yield JsonRequest(
                        url=self.hospital_api_url,
                        method='POST',
                        data=hospital_payload,
                        callback=self.parse_hospital,
                        meta={
                            'base_info': base_info,
                            'payload': hospital_payload
                        },
                        dont_filter=True
                    )
                else:
                    # 无ID，仅保存基础信息
                    item = FujianDrugItem()
                    item.update(base_info)
                    item['has_hospital_record'] = False
                    item.generate_md5_id()
                    yield item

            # 3. 药品列表翻页
            if current_page < total_pages:
                next_payload = response.meta['payload'].copy()
                next_payload['current'] = current_page + 1
                
                yield JsonRequest(
                    url=self.list_api_url,
                    method='POST',
                    data=next_payload,
                    callback=self.parse_drug_list,
                    meta={'payload': next_payload},
                    dont_filter=True
                )

        except Exception as e:
            self.logger.error(f"Parse Drug List Failed: {e}", exc_info=True)

    def parse_hospital(self, response):
        """解析医院列表（嵌套JSON解析）"""
        base_info = response.meta['base_info']
        current_payload = response.meta['payload']

        try:
            res_json = json.loads(response.text)
            
            # 注意：data 字段是一个 JSON 字符串，需要二次解析
            # 示例: "data": "{\"msg\":\"...\",\"total\":85,\"data\":[...]}"
            inner_data_str = res_json.get("data")
            
            if not inner_data_str or not isinstance(inner_data_str, str):
                # 可能是没有数据或者格式不对，视为无记录
                item = FujianDrugItem()
                item.update(base_info)
                item['has_hospital_record'] = False
                item.generate_md5_id()
                yield item
                return

            # 二次解析
            inner_json = json.loads(inner_data_str)
            hospitals = inner_json.get("data", [])
            total_records = int(inner_json.get("total", 0))
            current_page = int(inner_json.get("pageNo", 1))
            page_size = int(inner_json.get("pageSize", 100))

            if hospitals:
                self.logger.debug(f"[{base_info['drug_name']}] Found {len(hospitals)} hospitals (Page {current_page})")
                for hosp in hospitals:
                    item = FujianDrugItem()
                    item.update(base_info)
                    
                    item['has_hospital_record'] = True
                    item['hospital_name'] = hosp.get('hospitalName')
                    item['medins_code'] = hosp.get('medinsCode')
                    item['area_name'] = hosp.get('areaName')
                    item['area_code'] = hosp.get('areaCode')
                    
                    # 更新 source_data 包含两部分
                    full_source = {
                        "drug_info": json.loads(base_info['source_data']),
                        "hospital_info": hosp
                    }
                    item['source_data'] = json.dumps(full_source, ensure_ascii=False)
                    
                    item.generate_md5_id()
                    yield item
                
                # 4. 医院列表翻页
                # 计算总页数
                total_pages = (total_records + page_size - 1) // page_size
                
                if current_page < total_pages:
                    next_payload = current_payload.copy()
                    next_payload['pageNo'] = current_page + 1
                    
                    yield JsonRequest(
                        url=self.hospital_api_url,
                        method='POST',
                        data=next_payload,
                        callback=self.parse_hospital,
                        meta={
                            'base_info': base_info,
                            'payload': next_payload
                        },
                        dont_filter=True
                    )
            else:
                # 解析成功但列表为空
                if current_page == 1:
                    item = FujianDrugItem()
                    item.update(base_info)
                    item['has_hospital_record'] = False
                    item.generate_md5_id()
                    yield item

        except Exception as e:
            self.logger.error(f"Parse Hospital Failed for {base_info['ext_code']}: {e}", exc_info=True)
import scrapy
import json
import time
import base64
import ddddocr
import uuid
from ..models.shandong_drug import ShandongDrugItem
from scrapy.http import JsonRequest 
import pandas as pd
from ..utils.logger_utils import get_spider_logger
import os

# 获取脚本所在目录的绝对路径
script_dir = os.path.dirname(os.path.abspath(__file__))
# 构建Excel文件的绝对路径
excel_path = os.path.join(script_dir, "../../关键字采集(2).xlsx")

class ShandongDrugSpider(scrapy.Spider):
    name = "drug_hosipital_shandong"
    
    # 接口 URL
    index_url = "https://ypjc.ybj.shandong.gov.cn/trade/drug/query-of-hanging-directory/index"
    captcha_url = "https://ypjc.ybj.shandong.gov.cn/code/hsaTrade/tps-local/web/gwml/getPicVerCode"
    list_api_url = "https://ypjc.ybj.shandong.gov.cn/code/hsaTrade/tps-local/web/gwml/listDrug"
    hospital_api_url = "https://ypjc.ybj.shandong.gov.cn/code/hsaTrade/tps-local/web/gwml/listHospital"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spider_log = get_spider_logger(self.name)
        self.crawl_id = str(uuid.uuid4())
        
        # 初始化 OCR
        self.ocr = ddddocr.DdddOcr(show_ad=False)
        
        # 加载关键词
        try:
            df_name = pd.read_excel(excel_path)
            self.product_names = df_name.loc[:, "采集关键字"].to_list()
            self.spider_log.info(f"🚀 爬虫初始化完成，crawl_id: {self.crawl_id}，加载关键词: {len(self.product_names)} 个")
        except Exception as e:
            self.spider_log.error(f"❌ 关键词文件加载失败: {e}")
            self.product_names = []

    custom_settings = {
        'CONCURRENT_REQUESTS': 1, # 保持低并发，避免验证码封禁
        'DOWNLOAD_DELAY': 2,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh-Hans;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json;charset=utf-8',
            'Origin': 'https://ypjc.ybj.shandong.gov.cn',
            'Referer': 'https://ypjc.ybj.shandong.gov.cn/trade/drug/query-of-hanging-directory/index',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15',
            'queryToken': '05ea8b36dcbc4cbf925d1eb65324dd96', 
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Priority': 'u=3, i'
        },
        'ITEM_PIPELINES': {
            'hybrid_crawler.pipelines.DataCleaningPipeline': 300,        # 清洗
            'hybrid_crawler.pipelines.CrawlStatusPipeline': 350,         # 状态监控 (新增)
            'hybrid_crawler.pipelines.ShandongDrugPipeline': 400,        # 入库
        }
    }

    def start_requests(self):
        """第一步：访问索引页面获取必要的Cookie"""
        self.spider_log.info(f"📋 开始采集，队列中共 {len(self.product_names)} 个关键词")
        yield scrapy.Request(url=self.index_url, method='GET', callback=self.parse_index, dont_filter=True)

    def parse_index(self, response):
        """
        第二步：处理索引页面，并开始为每个关键字发起验证码请求
        """
        self.spider_log.info("✅ 索引页面访问成功，开始调度关键词任务")
        
        for prod_name in self.product_names:
            timestamp = int(time.time() * 1000)
            url = f"{self.captcha_url}?timestamp={timestamp}"
            
            # 上报开始采集状态 (针对每个关键词)
            # yield {
            #     '_status_': True,
            #     'crawl_id': self.crawl_id,
            #     'stage': 'start_requests',
            #     'page_no': 1,
            #     'params': {'keyword': prod_name},
            #     'api_url': self.list_api_url,
            #     'reference_id': prod_name,
            #     'success': True
            # }
            
            yield JsonRequest(
                url=url, 
                method='GET', 
                callback=self.parse_captcha, 
                meta={
                    'keyword': prod_name, 
                    'retry_count': 0,
                    'crawl_id': self.crawl_id # 传递根ID
                },
                dont_filter=True
            )

    def parse_captcha(self, response):
        """第三步：识别验证码并发起单次药品列表查询"""
        current_keyword = response.meta.get('keyword')
        retry_payload = response.meta.get('retry_payload') 
        parent_crawl_id = response.meta.get('crawl_id')
        
        try:
            res_json = json.loads(response.text)
            if not res_json.get("success"):
                self.spider_log.error(f"❌ [{current_keyword}] 验证码接口报错: {response.text}")
                return

            data = res_json.get("data", {})
            base64_str = data.get("base64Str", "")
            random_str = data.get("randomStr", "")
            resp_text = data.get("text", "") 

            if not base64_str:
                self.spider_log.error(f"❌ [{current_keyword}] 未找到验证码图片数据")
                return

            # ddddocr 识别
            img_bytes = base64.b64decode(base64_str.split(',')[-1])
            code_result = self.ocr.classification(img_bytes)
            
            self.spider_log.debug(f"🔢 [{current_keyword}] 验证码识别结果: {code_result}")

            # 构造请求 Payload
            if retry_payload:
                payload = retry_payload
                payload.update({
                    "randomStr": random_str,
                    "text": resp_text,
                    "code": code_result
                })
            else:
                payload = {
                    "current": 1,
                    "size": 100,
                    "randomStr": random_str,
                    "text": resp_text, 
                    "prodCode": "",
                    "prodName": current_keyword,
                    "prodentpName": "",
                    "purchaseType": "",
                    "queryType": "1",
                    "code": code_result
                }

            yield JsonRequest(
                url=self.list_api_url,
                method='POST',
                data=payload,
                callback=self.parse_list,
                meta={
                    'payload': payload,
                    'keyword': current_keyword,
                    'retry_count': response.meta.get('retry_count', 0),
                    'parent_crawl_id': parent_crawl_id
                },
                dont_filter=True
            )

        except Exception as e:
            self.spider_log.error(f"❌ [{current_keyword}] 解析验证码响应异常: {e}", exc_info=True)

    def parse_list(self, response):
        """第四步：解析药品列表"""
        page_crawl_id = str(uuid.uuid4())
        current_keyword = response.meta.get('keyword', 'Unknown')
        parent_crawl_id = response.meta.get('parent_crawl_id')
        current_payload = response.meta.get('payload')
        
        try:
            res_json = json.loads(response.text)
            
            if not res_json.get("success"):
                error_code = res_json.get("code")
                # 检查是否为验证码错误（code=160003）
                if error_code == 160003:
                    retry_count = response.meta.get('retry_count', 0)
                    max_retries = 5
                    
                    if retry_count < max_retries:
                        self.spider_log.warning(f"⚠️ [{current_keyword}] 验证码错误，准备重试 ({retry_count + 1}/{max_retries})")
                        
                        timestamp = int(time.time() * 1000)
                        captcha_url = f"{self.captcha_url}?timestamp={timestamp}"
                        
                        yield JsonRequest(
                            url=captcha_url,
                            method='GET',
                            callback=self.parse_captcha,
                            meta={
                                'keyword': current_keyword,
                                'retry_count': retry_count + 1,
                                'retry_payload': current_payload,
                                'crawl_id': parent_crawl_id # 保持 ID 传递
                            },
                            dont_filter=True
                        )
                        return
                    else:
                        self.spider_log.error(f"❌ [{current_keyword}] 验证码错误次数过多，放弃该词")
                        return
                else:
                    self.spider_log.warning(f"❌ [{current_keyword}] 列表请求异常: {res_json.get('msg', 'Unknown')}")
                    
                    yield {
                        '_status_': True,
                        'crawl_id': page_crawl_id,
                        'stage': 'list_page',
                        'page_no': current_payload.get('current'),
                        'params': current_payload,
                        'api_url': self.list_api_url,
                        'reference_id': current_keyword,
                        'success': False,
                        'error_message': res_json.get('msg', 'Unknown Error'),
                        'parent_crawl_id': parent_crawl_id
                    }
                    return

            # --- 正常数据处理逻辑 ---
            data_block = res_json.get("data", {})
            records = data_block.get("records", [])
            current_page = data_block.get("current", 1)
            total_pages = data_block.get("pages", 0)
            total_records = data_block.get("total", 0)

            self.spider_log.info(f"📄 关键词 [{current_keyword}] 列表页面 [{current_page}/{total_pages}] - 发现 {len(records)} 条记录 (总计: {total_records})")

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
                'reference_id': current_keyword,
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }

            item_count = 0
            for record in records:
                base_info = {
                    'prodCode': record.get('prodCode'),
                    'prodName': record.get('prodName'),
                    'prodentpName': record.get('prodentpName'),
                    'spec': record.get('prodSpec'),
                    'pac': record.get('prodPac'),
                    'price': record.get('pubonlnPricStr'),
                    'aprvno': record.get('aprvno'),
                    'manufacture_name': record.get('marketPermitHolder') or record.get('scqyName'),
                    'public_time': record.get('optTime'),
                    'source_data': json.dumps(record, ensure_ascii=False)
                }
                
                pubonln_id = record.get('pubonlnId')

                if pubonln_id:
                    hospital_payload = {
                        "current": 1,
                        "size": 50, 
                        "randomStr": "",
                        "text": "",
                        "medinsName": "",
                        "basicFlag": "",
                        "queryType": "0",
                        "code": "",
                        "procureCatalogId": pubonln_id
                    }
                    
                    yield JsonRequest(
                        url=self.hospital_api_url,
                        method='POST',
                        data=hospital_payload,
                        callback=self.parse_hospital,
                        meta={
                            'base_info': base_info, 
                            'payload': hospital_payload,
                            'keyword': current_keyword,
                            'parent_crawl_id': page_crawl_id,
                            'prod_code': base_info['prodCode']
                        },
                        dont_filter=True
                    )
                    item_count += 1
                else:
                    item = ShandongDrugItem()
                    for k, v in base_info.items():
                        item[k] = v
                    item['has_hospital_record'] = False
                    item.generate_md5_id()
                    yield item
                    item_count += 1

            # 更新页面采集状态
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': current_page,
                'total_pages': total_pages,
                'items_found': len(records),
                'items_stored': item_count,
                'params': current_payload,
                'api_url': self.list_api_url,
                'reference_id': current_keyword,
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }

            # 翻页逻辑
            if current_page < total_pages:
                self.spider_log.info(f"🔄 准备采集关键词 [{current_keyword}] 下一页 [{current_page + 1}/{total_pages}]")
                next_page = current_page + 1
                new_payload = current_payload.copy()
                new_payload['current'] = next_page
                
                # 假设翻页Session保持，无需重新验证码
                yield JsonRequest(
                    url=self.list_api_url,
                    method='POST',
                    data=new_payload,
                    callback=self.parse_list,
                    meta={
                        'payload': new_payload,
                        'keyword': current_keyword,
                        'parent_crawl_id': parent_crawl_id 
                    },
                    dont_filter=True
                )

        except Exception as e:
            self.spider_log.error(f"❌ [{current_keyword}] 解析药品列表页异常: {e}", exc_info=True)
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': current_payload.get('current'),
                'params': current_payload,
                'api_url': self.list_api_url,
                'reference_id': current_keyword,
                'success': False,
                'error_message': str(e),
                'parent_crawl_id': parent_crawl_id
            }

    def parse_hospital(self, response):
        """解析医院详情"""
        base_info = response.meta['base_info']
        current_payload = response.meta['payload']
        parent_crawl_id = response.meta['parent_crawl_id']
        prod_code = response.meta.get('prod_code')
        keyword = response.meta.get('keyword')
        detail_crawl_id = str(uuid.uuid4())
        
        try:
            res_json = json.loads(response.text)
            
            if not res_json.get("success"):
                msg = res_json.get('msg', 'Unknown Error')
                self.spider_log.warning(f"⚠️ 医院接口请求失败: {msg}")
                
                yield {
                    '_status_': True,
                    'crawl_id': detail_crawl_id,
                    'stage': 'detail_page',
                    'page_no': current_payload['current'],
                    'params': current_payload,
                    'api_url': self.hospital_api_url,
                    'reference_id': prod_code,
                    'success': False,
                    'error_message': msg,
                    'parent_crawl_id': parent_crawl_id
                }
                return

            data = res_json.get("data", {})
            records = data.get("records", [])
            current_page = data.get("current", 1)
            total_pages = data.get("pages", 0)
            
            self.spider_log.info(f"🏥 药品 [{base_info['prodName']}] 详情页 [{current_page}/{total_pages}] - 发现 {len(records)} 家医院")
            
            # 上报详情页采集状态
            yield {
                '_status_': True,
                'crawl_id': detail_crawl_id,
                'stage': 'detail_page',
                'page_no': current_page,
                'total_pages': total_pages,
                'items_found': len(records),
                'params': current_payload,
                'api_url': self.hospital_api_url,
                'reference_id': prod_code,
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }

            item_count = 0
            if not records:
                # 无医院记录，仅保存基础信息
                item = ShandongDrugItem()
                for k, v in base_info.items():
                    item[k] = v
                item['has_hospital_record'] = False
                item.generate_md5_id()
                yield item
                item_count += 1
            else:
                for hosp in records:
                    item = ShandongDrugItem()
                    for k, v in base_info.items():
                        item[k] = v
                    item['has_hospital_record'] = True
                    item['hospitalName'] = hosp.get('hospitalName')
                    item['hospitalId'] = hosp.get('hospitalId')
                    item['cityName'] = hosp.get('cityName')
                    item['cotyName'] = hosp.get('cotyName')
                    item['admdvsName'] = hosp.get('admdvsName')
                    item['drugPurchasePropertyStr'] = hosp.get('drugPurchasePropertyStr')
                    item['userName'] = hosp.get('userName')
                    item['admdvs'] = hosp.get('admdvs')
                    item.generate_md5_id()
                    yield item
                    item_count += 1

            # 更新详情页采集状态
            yield {
                '_status_': True,
                'crawl_id': detail_crawl_id,
                'stage': 'detail_page',
                'page_no': current_page,
                'total_pages': total_pages,
                'items_found': len(records),
                'items_stored': item_count,
                'params': current_payload,
                'api_url': self.hospital_api_url,
                'reference_id': prod_code,
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }

            if current_page < total_pages:
                next_page = current_page + 1
                new_payload = current_payload.copy()
                new_payload['current'] = next_page
                yield JsonRequest(
                    url=self.hospital_api_url,
                    method='POST',
                    data=new_payload,
                    callback=self.parse_hospital,
                    meta={
                        'base_info': base_info,
                        'payload': new_payload,
                        'keyword': keyword,
                        'parent_crawl_id': parent_crawl_id,
                        'prod_code': prod_code
                    },
                    dont_filter=True
                )
        except Exception as e:
            self.spider_log.error(f"❌ 解析医院详情页异常: {e}", exc_info=True)
            yield {
                '_status_': True,
                'crawl_id': detail_crawl_id,
                'stage': 'detail_page',
                'page_no': current_payload.get('current'),
                'params': current_payload,
                'api_url': self.hospital_api_url,
                'reference_id': prod_code,
                'success': False,
                'error_message': str(e),
                'parent_crawl_id': parent_crawl_id
            }
import scrapy
import json
import time
import base64
import ddddocr
from ..models.shandong_drug import ShandongDrugItem
from scrapy.http import JsonRequest 
import pandas as pd

# 读取Excel文件
df_name = pd.read_excel(r"/Users/jcagito/Desktop/未命名文件夹/gen_spider/脚本文件夹/关键字采集(2).xlsx")
product_list = df_name.loc[:,"采集关键字"].to_list()

class ShandongDrugSpider(scrapy.Spider):
    name = "shandong_drug_spider"
    
    # 接口 URL
    index_url = "https://ypjc.ybj.shandong.gov.cn/trade/drug/query-of-hanging-directory/index"
    captcha_url = "https://ypjc.ybj.shandong.gov.cn/code/hsaTrade/tps-local/web/gwml/getPicVerCode"
    list_api_url = "https://ypjc.ybj.shandong.gov.cn/code/hsaTrade/tps-local/web/gwml/listDrug"
    hospital_api_url = "https://ypjc.ybj.shandong.gov.cn/code/hsaTrade/tps-local/web/gwml/listHospital"
    
    product_names = product_list

    custom_settings = {
        'CONCURRENT_REQUESTS': 3, # 稍微降低并发，避免验证码接口请求过快被封
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
            'hybrid_crawler.pipelines.DataCleaningPipeline': 300,
            'hybrid_crawler.pipelines.ShandongDrugPipeline': 400,
        }
    }

    def start_requests(self):
        """第一步：访问索引页面获取必要的Cookie"""
        yield scrapy.Request(url=self.index_url, method='GET', callback=self.parse_index, dont_filter=True)

    def parse_index(self, response):
        """
        第二步：处理索引页面，并开始为每个关键字发起验证码请求
        """
        self.logger.info(f"访问索引页面成功，开始调度 {len(self.product_names)} 个关键词任务")
        
        # 核心修改：在这里遍历关键字，而不是在 parse_captcha 里遍历
        for prod_name in self.product_names:
            timestamp = int(time.time() * 1000)
            url = f"{self.captcha_url}?timestamp={timestamp}"
            
            # 将 prod_name 放入 meta，传递给下一个函数
            yield JsonRequest(
                url=url, 
                method='GET', 
                callback=self.parse_captcha, 
                meta={'keyword': prod_name, 'retry_count': 0},
                dont_filter=True
            )

    def parse_captcha(self, response):
        """第三步：识别验证码并发起单次药品列表查询"""
        # 从 meta 中取出当前要查询的关键字 或 待重试的 payload
        current_keyword = response.meta.get('keyword')
        retry_payload = response.meta.get('retry_payload') # 如果是重试，会有这个值
        
        try:
            res_json = json.loads(response.text)
            if not res_json.get("success"):
                self.logger.error(f"[{current_keyword}] 验证码获取接口报错: {response.text}")
                return

            data = res_json.get("data", {})
            base64_str = data.get("base64Str", "")
            random_str = data.get("randomStr", "")
            resp_text = data.get("text", "") 

            if not base64_str:
                self.logger.error(f"[{current_keyword}] 未找到验证码图片数据")
                return

            # ddddocr 识别
            ocr = ddddocr.DdddOcr(show_ad=False)
            img_bytes = base64.b64decode(base64_str.split(',')[-1])
            code_result = ocr.classification(img_bytes)
            
            self.logger.info(f"[{current_keyword}] 验证码识别: {code_result}")

            # 构造请求 Payload
            if retry_payload:
                # 场景A：这是验证码错误后的重试，使用之前的 payload，只更新验证码部分
                payload = retry_payload
                payload.update({
                    "randomStr": random_str,
                    "text": resp_text,
                    "code": code_result
                })
            else:
                # 场景B：这是全新的关键词请求
                payload = {
                    "current": 1,
                    "size": 100,
                    "randomStr": random_str,
                    "text": resp_text, 
                    "prodCode": "",
                    "prodName": current_keyword,  # 使用 Meta 传递进来的具体关键词
                    "prodentpName": "",
                    "purchaseType": "",
                    "queryType": "1",
                    "code": code_result
                }

            # 发起真正的查询请求（不再有 for 循环）
            yield JsonRequest(
                url=self.list_api_url,
                method='POST',
                data=payload,
                callback=self.parse_list,
                meta={
                    'payload': payload,          # 携带 payload 用于翻页或重试
                    'keyword': current_keyword,  # 持续携带 keyword 用于日志或逻辑判断
                    'retry_count': response.meta.get('retry_count', 0)
                },
                dont_filter=True
            )

        except Exception as e:
            self.logger.error(f"[{current_keyword}] 解析验证码响应异常: {e}")

    def parse_list(self, response):
        """第四步：解析药品列表"""
        current_keyword = response.meta.get('keyword', 'Unknown')
        
        try:
            res_json = json.loads(response.text)
            
            if not res_json.get("success"):
                error_code = res_json.get("code")
                # 检查是否为验证码错误（code=160003）
                if error_code == 160003:
                    retry_count = response.meta.get('retry_count', 0)
                    max_retries = 5
                    
                    if retry_count < max_retries:
                        self.logger.warning(f"[{current_keyword}] 验证码错误，准备重试 ({retry_count + 1}/{max_retries})")
                        
                        # 重新获取验证码
                        timestamp = int(time.time() * 1000)
                        captcha_url = f"{self.captcha_url}?timestamp={timestamp}"
                        
                        # 获取当前的 payload，准备传回 parse_captcha 进行修正
                        current_payload = response.meta.get('payload')
                        
                        yield JsonRequest(
                            url=captcha_url,
                            method='GET',
                            callback=self.parse_captcha,
                            meta={
                                'keyword': current_keyword,
                                'retry_count': retry_count + 1,
                                'retry_payload': current_payload # 关键：将旧的 payload 传回去
                            },
                            dont_filter=True
                        )
                        return # 结束当前流程，等待重试请求的回调
                    else:
                        self.logger.error(f"[{current_keyword}] 验证码错误次数过多，放弃该词")
                        return
                else:
                    self.logger.warning(f"[{current_keyword}] 列表请求异常: {response.text[:100]}")
                    return

            # --- 正常数据处理逻辑 ---
            data_block = res_json.get("data", {})
            records = data_block.get("records", [])
            current_page = data_block.get("current", 1)
            total_pages = data_block.get("pages", 0)

            self.logger.info(f"[{current_keyword}] 数据解析: 第 {current_page}/{total_pages} 页，条数: {len(records)}")

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
                            'keyword': current_keyword
                        },
                        dont_filter=True
                    )
                else:
                    # 无 pubonlnId 情况
                    item = ShandongDrugItem()
                    for k, v in base_info.items():
                        item[k] = v
                    item.generate_md5_id()
                    yield item

            # 翻页逻辑
            if current_page < total_pages:
                next_page = current_page + 1
                new_payload = response.meta['payload'].copy()
                new_payload['current'] = next_page
                
                # 翻页通常不需要重新验证码（除非Token过期），直接发请求
                # 如果翻页也需要验证码，逻辑会更复杂，这里假设Session保持内翻页无需验证码
                yield JsonRequest(
                    url=self.list_api_url,
                    method='POST',
                    data=new_payload,
                    callback=self.parse_list,
                    meta={
                        'payload': new_payload,
                        'keyword': current_keyword
                    },
                    dont_filter=True
                )

        except Exception as e:
            self.logger.error(f"[{current_keyword}] 解析药品列表页异常: {e}")

    def parse_hospital(self, response):
        # 医院解析逻辑保持不变，只需注意传递 meta
        base_info = response.meta['base_info']
        current_payload = response.meta['payload']
        # ... (其余原有逻辑) ...
        # (这里为了节省篇幅，假设你原有的parse_hospital逻辑没问题，直接复制原有的即可)
        # 只有一点建议：Log里加上 keyword 方便调试
        
        try:
            res_json = json.loads(response.text)
            # ... 检查 success ...
            if not res_json.get("success"):
                 self.logger.warning(f"医院接口请求失败: {response.text[:100]}")
                 return

            data = res_json.get("data", {})
            records = data.get("records", [])
            current_page = data.get("current", 1)
            total_pages = data.get("pages", 0)
            
            # ... 生成 Item ...
            if not records:
                item = ShandongDrugItem()
                for k, v in base_info.items():
                    item[k] = v
                item.generate_md5_id()
                yield item
            else:
                for hosp in records:
                    item = ShandongDrugItem()
                    for k, v in base_info.items():
                        item[k] = v
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
                        'keyword': response.meta.get('keyword')
                    },
                    dont_filter=True
                )
        except Exception as e:
            self.logger.error(f"解析医院详情页异常: {e}")
import scrapy
import json
import random
import string
from ..models.tianjin_drug import TianjinDrugItem
from scrapy.http import JsonRequest
import pandas as pd

df_name = pd.read_excel(r"/Users/jcagito/Desktop/未命名文件夹/gen_spider/脚本文件夹/关键字采集(2).xlsx")
product_list =df_name.loc[:,"采集关键字"].to_list()

class TianjinDrugSpider(scrapy.Spider):
    """
    天津市医药采购中心 - 药品及配送医院查询
    Target: https://tps.ylbz.tj.gov.cn
    """
    name = "tianjin_drug_spider"
    
    # 接口地址
    drug_list_url = "https://tps.ylbz.tj.gov.cn/csb/1.0.0/guideGetMedList"
    hospital_list_url = "https://tps.ylbz.tj.gov.cn/csb/1.0.0/guideGetHosp"
    
    # 搜索关键词列表 (可根据需求扩展)
    search_contents = product_list

    custom_settings = {
        'CONCURRENT_REQUESTS': 3, # 稍微降低并发，避免验证码接口风控过严
        'DOWNLOAD_DELAY': 3,
        'DEFAULT_REQUEST_HEADERS': {
            'Content-Type': 'application/json',
            'Accept': 'application/json, text/plain, */*',
            'Sec-Fetch-Site': 'same-origin',
            'Accept-Language': 'zh-CN,zh-Hans;q=0.9',
            'Sec-Fetch-Mode': 'cors',
            'Origin': 'https://tps.ylbz.tj.gov.cn',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15',
            'Referer': 'https://tps.ylbz.tj.gov.cn/drugGuide/tps-local/b/',
            'Sec-Fetch-Dest': 'empty',
            'Priority': 'u=3, i'
        },
        'ITEM_PIPELINES': {
            'hybrid_crawler.pipelines.DataCleaningPipeline': 300,        # 清洗
            'hybrid_crawler.pipelines.TianjinDrugPipeline': 400,           # 入库
        }
    }

    def get_verification_code(self):
        """生成4位随机字母数字混合验证码"""
        chars = string.ascii_lowercase + string.digits
        return ''.join(random.choices(chars, k=4))

    def start_requests(self):
        """遍历关键词发起请求"""
        for content in self.search_contents:
            payload = {
                "verificationCode": self.get_verification_code(),
                "content": content
            }
            
            yield JsonRequest(
                url=self.drug_list_url,
                method='POST',
                data=payload,
                callback=self.parse_drug_list,
                meta={'keyword': content},
                dont_filter=True
            )

    def parse_drug_list(self, response):
        """解析药品列表"""
        try:
            res_json = json.loads(response.text)
            
            # 检查响应状态
            if res_json.get("code") != 200:
                self.logger.error(f"Drug List Error for {response.meta['keyword']}: {res_json.get('message')}")
                return

            data = res_json.get("data", {})
            drug_list = data.get("list", [])
            
            if not drug_list:
                self.logger.info(f"No drugs found for keyword: {response.meta['keyword']}")
                return

            self.logger.info(f"Found {len(drug_list)} drugs for keyword: {response.meta['keyword']}")

            for drug in drug_list:
                # 1. 提取药品基础信息
                base_info = {
                    'med_id': drug.get('medid'),
                    'gen_name': drug.get('genname'),
                    'prod_name': drug.get('prodname'),
                    'dosform': drug.get('dosform'),
                    'spec': drug.get('spec'),
                    'pac': drug.get('pac'),
                    'conv_rat': drug.get('convrat'),
                    'min_sal_unt': drug.get('minSalunt'),
                    'prod_entp': drug.get('prodentp'),
                    'aprv_no': drug.get('aprvno'),
                    'source_data': json.dumps(drug, ensure_ascii=False)
                }

                # 2. 构建医院查询参数
                # 根据curl示例，需要使用药品详情中的 specific fields
                hospital_payload = {
                    "verificationCode": self.get_verification_code(),
                    "genname": drug.get('genname'),
                    "dosform": drug.get('dosform'),
                    "spec": drug.get('spec'),
                    "pac": drug.get('pac')
                }

                # 发起医院详情请求
                yield JsonRequest(
                    url=self.hospital_list_url,
                    method='POST',
                    data=hospital_payload,
                    callback=self.parse_hospital_list,
                    meta={'base_info': base_info},
                    dont_filter=True
                )

        except Exception as e:
            self.logger.error(f"Parse Drug List Failed: {e}", exc_info=True)

    def parse_hospital_list(self, response):
        """解析医院列表并生成最终Item"""
        base_info = response.meta['base_info']
        
        try:
            res_json = json.loads(response.text)
            
            if res_json.get("code") != 200:
                self.logger.warning(f"Hospital List API Warning: {res_json.get('message', '')} | Drug: {base_info['gen_name']}")
                # 即使医院接口报错，也可以选择保存药品基础信息，这里暂且跳过
                return

            data = res_json.get("data", {})
            hosp_list = data.get("list", [])
            
            if hosp_list:
                for hosp in hosp_list:
                    item = TianjinDrugItem()
                    item.update(base_info)
                    
                    # 注入医院信息
                    item['has_hospital_record'] = True
                    item['hs_name'] = hosp.get('hsname')
                    item['hs_lav'] = hosp.get('hslav')
                    item['got_time'] = hosp.get('gottime')
                    
                    item.generate_md5_id()
                    yield item
            else:
                # 无医院记录，仅保存药品信息
                item = TianjinDrugItem()
                item.update(base_info)
                item['has_hospital_record'] = False
                item['hs_name'] = None
                item['hs_lav'] = None
                item['got_time'] = None
                
                item.generate_md5_id()
                yield item

        except Exception as e:
            self.logger.error(f"Parse Hospital List Failed: {e}", exc_info=True)
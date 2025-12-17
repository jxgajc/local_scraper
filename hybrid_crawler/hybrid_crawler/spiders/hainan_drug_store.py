import scrapy
import json
from urllib.parse import urlencode
from ..models.hainan_drug import HainanDrugItem
import pandas as pd

df_name = pd.read_excel(r"/Users/jcagito/Desktop/未命名文件夹/gen_spider/脚本文件夹/关键字采集(2).xlsx")
product_list =df_name.loc[:,"采集关键字"].to_list()

class HainanDrugSpider(scrapy.Spider):
    """
    海南省医保服务平台 - 药品门店查询爬虫
    Target: https://ybj.hainan.gov.cn
    """
    name = "hainan_drug_spider"
    
    # API Endpoints
    list_api_base = "https://ybj.hainan.gov.cn/tps-local/local/web/std/drugStore/getDrugStore"
    detail_api_base = "https://ybj.hainan.gov.cn/tps-local/local/web/std/drugStore/getDrugStoreDetl"
    
    # 搜索关键词
    keywords = product_list

    custom_settings = {
        'CONCURRENT_REQUESTS': 5,
        'DOWNLOAD_DELAY': 3,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Referer': 'https://ybj.hainan.gov.cn/tps-local/b/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"'
        },
        'ITEM_PIPELINES': {
            'hybrid_crawler.pipelines.DataCleaningPipeline': 300,        # 清洗
            'hybrid_crawler.pipelines.HainanDrugPipeline': 400,           # 入库
        }
    }

    def start_requests(self):
        """遍历关键词，发起列表请求"""
        for keyword in self.keywords:
            params = {
                'current': 1,
                'size': 500,  # 可以适当增大size
                'prodName': keyword
            }
            url = f"{self.list_api_base}?{urlencode(params)}"
            
            yield scrapy.Request(
                url=url,
                callback=self.parse_list,
                meta={'keyword': keyword, 'current_page': 1, 'page_size': 500}
            )

    def parse_list(self, response):
        """解析药品列表并处理翻页"""
        try:
            res_json = json.loads(response.text)
            if res_json.get("code") != 0:
                self.logger.error(f"List API Error: {res_json.get('msg', 'Unknown Error')}")
                return

            data = res_json.get("data", {})
            records = data.get("records", [])
            total_pages = data.get("pages", 0)
            current_page = response.meta['current_page']

            self.logger.info(f"[{response.meta['keyword']}] List Page {current_page}/{total_pages}, Found {len(records)} drugs")

            for record in records:
                # 1. 提取药品基础信息
                base_info = {
                    'drug_code': record.get('prodCode'),
                    'prod_name': record.get('prodName'),
                    'dosform': record.get('dosform'),
                    'spec': record.get('prodSpec'),
                    'pac': record.get('prodPac'),
                    'conv_rat': record.get('convrat'),
                    'prod_entp': record.get('prodentpName'),
                    'dcla_entp': record.get('dclaEntpName'),
                    'aprv_no': record.get('aprvno'),
                    'source_data': json.dumps(record, ensure_ascii=False)
                }

                # 2. 如果有药品编码，查询门店详情
                drug_code = record.get('prodCode')
                if drug_code:
                    detail_params = {
                        'current': 1,
                        'size': 20,
                        'drugCode': drug_code
                    }
                    detail_url = f"{self.detail_api_base}?{urlencode(detail_params)}"
                    
                    yield scrapy.Request(
                        url=detail_url,
                        callback=self.parse_detail,
                        meta={
                            'base_info': base_info,
                            'current_page': 1,
                            'page_size': 500,
                            'drug_code': drug_code
                        }
                    )
                else:
                    # 无编码，直接保存
                    item = HainanDrugItem()
                    item.update(base_info)
                    item['has_shop_record'] = False
                    item.generate_md5_id()
                    yield item

            # 3. 列表页翻页
            if current_page < total_pages:
                next_page = current_page + 1
                params = {
                    'current': next_page,
                    'size': response.meta['page_size'],
                    'prodName': response.meta['keyword']
                }
                url = f"{self.list_api_base}?{urlencode(params)}"
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_list,
                    meta={
                        'keyword': response.meta['keyword'],
                        'current_page': next_page,
                        'page_size': response.meta['page_size']
                    }
                )

        except Exception as e:
            self.logger.error(f"Parse List Failed: {e}", exc_info=True)

    def parse_detail(self, response):
        """解析门店/医院详情并处理翻页"""
        base_info = response.meta['base_info']
        current_page = response.meta['current_page']
        drug_code = response.meta['drug_code']

        try:
            res_json = json.loads(response.text)
            if res_json.get("code") != 0:
                self.logger.warning(f"Detail API Error for {drug_code}: {res_json.get('msg')}")
                return

            data = res_json.get("data", {})
            records = data.get("records", [])
            total_pages = data.get("pages", 0)

            if records:
                self.logger.info(f"[{base_info['prod_name']}] Detail Page {current_page}/{total_pages}, Found {len(records)} shops")
                
                for shop in records:
                    item = HainanDrugItem()
                    item.update(base_info)
                    
                    # 注入门店信息
                    item['has_shop_record'] = True
                    item['shop_name'] = shop.get('medinsName')
                    item['shop_code'] = shop.get('medinsCode')
                    item['shop_type_memo'] = shop.get('memo')
                    item['price'] = shop.get('pric')
                    item['inventory'] = shop.get('invCnt')
                    item['update_time'] = shop.get('invChgTime')
                    item['hilist_name'] = shop.get('fixmedinsHilistName')
                    
                    # 更新 source_data 包含两部分信息
                    full_source = {
                        "drug_info": json.loads(base_info['source_data']),
                        "shop_info": shop
                    }
                    item['source_data'] = json.dumps(full_source, ensure_ascii=False)
                    
                    item.generate_md5_id()
                    yield item
            elif current_page == 1:
                # 第一页就没数据，说明该药没库存记录，保存一条基础信息
                item = HainanDrugItem()
                item.update(base_info)
                item['has_shop_record'] = False
                item.generate_md5_id()
                yield item

            # 详情页翻页
            if current_page < total_pages:
                next_page = current_page + 1
                params = {
                    'current': next_page,
                    'size': response.meta['page_size'],
                    'drugCode': drug_code
                }
                url = f"{self.detail_api_base}?{urlencode(params)}"
                
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_detail,
                    meta={
                        'base_info': base_info,
                        'current_page': next_page,
                        'page_size': response.meta['page_size'],
                        'drug_code': drug_code
                    }
                )

        except Exception as e:
            self.logger.error(f"Parse Detail Failed: {e}", exc_info=True)
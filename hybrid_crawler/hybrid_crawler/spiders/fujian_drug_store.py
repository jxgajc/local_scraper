import scrapy
import json
import uuid
from ..models.fujian_drug import FujianDrugItem
from scrapy.http import JsonRequest
from ..utils.logger_utils import get_spider_logger
from .mixins import SpiderStatusMixin

class FujianDrugSpider(SpiderStatusMixin, scrapy.Spider):
    """
    福建省医疗保障局 - 药品挂网及采购医院查询
    Target: https://open.ybj.fujian.gov.cn:10013/tps-local/#/external/product-publicity
    """
    name = "fujian_drug_spider"
    
    # API Endpoints
    list_api_url = "https://open.ybj.fujian.gov.cn:10013/tps-local/web/tender/plus/item-cfg-info/list"
    hospital_api_url = "https://open.ybj.fujian.gov.cn:10013/tps-local/web/trans/api/open/v2/queryHospital"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spider_log = get_spider_logger(self.name)
        self.crawl_id = str(uuid.uuid4())
        self.spider_log.info(f"🚀 爬虫初始化完成，crawl_id: {self.crawl_id}")

    custom_settings = {
        'CONCURRENT_REQUESTS': 5, # 保持并发限制
        'DOWNLOAD_DELAY': 3,
        'DEFAULT_REQUEST_HEADERS': {
            'Content-Type': 'application/json;charset=utf-8',
            'Accept': 'application/json, text/plain, */*',
            # 'User-Agent': 由 RandomUserAgentMiddleware 自动设置
            'Origin': 'https://open.ybj.fujian.gov.cn:10013',
            'Referer': 'https://open.ybj.fujian.gov.cn:10013/tps-local/',
            'prodType': '2',
            'Priority': 'u=3, i'
        },
        # Pipeline 配置已移至全局 settings.py
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
        
        self.spider_log.info(f"📋 开始采集药品列表，初始payload: {json.dumps(payload)}")
        
        yield JsonRequest(
            url=self.list_api_url,
            method='POST',
            data=payload,
            callback=self.parse_drug_list,
            meta={'payload': payload, 'crawl_id': self.crawl_id},
            dont_filter=True
        )

    def parse_drug_list(self, response):
        """解析药品列表"""
        page_crawl_id = str(uuid.uuid4())
        current_payload = response.meta['payload']
        
        try:
            res_json = json.loads(response.text)
            if res_json.get("code") != 0:
                error_msg = res_json.get('message', 'Unknown error')
                self.spider_log.error(f"❌ 药品列表API错误 (Page {current_payload['current']}): {error_msg}")
                
                yield self.report_error(
                    stage='list_page',
                    error_msg=error_msg,
                    crawl_id=page_crawl_id,
                    params=current_payload,
                    api_url=self.list_api_url,
                    parent_crawl_id=self.crawl_id
                )
                return

            data_block = res_json.get("data", {})
            records = data_block.get("records", [])
            current_page = data_block.get("current", 1)
            total_pages = data_block.get("pages", 0)
            page_size = data_block.get("size", 1000)

            self.spider_log.info(f"📄 药品列表页面 [{current_page}/{total_pages}] - 发现 {len(records)} 条药品记录")
            
            yield self.report_list_page(
                crawl_id=page_crawl_id,
                page_no=current_page,
                total_pages=total_pages,
                items_found=len(records),
                params=current_payload,
                api_url=self.list_api_url,
                parent_crawl_id=self.crawl_id,
                page_size=page_size
            )

            item_count = 0
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
                            'payload': hospital_payload,
                            'parent_crawl_id': page_crawl_id,
                            'drug_name': base_info['drug_name']
                        },
                        dont_filter=True
                    )
                    item_count += 1
                else:
                    item = FujianDrugItem()
                    item.update(base_info)
                    item['has_hospital_record'] = False
                    item.generate_md5_id()
                    yield item
                    item_count += 1

            # 更新页面采集状态，记录成功存储的条数
            yield self.report_list_page(
                crawl_id=page_crawl_id,
                page_no=current_page,
                total_pages=total_pages,
                items_found=len(records),
                params=current_payload,
                api_url=self.list_api_url,
                parent_crawl_id=self.crawl_id,
                page_size=page_size,
                items_stored=item_count
            )

            # 3. 药品列表翻页
            if current_page < total_pages:
                self.spider_log.info(f"🔄 准备采集下一页药品列表 [{current_page + 1}/{total_pages}]")
                next_payload = current_payload.copy()
                next_payload['current'] = current_page + 1
                
                yield JsonRequest(
                    url=self.list_api_url,
                    method='POST',
                    data=next_payload,
                    callback=self.parse_drug_list,
                    meta={'payload': next_payload, 'crawl_id': self.crawl_id},
                    dont_filter=True
                )
            else:
                self.spider_log.info(f"✅ 药品列表采集完成，共 {total_pages} 页")

        except Exception as e:
            self.spider_log.error(f"❌ 解析药品列表失败 (Page {current_payload.get('current', 1)}): {e}", exc_info=True)
            
            yield self.report_error(
                stage='list_page',
                error_msg=e,
                crawl_id=page_crawl_id,
                params=current_payload,
                api_url=self.list_api_url,
                parent_crawl_id=self.crawl_id
            )

    def parse_hospital(self, response):
        """解析医院列表（嵌套JSON解析）"""
        base_info = response.meta['base_info']
        current_payload = response.meta['payload']
        parent_crawl_id = response.meta['parent_crawl_id']
        drug_name = response.meta['drug_name']
        hospital_crawl_id = str(uuid.uuid4())

        try:
            res_json = json.loads(response.text)
            inner_data_str = res_json.get("data")
            
            if not inner_data_str or not isinstance(inner_data_str, str):
                self.spider_log.warning(f"⚠️ 药品 [{drug_name}] 医院数据格式异常，返回空记录")
                
                yield self.report_detail_page(
                    crawl_id=hospital_crawl_id,
                    page_no=current_payload['pageNo'],
                    items_found=0,
                    params=current_payload,
                    api_url=self.hospital_api_url,
                    parent_crawl_id=parent_crawl_id,
                    reference_id=base_info['ext_code'],
                    items_stored=1,
                    total_pages=1
                )
                
                item = FujianDrugItem()
                item.update(base_info)
                item['has_hospital_record'] = False
                item.generate_md5_id()
                yield item
                return

            inner_json = json.loads(inner_data_str)
            hospitals = inner_json.get("data", [])
            total_records = int(inner_json.get("total", 0))
            current_page = int(inner_json.get("pageNo", 1))
            page_size = int(inner_json.get("pageSize", 100))
            
            total_pages = (total_records + page_size - 1) // page_size if total_records > 0 else 1

            self.spider_log.info(f"🏥 药品 [{drug_name}] 医院列表 [{current_page}/{total_pages}] - 发现 {len(hospitals)} 条医院记录")
            
            # 优化：移除这里冗余的 report_detail_page 调用，只在最后处理完数据后上报一次
            # 减少数据库并发压力

            item_count = 0
            if hospitals:
                for hosp in hospitals:
                    item = FujianDrugItem()
                    item.update(base_info)
                    
                    item['has_hospital_record'] = True
                    item['hospital_name'] = hosp.get('hospitalName')
                    item['medins_code'] = hosp.get('medinsCode')
                    item['area_name'] = hosp.get('areaName')
                    item['area_code'] = hosp.get('areaCode')
                    
                    full_source = {
                        "drug_info": json.loads(base_info['source_data']),
                        "hospital_info": hosp
                    }
                    item['source_data'] = json.dumps(full_source, ensure_ascii=False)
                    
                    item.generate_md5_id()
                    yield item
                    item_count += 1
                
                yield self.report_detail_page(
                    crawl_id=hospital_crawl_id,
                    page_no=current_page,
                    items_found=len(hospitals),
                    params=current_payload,
                    api_url=self.hospital_api_url,
                    parent_crawl_id=parent_crawl_id,
                    reference_id=base_info['ext_code'],
                    total_pages=total_pages,
                    items_stored=item_count
                )
                
                if current_page < total_pages:
                    self.spider_log.info(f"🔄 准备采集药品 [{drug_name}] 下一页医院列表 [{current_page + 1}/{total_pages}]")
                    next_payload = current_payload.copy()
                    next_payload['pageNo'] = current_page + 1
                    
                    yield JsonRequest(
                        url=self.hospital_api_url,
                        method='POST',
                        data=next_payload,
                        callback=self.parse_hospital,
                        meta={
                            'base_info': base_info,
                            'payload': next_payload,
                            'parent_crawl_id': parent_crawl_id,
                            'drug_name': drug_name
                        },
                        dont_filter=True
                    )
            else:
                if current_page == 1:
                    self.spider_log.info(f"📋 药品 [{drug_name}] 没有医院采购记录")
                    
                    yield self.report_detail_page(
                        crawl_id=hospital_crawl_id,
                        page_no=current_page,
                        items_found=0,
                        params=current_payload,
                        api_url=self.hospital_api_url,
                        parent_crawl_id=parent_crawl_id,
                        reference_id=base_info['ext_code'],
                        total_pages=total_pages,
                        items_stored=1
                    )
                    
                    item = FujianDrugItem()
                    item.update(base_info)
                    item['has_hospital_record'] = False
                    item.generate_md5_id()
                    yield item
                    item_count += 1

        except Exception as e:
            self.spider_log.error(f"❌ 药品 [{drug_name}] 医院查询失败: {e}", exc_info=True)
            
            yield self.report_error(
                stage='detail_page',
                error_msg=e,
                crawl_id=hospital_crawl_id,
                params=current_payload,
                api_url=self.hospital_api_url,
                parent_crawl_id=parent_crawl_id,
                reference_id=base_info['ext_code']
            )

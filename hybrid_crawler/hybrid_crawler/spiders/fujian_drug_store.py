import scrapy
import json
import uuid
import requests
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

    # 补采配置
    recrawl_config = {
        'table_name': 'drug_hospital_fujian_test',
        'unique_id': 'ext_code',
    }

    @classmethod
    def fetch_all_ids_from_api(cls, logger=None, stop_check=None):
        """
        从官网 API 获取所有 ext_code 及其基础信息
        返回: {ext_code: base_info} 字典
        """
        api_data = {}  # {ext_code: base_info}
        current = 1
        page_size = 1000
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
        })

        while True:
            if stop_check and stop_check():
                break
            try:
                payload = {
                    "druglistName": "", "druglistCode": "", "drugName": "",
                    "ruteName": "", "dosformName": "", "specName": "",
                    "pac": "", "prodentpName": "", "current": current,
                    "size": page_size, "tenditmType": ""
                }
                response = session.post(cls.list_api_url, json=payload, timeout=30)
                response.raise_for_status()
                res_json = response.json()

                if res_json.get("code") != 0:
                    break

                data_block = res_json.get("data", {})
                records = data_block.get("records", [])
                total_pages = data_block.get("pages", 0)

                for record in records:
                    ext_code = record.get('extCode')
                    if ext_code:
                        api_data[ext_code] = {
                            'ext_code': ext_code,
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

                if logger:
                    logger.info(f"福建API第{current}/{total_pages}页，获取{len(records)}条")

                if current >= total_pages:
                    break
                current += 1
            except Exception as e:
                if logger:
                    logger.error(f"请求福建API失败: {e}")
                break

        return api_data

    @classmethod
    def recrawl_by_ids(cls, missing_data, db_session, logger=None):
        """
        根据缺失的 ext_code 及其基础信息直接调用详情API进行补采

        Args:
            missing_data: {ext_code: base_info} 字典，包含缺失记录的基础信息
            db_session: 数据库会话
            logger: 日志记录器
        """
        from ..models.fujian_drug import FujianDrug
        from datetime import datetime
        import hashlib
        import time

        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json;charset=utf-8',
        })

        success_count = 0
        for ext_code, base_info in missing_data.items():
            try:
                hospital_payload = {
                    "area": "", "hospitalName": "", "pageNo": 1,
                    "pageSize": 100, "productId": ext_code, "tenditmType": ""
                }

                resp = session.post(cls.hospital_api_url, json=hospital_payload, timeout=30)
                resp.raise_for_status()
                res_json = resp.json()

                inner_data_str = res_json.get("data")
                if inner_data_str and isinstance(inner_data_str, str):
                    inner_json = json.loads(inner_data_str)
                    hospitals = inner_json.get("data", [])

                    if hospitals:
                        for hosp in hospitals:
                            record = FujianDrug(
                                **base_info,
                                has_hospital_record=True,
                                hospital_name=hosp.get('hospitalName'),
                                medins_code=hosp.get('medinsCode'),
                                area_name=hosp.get('areaName'),
                                area_code=hosp.get('areaCode'),
                                collect_time=datetime.now()
                            )
                            field_values = {
                                'ext_code': ext_code,
                                'hospital_name': hosp.get('hospitalName'),
                                'medins_code': hosp.get('medinsCode'),
                            }
                            record.md5_id = hashlib.md5(
                                json.dumps(field_values, sort_keys=True, ensure_ascii=False).encode()
                            ).hexdigest()
                            db_session.add(record)
                    else:
                        record = FujianDrug(
                            **base_info,
                            has_hospital_record=False,
                            collect_time=datetime.now()
                        )
                        record.md5_id = hashlib.md5(ext_code.encode()).hexdigest()
                        db_session.add(record)

                success_count += 1
                if logger:
                    logger.info(f"补采 ext_code={ext_code} 成功")
                
                time.sleep(3)

            except Exception as e:
                if logger:
                    logger.error(f"补采 ext_code={ext_code} 失败: {e}")

        db_session.commit()
        return success_count

    def __init__(self, recrawl_ids=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spider_log = get_spider_logger(self.name)
        self.crawl_id = str(uuid.uuid4())
        # 补采模式：只采集指定的 ext_code
        self.recrawl_ids = set(recrawl_ids.split(',')) if recrawl_ids else None
        self.recrawl_mode = self.recrawl_ids is not None
        mode_str = f"补采模式，目标 {len(self.recrawl_ids)} 条" if self.recrawl_mode else "全量采集"
        self.spider_log.info(f"🚀 爬虫初始化完成，crawl_id: {self.crawl_id}，模式: {mode_str}")

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
                ext_code = record.get('extCode')
                # 补采模式：跳过不在目标列表中的记录
                if self.recrawl_mode:
                    if ext_code not in self.recrawl_ids:
                        continue
                    self.recrawl_ids.discard(ext_code)  # 已处理，从列表移除

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
                # 补采模式：如果所有目标都已采集完成，提前结束
                if self.recrawl_mode and not self.recrawl_ids:
                    self.spider_log.info(f"✅ 补采模式：所有目标数据已采集完成")
                    return

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

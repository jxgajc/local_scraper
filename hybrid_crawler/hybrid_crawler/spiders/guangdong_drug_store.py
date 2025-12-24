import scrapy
import json
import uuid
from ..models.guangdong_drug import GuangdongDrugItem
from scrapy.http import JsonRequest
from ..utils.logger_utils import get_spider_logger
from .mixins import SpiderStatusMixin

class GuangdongDrugSpider(SpiderStatusMixin, scrapy.Spider):
    """
    广东省药品挂网及采购医院列表爬虫
    Target: https://igi.hsa.gd.gov.cn
    """
    name = "guangdong_drug_spider"
    
    # API Endpoints
    list_api_url = "https://igi.hsa.gd.gov.cn/tps_local_bd/web/publicity/pubonlnPublicity/queryPubonlnPage"
    hospital_api_url = "https://igi.hsa.gd.gov.cn/tps_local_bd/web/publicity/pubonlnPublicity/getPurcHospitalInfoListNew"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spider_log = get_spider_logger(self.name)
        self.crawl_id = str(uuid.uuid4())
        self.spider_log.info(f"🚀 爬虫初始化完成，crawl_id: {self.crawl_id}")

    custom_settings = {
        'CONCURRENT_REQUESTS': 5,
        'DOWNLOAD_DELAY': 3,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Origin': 'https://igi.hsa.gd.gov.cn',
            'Referer': 'https://igi.hsa.gd.gov.cn/tps/tps_public/publicity/listPubonlnPublicityD',
            # 'User-Agent': Handled by RandomUserAgentMiddleware
            'Authorization': 'null',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
        },
        # Pipeline 配置已移至全局 settings.py
    }

    def start_requests(self):
        """Initial request to the drug list API"""
        payload = {
            "current": 1,
            "size": 500,
            "searchCount": True
        }
        
        self.spider_log.info(f"📋 开始采集药品列表，初始payload: {json.dumps(payload)}")
        
        yield JsonRequest(
            url=self.list_api_url,
            method='POST',
            data=payload,
            callback=self.parse_list,
            meta={'payload': payload, 'crawl_id': self.crawl_id},
            dont_filter=True
        )

    def parse_list(self, response):
        """Parse drug list and trigger hospital queries"""
        page_crawl_id = str(uuid.uuid4())
        current_payload = response.meta['payload']
        parent_crawl_id = response.meta['crawl_id']
        
        try:
            res_json = json.loads(response.text)
            if not res_json.get("success"):
                error_msg = res_json.get('message', 'Unknown Error')
                self.spider_log.error(f"❌ 药品列表API错误 (Page {current_payload['current']}): {error_msg}")
                
                yield self.report_error(
                    stage='list_page',
                    error_msg=error_msg,
                    crawl_id=page_crawl_id,
                    params=current_payload,
                    api_url=self.list_api_url,
                    parent_crawl_id=parent_crawl_id
                )
                return

            data_block = res_json.get("data", {})
            records = data_block.get("records", [])
            current_page = data_block.get("current", 1)
            total_pages = data_block.get("pages", 0)
            page_size = data_block.get("size", 500)

            self.spider_log.info(f"📄 药品列表页面 [{current_page}/{total_pages}] - 发现 {len(records)} 条药品记录")
            
            yield self.report_list_page(
                crawl_id=page_crawl_id,
                page_no=current_page,
                total_pages=total_pages,
                items_found=len(records),
                params=current_payload,
                api_url=self.list_api_url,
                parent_crawl_id=parent_crawl_id,
                page_size=page_size
            )

            item_count = 0
            for record in records:
                # 1. Map drug info to Item (Lossless mapping)
                base_info = {
                    'drug_id': record.get('drugId'),
                    'drug_code': record.get('drugCode'),
                    'gw_active': record.get('gwActive'),
                    'gen_name': record.get('genname'),
                    'trade_name': record.get('tradeName'),
                    'dosform_name': record.get('dosformName'),
                    'spec_name': record.get('specName'),
                    'pac_matl': record.get('pacmatl'),
                    'listing_holder': record.get('listingLicenseHolder'),
                    'prod_entp_name': record.get('prodentpName'),
                    'dcla_entp_name': record.get('dclaEntpName'),
                    'dcla_entp_uscc': record.get('dclaEntpUscc'),
                    'price': record.get('minPacPubonlnPric'),
                    'min_unit': record.get('minuntName'),
                    'min_pac_name': record.get('minpacName'),
                    'conv_rat': record.get('convrat'),
                    'aprv_no': record.get('aprvno'),
                    'reg_dosform_name': record.get('regDosformName'),
                    'reg_spec_name': record.get('regSpecName'),
                    'quality_lv': record.get('qualityLv'),
                    'jyl_category': record.get('jylCategory'),
                    'jyl_no': record.get('jylNo'),
                    'policy_att': record.get('policyAtt'),
                    'drug_select_type': record.get('drugSelectType'),
                    'formation_mode': record.get('formationMode'),
                    'pubonln_time': record.get('pubonlnTime'),
                    'erm_flag': record.get('ermFlag'),
                    'zc_spt_id': record.get('zcSptId'),
                    'exist_price_flag': record.get('existPubonlnPric'),
                    'stop_flag': record.get('stopPubonln'),
                    'source_data': json.dumps(record, ensure_ascii=False)
                }

                drug_code = record.get('drugCode')
                
                # If drugCode exists, query hospitals
                if drug_code:
                    hospital_payload = {
                        "current": 1,
                        "size": 50,
                        "searchCount": True,
                        "drugCode": drug_code
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
                            'drug_name': base_info.get('gen_name')
                        },
                        dont_filter=True
                    )
                    # Note: item_count increment happens in parse_hospital or via stored items later
                    item_count += 1 
                else:
                    # No drugCode, cannot query hospitals, save basic info
                    item = GuangdongDrugItem()
                    item.update(base_info)
                    item['has_hospital_record'] = False
                    item.generate_md5_id()
                    yield item
                    item_count += 1

            # 更新页面采集状态
            yield self.report_list_page(
                crawl_id=page_crawl_id,
                page_no=current_page,
                total_pages=total_pages,
                items_found=len(records),
                params=current_payload,
                api_url=self.list_api_url,
                parent_crawl_id=parent_crawl_id,
                page_size=page_size,
                items_stored=item_count
            )

            # Pagination for Drug List
            if current_page < total_pages:
                self.spider_log.info(f"🔄 准备采集下一页药品列表 [{current_page + 1}/{total_pages}]")
                next_page = current_page + 1
                new_payload = current_payload.copy()
                new_payload['current'] = next_page
                
                yield JsonRequest(
                    url=self.list_api_url,
                    method='POST',
                    data=new_payload,
                    callback=self.parse_list,
                    meta={'payload': new_payload, 'crawl_id': self.crawl_id},
                    dont_filter=True
                )
            else:
                self.spider_log.info(f"✅ 药品列表采集完成，共 {total_pages} 页")

        except Exception as e:
            self.spider_log.error(f"❌ 解析药品列表失败 (Page {current_payload.get('current')}): {e}", exc_info=True)
            
            yield self.report_error(
                stage='list_page',
                error_msg=e,
                crawl_id=page_crawl_id,
                params=current_payload,
                api_url=self.list_api_url,
                parent_crawl_id=parent_crawl_id
            )

    def parse_hospital(self, response):
        """Parse hospital list and yield items"""
        base_info = response.meta['base_info']
        current_payload = response.meta['payload']
        parent_crawl_id = response.meta['parent_crawl_id']
        drug_name = response.meta.get('drug_name', 'Unknown')
        hospital_crawl_id = str(uuid.uuid4())

        try:
            res_json = json.loads(response.text)
            
            if not res_json.get("success"):
                error_msg = res_json.get('message', 'Unknown Error')
                self.spider_log.warning(f"⚠️ 药品 [{drug_name}] 医院API错误: {error_msg}")
                
                # 上报错误但记录基础信息
                yield self.report_detail_page(
                    crawl_id=hospital_crawl_id,
                    page_no=current_payload['current'],
                    items_found=0,
                    params=current_payload,
                    api_url=self.hospital_api_url,
                    parent_crawl_id=parent_crawl_id,
                    reference_id=base_info['drug_code'],
                    success=False,
                    error_message=error_msg,
                    total_pages=0
                )
                return

            data = res_json.get("data", {})
            records = data.get("records", [])
            current_page = data.get("current", 1)
            total_pages = data.get("pages", 0)
            page_size = data.get("size", 50)
            
            self.spider_log.info(f"🏥 药品 [{drug_name}] 医院列表 [{current_page}/{total_pages}] - 发现 {len(records)} 条医院记录")
            
            # 优化：移除冗余状态上报，减少数据库压力

            item_count = 0
            if records:
                for hosp in records:
                    item = GuangdongDrugItem()
                    item.update(base_info)
                    
                    # --- Hospital Info Mapping ---
                    item['has_hospital_record'] = True
                    item['medins_code'] = hosp.get('medinsCode')
                    item['medins_name'] = hosp.get('medinsName')
                    item['hosp_type'] = hosp.get('type')
                    item['source_id'] = hosp.get('sourceId')
                    item['url'] = "https://igi.hsa.gd.gov.cn/tps/tps_public/publicity/listPubonlnPublicityD"
                    
                    # Parse Administration Division
                    admdvs_full = hosp.get('admdvsName', '')
                    item['admdvs_name'] = admdvs_full
                    
                    if admdvs_full:
                        parts = admdvs_full.split('＞')
                        if len(parts) >= 2:
                            item['city_name'] = parts[1]
                        if len(parts) >= 3:
                            item['area_name'] = parts[2]
                    
                    item.generate_md5_id()
                    yield item
                    item_count += 1
                
                # Pagination for Hospital List
                if current_page < total_pages:
                    self.spider_log.info(f"🔄 准备采集药品 [{drug_name}] 下一页医院列表 [{current_page + 1}/{total_pages}]")
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
                            'parent_crawl_id': parent_crawl_id,
                            'drug_name': drug_name
                        },
                        dont_filter=True
                    )
            else:
                # No hospital records found
                if current_page == 1:
                    self.spider_log.info(f"📋 药品 [{drug_name}] 没有医院采购记录")
                    item = GuangdongDrugItem()
                    item.update(base_info)
                    item['has_hospital_record'] = False
                    item['url'] = "https://igi.hsa.gd.gov.cn/tps/tps_public/publicity/listPubonlnPublicityD"
                    item.generate_md5_id()
                    yield item
                    item_count += 1
            
            # 更新状态，记录实际存储条数
            yield self.report_detail_page(
                crawl_id=hospital_crawl_id,
                page_no=current_page,
                items_found=len(records),
                params=current_payload,
                api_url=self.hospital_api_url,
                parent_crawl_id=parent_crawl_id,
                reference_id=base_info['drug_code'],
                total_pages=total_pages,
                items_stored=item_count
            )

        except Exception as e:
            self.spider_log.error(f"❌ 药品 [{drug_name}] 医院查询失败: {e}", exc_info=True)
            
            yield self.report_error(
                stage='detail_page',
                error_msg=e,
                crawl_id=hospital_crawl_id,
                params=current_payload,
                api_url=self.hospital_api_url,
                parent_crawl_id=parent_crawl_id,
                reference_id=base_info.get('drug_code')
            )

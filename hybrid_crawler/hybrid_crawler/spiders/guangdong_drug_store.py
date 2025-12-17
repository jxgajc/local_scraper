import scrapy
import json
from ..models.guangdong_drug import GuangdongDrugItem
from scrapy.http import JsonRequest

class GuangdongDrugSpider(scrapy.Spider):
    """
    广东省药品挂网及采购医院列表爬虫
    Target: https://igi.hsa.gd.gov.cn
    """
    name = "guangdong_drug_spider"

    # API Endpoints
    list_api_url = "https://igi.hsa.gd.gov.cn/tps_local_bd/web/publicity/pubonlnPublicity/queryPubonlnPage"
    hospital_api_url = "https://igi.hsa.gd.gov.cn/tps_local_bd/web/publicity/pubonlnPublicity/getPurcHospitalInfoListNew"

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
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            'Authorization': 'null', # Explicitly set as per curl
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
        },
        'ITEM_PIPELINES': {
            'hybrid_crawler.pipelines.DataCleaningPipeline': 300,        # 清洗
            'hybrid_crawler.pipelines.GuangdongDrugPipeline': 400,           # 入库
        }
    }

    def start_requests(self):
        """Initial request to the drug list API"""
        payload = {
            "current": 1,
            "size": 500,
            "searchCount": True
        }
        yield JsonRequest(
            url=self.list_api_url,
            method='POST',
            data=payload,
            callback=self.parse_list,
            meta={'payload': payload},
            dont_filter=True
        )

    def parse_list(self, response):
        """Parse drug list and trigger hospital queries"""
        try:
            res_json = json.loads(response.text)
            if not res_json.get("success"):
                self.logger.error(f"List API Error: {response.text[:100]}")
                return

            data_block = res_json.get("data", {})
            records = data_block.get("records", [])
            current_page = data_block.get("current", 1)
            total_pages = data_block.get("pages", 0)

            self.logger.info(f"[Page {current_page}/{total_pages}] Fetched {len(records)} drug records")

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
                        },
                        dont_filter=True
                    )
                else:
                    # No drugCode, cannot query hospitals, save basic info
                    item = GuangdongDrugItem()
                    item.update(base_info)
                    item['has_hospital_record'] = False
                    item.generate_md5_id()
                    yield item

            # Pagination for Drug List
            if current_page < total_pages:
                next_page = current_page + 1
                new_payload = response.meta['payload'].copy()
                new_payload['current'] = next_page
                
                yield JsonRequest(
                    url=self.list_api_url,
                    method='POST',
                    data=new_payload,
                    callback=self.parse_list,
                    meta={'payload': new_payload},
                    dont_filter=True
                )

        except Exception as e:
            self.logger.error(f"Parse List Error: {e}", exc_info=True)

    def parse_hospital(self, response):
        """Parse hospital list and yield items"""
        base_info = response.meta['base_info']
        current_payload = response.meta['payload']

        try:
            res_json = json.loads(response.text)
            
            if not res_json.get("success"):
                self.logger.warning(f"Hospital API Fail for {base_info['drug_code']}: {response.text[:100]}")
                # Yield basic info even if hospital query fails? 
                # Better to log and maybe retry, or yield basic info. Here we strictly yield only valid data.
                return

            data = res_json.get("data", {})
            records = data.get("records", [])
            current_page = data.get("current", 1)
            total_pages = data.get("pages", 0)
            
            if records:
                self.logger.info(f"[{base_info['gen_name']}] Found {len(records)} hospital records (Page {current_page}/{total_pages})")
                
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
                    # Format: "广东省＞广州市＞增城分中心"
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
            else:
                # No hospital records found
                # self.logger.debug(f"[{base_info['gen_name']}] No hospital records.")
                item = GuangdongDrugItem()
                item.update(base_info)
                item['has_hospital_record'] = False
                item['url'] = "https://igi.hsa.gd.gov.cn/tps/tps_public/publicity/listPubonlnPublicityD"
                item.generate_md5_id()
                yield item

            # Pagination for Hospital List
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
                        'payload': new_payload
                    },
                    dont_filter=True
                )

        except Exception as e:
            self.logger.error(f"Parse Hospital Error: {e}", exc_info=True)
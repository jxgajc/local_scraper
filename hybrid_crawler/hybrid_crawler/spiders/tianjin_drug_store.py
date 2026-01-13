import scrapy
import json
import random
import string
import uuid
import requests
from ..models.tianjin_drug import TianjinDrugItem
from scrapy.http import JsonRequest
from ..utils.logger_utils import get_spider_logger
import pandas as pd
import os
from .mixins import SpiderStatusMixin

# 获取脚本所在目录的绝对路径
script_dir = os.path.dirname(os.path.abspath(__file__))
# 构建Excel文件的绝对路径
excel_path = os.path.join(script_dir, "../../关键字采集(2).xlsx")

class TianjinDrugSpider(SpiderStatusMixin, scrapy.Spider):
    """
    天津市医药采购中心 - 药品及配送医院查询
    Target: https://tps.ylbz.tj.gov.cn
    """
    name = "tianjin_drug_spider"

    # 接口地址
    drug_list_url = "https://tps.ylbz.tj.gov.cn/csb/1.0.0/guideGetMedList"
    hospital_list_url = "https://tps.ylbz.tj.gov.cn/csb/1.0.0/guideGetHosp"

    # 补采配置
    recrawl_config = {
        'table_name': 'drug_hospital_tianjin_test',
        'unique_id': 'med_id',
    }

    @staticmethod
    def _get_verification_code():
        """生成4位随机字母数字混合验证码"""
        chars = string.ascii_lowercase + string.digits
        return ''.join(random.choices(chars, k=4))

    @classmethod
    def fetch_all_ids_from_api(cls, logger=None, stop_check=None):
        """
        天津爬虫需要验证码，遍历关键词获取所有数据
        返回: {med_id: base_info} 字典
        """
        api_data = {}
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Content-Type': 'application/json',
        })

        # 加载关键词
        try:
            df_name = pd.read_excel(excel_path)
            keywords = df_name.loc[:, "采集关键字"].to_list()
        except Exception as e:
            if logger:
                logger.error(f"关键词文件加载失败: {e}")
            return api_data

        for keyword in keywords:
            if stop_check and stop_check():
                break
            try:
                payload = {
                    "verificationCode": cls._get_verification_code(),
                    "content": keyword
                }
                response = session.post(cls.drug_list_url, json=payload, timeout=30)
                response.raise_for_status()
                res_json = response.json()

                if res_json.get("code") != 200:
                    continue

                data = res_json.get("data", {})
                drug_list = data.get("list", [])

                for drug in drug_list:
                    med_id = drug.get('medid')
                    if med_id:
                        api_data[med_id] = {
                            'med_id': med_id,
                            'gen_name': drug.get('genname'),
                            'prod_name': drug.get('prodname'),
                            'dosform': drug.get('dosform'),
                            'spec': drug.get('spec'),
                            'pac': drug.get('pac'),
                            'prod_entp': drug.get('prodentp'),
                            'source_data': json.dumps(drug, ensure_ascii=False)
                        }

                if logger:
                    logger.info(f"天津API关键词[{keyword}]获取{len(drug_list)}条")

            except Exception as e:
                if logger:
                    logger.error(f"请求天津API失败: {e}")

        return api_data

    @classmethod
    def recrawl_by_ids(cls, missing_data, db_session, logger=None):
        """根据缺失的 med_id 及其基础信息调用医院API进行补采"""
        from ..models.tianjin_drug import TianjinDrug
        from datetime import datetime
        import hashlib

        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Content-Type': 'application/json',
        })

        success_count = 0
        for med_id, base_info in missing_data.items():
            try:
                hospital_payload = {
                    "verificationCode": cls._get_verification_code(),
                    "genname": base_info.get('gen_name'),
                    "dosform": base_info.get('dosform'),
                    "spec": base_info.get('spec'),
                    "pac": base_info.get('pac')
                }

                resp = session.post(cls.hospital_list_url, json=hospital_payload, timeout=30)
                resp.raise_for_status()
                res_json = resp.json()

                if res_json.get("code") != 200:
                    continue

                data = res_json.get("data", {})
                hosp_list = data.get("list", [])

                if hosp_list:
                    for hosp in hosp_list:
                        record = TianjinDrug(
                            **base_info,
                            has_hospital_record=True,
                            hs_name=hosp.get('hsname'),
                            hs_lav=hosp.get('hslav'),
                            got_time=hosp.get('gottime'),
                            collect_time=datetime.now()
                        )
                        field_values = {'med_id': med_id, 'hs_name': hosp.get('hsname')}
                        record.md5_id = hashlib.md5(
                            json.dumps(field_values, sort_keys=True, ensure_ascii=False).encode()
                        ).hexdigest()
                        db_session.add(record)
                else:
                    record = TianjinDrug(
                        **base_info,
                        has_hospital_record=False,
                        collect_time=datetime.now()
                    )
                    record.md5_id = hashlib.md5(med_id.encode()).hexdigest()
                    db_session.add(record)

                success_count += 1
                if logger:
                    logger.info(f"补采 med_id={med_id} 成功")

            except Exception as e:
                if logger:
                    logger.error(f"补采 med_id={med_id} 失败: {e}")

        db_session.commit()
        return success_count
    
    def __init__(self, recrawl_ids=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spider_log = get_spider_logger(self.name)
        self.crawl_id = str(uuid.uuid4())

        # 补采模式：只采集指定的 med_id
        self.recrawl_ids = set(recrawl_ids.split(',')) if recrawl_ids else None
        self.recrawl_mode = self.recrawl_ids is not None

        # 加载关键词
        try:
            df_name = pd.read_excel(excel_path)
            self.search_contents = df_name.loc[:, "采集关键字"].to_list()
            mode_str = f"补采模式，目标 {len(self.recrawl_ids)} 条" if self.recrawl_mode else "全量采集"
            self.spider_log.info(f"🚀 爬虫初始化完成，crawl_id: {self.crawl_id}，模式: {mode_str}，加载关键词: {len(self.search_contents)} 个")
        except Exception as e:
            self.spider_log.error(f"❌ 关键词文件加载失败: {e}")
            self.search_contents = []

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
            # 'User-Agent': Handled by RandomUserAgentMiddleware
            'Referer': 'https://tps.ylbz.tj.gov.cn/drugGuide/tps-local/b/',
            'Sec-Fetch-Dest': 'empty',
            'Priority': 'u=3, i'
        },
        # Pipeline 配置已移至全局 settings.py
    }

    def get_verification_code(self):
        """生成4位随机字母数字混合验证码"""
        chars = string.ascii_lowercase + string.digits
        return ''.join(random.choices(chars, k=4))

    def start_requests(self):
        """遍历关键词发起请求"""
        total_keywords = len(self.search_contents)
        self.spider_log.info(f"📋 开始采集，共 {total_keywords} 个关键词")
        
        for index, content in enumerate(self.search_contents):
            payload = {
                "verificationCode": self.get_verification_code(),
                "content": content
            }
            
            yield JsonRequest(
                url=self.drug_list_url,
                method='POST',
                data=payload,
                callback=self.parse_drug_list,
                meta={
                    'keyword': content, 
                    'crawl_id': self.crawl_id, 
                    'payload': payload,
                    'keyword_index': index + 1,
                    'total_keywords': total_keywords
                },
                dont_filter=True
            )

    def parse_drug_list(self, response):
        """解析药品列表"""
        page_crawl_id = str(uuid.uuid4())
        keyword = response.meta['keyword']
        parent_crawl_id = response.meta['crawl_id']
        current_payload = response.meta['payload']
        
        # 使用关键词进度作为任务进度
        current_page = response.meta['keyword_index']
        total_pages = response.meta['total_keywords']
        
        try:
            res_json = json.loads(response.text)
            
            # 检查响应状态
            if res_json.get("code") != 200:
                error_msg = res_json.get('message', 'Unknown Error')
                self.spider_log.error(f"❌ 关键词 [{keyword}] 列表API错误: {error_msg}")
                
                yield self.report_error(
                    stage='list_page',
                    error_msg=error_msg,
                    crawl_id=page_crawl_id,
                    params=current_payload,
                    api_url=self.drug_list_url,
                    parent_crawl_id=parent_crawl_id,
                    reference_id=keyword
                )
                return

            data = res_json.get("data", {})
            drug_list = data.get("list", [])
            
            if not drug_list:
                self.spider_log.info(f"📄 关键词 [{keyword}] ({current_page}/{total_pages}) 未找到药品记录")
                yield self.report_list_page(
                    crawl_id=page_crawl_id,
                    page_no=current_page,
                    total_pages=total_pages,
                    items_found=0,
                    params=current_payload,
                    api_url=self.drug_list_url,
                    parent_crawl_id=parent_crawl_id,
                    reference_id=keyword
                )
                return

            self.spider_log.info(f"📄 关键词 [{keyword}] ({current_page}/{total_pages}) 发现 {len(drug_list)} 条药品记录")
            
            # 上报页面采集状态
            yield self.report_list_page(
                crawl_id=page_crawl_id,
                page_no=current_page,
                total_pages=total_pages,
                items_found=len(drug_list),
                params=current_payload,
                api_url=self.drug_list_url,
                parent_crawl_id=parent_crawl_id,
                reference_id=keyword
            )

            item_count = 0
            for drug in drug_list:
                med_id = drug.get('medid')
                # 补采模式：跳过不在目标列表中的记录
                if self.recrawl_mode:
                    if med_id not in self.recrawl_ids:
                        continue
                    self.recrawl_ids.discard(med_id)  # 已处理，从列表移除

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
                    meta={
                        'base_info': base_info, 
                        'parent_crawl_id': page_crawl_id,
                        'payload': hospital_payload
                    },
                    dont_filter=True
                )
                item_count += 1
            
            # 更新页面采集状态
            yield self.report_list_page(
                crawl_id=page_crawl_id,
                page_no=current_page,
                total_pages=total_pages,
                items_found=len(drug_list),
                items_stored=item_count,
                params=current_payload,
                api_url=self.drug_list_url,
                parent_crawl_id=parent_crawl_id,
                reference_id=keyword
            )

        except Exception as e:
            self.spider_log.error(f"❌ 解析药品列表失败: {e}", exc_info=True)
            yield self.report_error(
                stage='list_page',
                error_msg=e,
                crawl_id=page_crawl_id,
                params=current_payload,
                api_url=self.drug_list_url,
                parent_crawl_id=parent_crawl_id,
                reference_id=keyword
            )

    def parse_hospital_list(self, response):
        """解析医院列表并生成最终Item"""
        base_info = response.meta['base_info']
        parent_crawl_id = response.meta['parent_crawl_id']
        current_payload = response.meta['payload']
        detail_crawl_id = str(uuid.uuid4())
        
        try:
            res_json = json.loads(response.text)
            
            if res_json.get("code") != 200:
                msg = res_json.get('message', 'Unknown Error')
                self.spider_log.warning(f"⚠️ 药品 [{base_info['gen_name']}] 医院API警告: {msg}")
                
                # 即使医院接口报错，也可以选择保存药品基础信息
                # 但这里我们记录错误状态
                yield self.report_error(
                    stage='detail_page',
                    error_msg=msg,
                    crawl_id=detail_crawl_id,
                    params=current_payload,
                    api_url=self.hospital_list_url,
                    parent_crawl_id=parent_crawl_id,
                    reference_id=base_info.get('med_id')
                )
                return

            data = res_json.get("data", {})
            hosp_list = data.get("list", [])
            
            self.spider_log.info(f"🏥 药品 [{base_info['gen_name']}] 发现 {len(hosp_list)} 家医院")
            
            # 优化：移除冗余状态上报

            item_count = 0
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
                    item_count += 1
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
                item_count += 1
            
            # 更新详情页采集状态
            yield self.report_detail_page(
                crawl_id=detail_crawl_id,
                page_no=1,
                items_found=len(hosp_list),
                items_stored=item_count,
                params=current_payload,
                api_url=self.hospital_list_url,
                parent_crawl_id=parent_crawl_id,
                reference_id=base_info.get('med_id')
            )

        except Exception as e:
            self.spider_log.error(f"❌ 解析医院列表失败: {e}", exc_info=True)
            yield self.report_error(
                stage='detail_page',
                error_msg=e,
                crawl_id=detail_crawl_id,
                params=current_payload,
                api_url=self.hospital_list_url,
                parent_crawl_id=parent_crawl_id,
                reference_id=base_info.get('med_id')
            )

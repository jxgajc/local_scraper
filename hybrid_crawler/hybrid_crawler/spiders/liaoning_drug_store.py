from .base_spiders import BaseRequestSpider
from ..items import HybridCrawlerItem
from ..models.liaoning_drug import LiaoningDrugItem
from ..utils.logger_utils import get_spider_logger
import json
import scrapy
import time
import pandas as pd
import uuid
import requests
from scrapy.http import JsonRequest, FormRequest
import os
from .mixins import SpiderStatusMixin

# 获取脚本所在目录的绝对路径
script_dir = os.path.dirname(os.path.abspath(__file__))
# 构建Excel文件的绝对路径
excel_path = os.path.join(script_dir, "../../关键字采集(2).xlsx")

class LiaoningDrugSpider(SpiderStatusMixin, BaseRequestSpider):
    """
    辽宁药店数据爬虫
    目标: 爬取辽宁医保局药店信息
    API: http://ggzy.ln.gov.cn/yphc/gzcx/
    """
    name = "liaoning_drug_store"

    # 药品列表API URL
    list_api_url = "https://ggzy.ln.gov.cn/medical"

    # 补采配置
    recrawl_config = {
        'table_name': 'drug_hospital_liaoning_test',
        'unique_id': 'goodscode',
    }

    @classmethod
    def fetch_all_ids_from_api(cls, logger=None, stop_check=None):
        """
        辽宁爬虫是基于关键词的，需要遍历关键词获取所有数据
        返回: {goodscode: base_info} 字典
        """
        api_data = {}
        session = requests.Session()

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

            page_num = 1
            while True:
                if stop_check and stop_check():
                    break
                try:
                    form_data = {
                        "apiName": "GetYPYYCG",
                        "product": keyword,
                        "company": "",
                        "pageNum": str(page_num)
                    }
                    response = session.post(cls.list_api_url, data=form_data, timeout=30)
                    response.raise_for_status()
                    res_json = response.json()

                    data_block = res_json.get("data", {})
                    rows = data_block.get("data", [])
                    total_pages = int(data_block.get("totalPage", 0))

                    for record in rows:
                        goods_code = record.get('goodscode')
                        if goods_code:
                            api_data[goods_code] = record

                    if logger:
                        logger.info(f"辽宁API关键词[{keyword}]第{page_num}/{total_pages}页，获取{len(rows)}条")

                    if page_num >= total_pages:
                        break
                    page_num += 1
                except Exception as e:
                    if logger:
                        logger.error(f"请求辽宁API失败: {e}")
                    break

        return api_data

    @classmethod
    def recrawl_by_ids(cls, missing_data, db_session, logger=None):
        """辽宁爬虫补采 - 直接保存缺失的数据"""
        from ..models.liaoning_drug import LiaoningDrug
        from datetime import datetime
        import hashlib

        success_count = 0
        for goods_code, drug_info in missing_data.items():
            time.sleep(3)
            try:
                record = LiaoningDrug(
                    goodscode=goods_code,
                    ProductName=drug_info.get('ProductName'),
                    Spec=drug_info.get('Spec'),
                    Manufacturer=drug_info.get('Manufacturer'),
                    collect_time=datetime.now()
                )
                record.md5_id = hashlib.md5(goods_code.encode()).hexdigest()
                db_session.add(record)
                success_count += 1

                if logger:
                    logger.info(f"补采 goodscode={goods_code} 成功")
            except Exception as e:
                if logger:
                    logger.error(f"补采 goodscode={goods_code} 失败: {e}")

        db_session.commit()
        return success_count 

    def __init__(self, recrawl_ids=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spider_log = get_spider_logger(self.name)
        self.crawl_id = str(uuid.uuid4())

        # 补采模式：只采集指定的 goodscode
        self.recrawl_ids = set(recrawl_ids.split(',')) if recrawl_ids else None
        self.recrawl_mode = self.recrawl_ids is not None

        # 加载关键词
        try:
            df_name = pd.read_excel(excel_path)
            self.product_list = df_name.loc[:, "采集关键字"].to_list()
            mode_str = f"补采模式，目标 {len(self.recrawl_ids)} 条" if self.recrawl_mode else "全量采集"
            self.spider_log.info(f"🚀 爬虫初始化完成，crawl_id: {self.crawl_id}，模式: {mode_str}，加载关键词: {len(self.product_list)} 个")
        except Exception as e:
            self.spider_log.error(f"❌ 关键词文件加载失败: {e}")
            self.product_list = []

    custom_settings = {
        'CONCURRENT_REQUESTS': 8,
        'DOWNLOAD_DELAY': 3,
        # 'USER_AGENT': Handled by Middleware
        # Pipeline 配置已移至全局 settings.py
    }

    def start_requests(self):
        """构造初始的POST请求"""
        self.spider_log.info(f"📋 开始采集，共 {len(self.product_list)} 个关键词")
        
        for product in self.product_list:
            form_data = {
                "apiName": "GetYPYYCG",
                "product": product,
                "company": "",
                "pageNum": "1" # 显式转为字符串
            }
            
            self.spider_log.info(f"🔍 正在采集关键词: {product}")
            
            yield FormRequest(
                url=self.list_api_url,
                method='POST',
                formdata=form_data,
                callback=self.parse_logic,
                meta={'form_data': form_data, 'crawl_id': self.crawl_id, 'keyword': product},
                dont_filter=True
            )

    def parse_logic(self, response):
        """处理药品列表第一页响应 + 生成后续页码请求"""
        page_crawl_id = str(uuid.uuid4())
        parent_crawl_id = response.meta['crawl_id']
        current_form_data = response.meta['form_data']
        keyword = response.meta['keyword']
        
        try:
            res_json = json.loads(response.text)
            
            # 获取数据和分页信息
            data_block = res_json.get("data", {})
            rows = data_block.get("data", [])
            total_pages = int(data_block.get("totalPage", 0))
            total_records = int(data_block.get("totalData", 0))
            current_page = int(current_form_data.get("pageNum", 1))
            
            self.spider_log.info(f"📄 关键词 [{keyword}] 列表页面 [{current_page}/{total_pages}] - 发现 {len(rows)} 条记录 (总计: {total_records})")

            yield self.report_list_page(
                crawl_id=page_crawl_id,
                page_no=current_page,
                total_pages=total_pages,
                items_found=len(rows),
                params=current_form_data,
                api_url=self.list_api_url,
                parent_crawl_id=parent_crawl_id,
                reference_id=keyword
            )

            item_count = 0
            # 1. 处理当前页的每一条药品数据
            for drug_item in rows:
                goods_code = drug_item.get('goodscode')
                # 补采模式：跳过不在目标列表中的记录
                if self.recrawl_mode:
                    if goods_code not in self.recrawl_ids:
                        continue
                    self.recrawl_ids.discard(goods_code)  # 已处理，从列表移除

                yield self._create_item(drug_item, current_page)
                item_count += 1

            # 更新页面采集状态
            yield self.report_list_page(
                crawl_id=page_crawl_id,
                page_no=current_page,
                total_pages=total_pages,
                items_found=len(rows),
                params=current_form_data,
                api_url=self.list_api_url,
                parent_crawl_id=parent_crawl_id,
                reference_id=keyword,
                items_stored=item_count
            )

            # 2. 生成剩余页码请求 (从第2页开始)
            if current_page < total_pages:
                self.spider_log.info(f"🔄 准备采集关键词 [{keyword}] 后续页面 (2-{total_pages})")
                
                for next_page in range(2, total_pages + 1):
                    next_form_data = current_form_data.copy()
                    next_form_data['pageNum'] = str(next_page)
                    
                    yield FormRequest(
                        url=self.list_api_url,
                        method='POST',
                        formdata=next_form_data,
                        callback=self.parse_list_page, # 使用独立回调处理后续页面
                        meta={
                            'form_data': next_form_data, 
                            'parent_crawl_id': parent_crawl_id, # 列表页的父级是 keyword 级的 id
                            'keyword': keyword,
                            'page_num': next_page
                        },
                        dont_filter=True
                    )

        except Exception as e:
            self.spider_log.error(f"❌ 列表页解析失败 (Page 1): {e}", exc_info=True)
            
            yield self.report_error(
                stage='list_page',
                error_msg=e,
                crawl_id=page_crawl_id,
                params=current_form_data,
                api_url=self.list_api_url,
                parent_crawl_id=parent_crawl_id,
                reference_id=keyword
            )

    def parse_list_page(self, response):
        """处理后续页码的响应"""
        page_crawl_id = str(uuid.uuid4())
        parent_crawl_id = response.meta['parent_crawl_id']
        current_form_data = response.meta['form_data']
        keyword = response.meta['keyword']
        page_num = response.meta['page_num']

        try:
            res_json = json.loads(response.text)
            data_block = res_json.get("data", {})
            rows = data_block.get("data", [])
            total_pages = int(data_block.get("totalPage", 0))
            
            self.spider_log.info(f"📄 关键词 [{keyword}] 列表页面 [{page_num}/{total_pages}] - 发现 {len(rows)} 条记录")
            
            yield self.report_list_page(
                crawl_id=page_crawl_id,
                page_no=page_num,
                total_pages=total_pages,
                items_found=len(rows),
                params=current_form_data,
                api_url=self.list_api_url,
                parent_crawl_id=parent_crawl_id,
                reference_id=keyword
            )
            
            item_count = 0
            for item in rows:
                goods_code = item.get('goodscode')
                # 补采模式：跳过不在目标列表中的记录
                if self.recrawl_mode:
                    if goods_code not in self.recrawl_ids:
                        continue
                    self.recrawl_ids.discard(goods_code)  # 已处理，从列表移除

                yield self._create_item(item, page_num)
                item_count += 1
            
            yield self.report_list_page(
                crawl_id=page_crawl_id,
                page_no=page_num,
                total_pages=total_pages,
                items_found=len(rows),
                params=current_form_data,
                api_url=self.list_api_url,
                parent_crawl_id=parent_crawl_id,
                reference_id=keyword,
                items_stored=item_count
            )
                
        except Exception as e:
            self.spider_log.error(f"❌ 页面处理失败 (Page {page_num}): {e}", exc_info=True)
            
            yield self.report_error(
                stage='list_page',
                error_msg=e,
                crawl_id=page_crawl_id,
                params=current_form_data,
                api_url=self.list_api_url,
                parent_crawl_id=parent_crawl_id,
                reference_id=keyword
            )
    
    def _create_item(self, drug_item, page_num):
        """
        构建 LiaoningDrugItem
        :param drug_item: 请求获取的药品信息 (Dict)
        :param page_num: 采集页码
        """
        item = LiaoningDrugItem()
        
        # 直接使用API返回的字段名（驼峰命名）
        for field_name in item.fields:
            if field_name in ['id', 'collect_time', 'url', 'url_hash', 'page_num']:
                continue  # 跳过需要单独处理的字段
            item[field_name] = drug_item.get(field_name, '')
        
        # 设置URL字段
        item['url'] = f"https://nhsa.drug/{drug_item.get('goodscode', 'unknown')}"
        
        # 设置页码
        item['page_num'] = page_num
        
        # 生成MD5唯一ID和采集时间
        item.generate_md5_id()
        
        return item

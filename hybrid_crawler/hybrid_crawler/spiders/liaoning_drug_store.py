from .base_spiders import BaseRequestSpider
from ..items import HybridCrawlerItem
from ..models.liaoning_drug import LiaoningDrugItem
from ..utils.logger_utils import get_spider_logger
import json
import scrapy
import pandas as pd
import uuid
from scrapy.http import JsonRequest, FormRequest
import os

# 获取脚本所在目录的绝对路径
script_dir = os.path.dirname(os.path.abspath(__file__))
# 构建Excel文件的绝对路径
excel_path = os.path.join(script_dir, "../../关键字采集(2).xlsx")

class LiaoningDrugSpider(BaseRequestSpider):
    """
    辽宁药店数据爬虫
    目标: 爬取辽宁医保局药店信息
    API: http://ggzy.ln.gov.cn/yphc/gzcx/
    """
    name = "liaoning_drug_store"
    
    # 药品列表API URL
    list_api_url = "https://ggzy.ln.gov.cn/medical" # 去除了原代码中的空格

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spider_log = get_spider_logger(self.name)
        self.crawl_id = str(uuid.uuid4())
        
        # 加载关键词
        try:
            df_name = pd.read_excel(excel_path)
            self.product_list = df_name.loc[:, "采集关键字"].to_list()
            self.spider_log.info(f"🚀 爬虫初始化完成，crawl_id: {self.crawl_id}，加载关键词: {len(self.product_list)} 个")
        except Exception as e:
            self.spider_log.error(f"❌ 关键词文件加载失败: {e}")
            self.product_list = []

    custom_settings = {
        'CONCURRENT_REQUESTS': 8,
        'DOWNLOAD_DELAY': 3,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'ITEM_PIPELINES': {
            'hybrid_crawler.pipelines.DataCleaningPipeline': 300,        # 清洗
            'hybrid_crawler.pipelines.CrawlStatusPipeline': 350,         # 状态监控 (新增)
            'hybrid_crawler.pipelines.LiaoningDrugPipeline': 400,        # 入库
        }
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
            
            # 上报开始采集状态
            # yield {
            #     '_status_': True,
            #     'crawl_id': self.crawl_id,
            #     'stage': 'start_requests',
            #     'page_no': 1,
            #     'params': form_data,
            #     'api_url': self.list_api_url,
            #     'reference_id': product,
            #     'success': True
            # }
            
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

            # 上报页面采集状态
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': current_page,
                'total_pages': total_pages,
                'items_found': len(rows),
                'params': current_form_data,
                'api_url': self.list_api_url,
                'reference_id': keyword,
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }

            item_count = 0
            # 1. 处理当前页的每一条药品数据
            for drug_item in rows:
                yield self._create_item(drug_item, current_page)
                item_count += 1

            # 更新页面采集状态
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': current_page,
                'total_pages': total_pages,
                'items_found': len(rows),
                'items_stored': item_count,
                'params': current_form_data,
                'api_url': self.list_api_url,
                'reference_id': keyword,
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }

            # 2. 生成剩余页码请求 (从第2页开始)
            if current_page < total_pages:
                self.spider_log.info(f"🔄 准备采集关键词 [{keyword}] 后续页面 (2-{total_pages})")
                
                # 批量生成后续请求（如果页数非常多，可能需要优化为递归模式，但目前逻辑沿用原意）
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
            
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': 1,
                'params': current_form_data,
                'api_url': self.list_api_url,
                'reference_id': keyword,
                'success': False,
                'error_message': str(e),
                'parent_crawl_id': parent_crawl_id
            }

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
            
            # 上报页面采集状态
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': page_num,
                'total_pages': total_pages,
                'items_found': len(rows),
                'params': current_form_data,
                'api_url': self.list_api_url,
                'reference_id': keyword,
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }
            
            item_count = 0
            for item in rows:
                yield self._create_item(item, page_num)
                item_count += 1
            
            # 更新页面采集状态
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': page_num,
                'total_pages': total_pages,
                'items_found': len(rows),
                'items_stored': item_count,
                'params': current_form_data,
                'api_url': self.list_api_url,
                'reference_id': keyword,
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }
                
        except Exception as e:
            self.spider_log.error(f"❌ 页面处理失败 (Page {page_num}): {e}", exc_info=True)
            
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': page_num,
                'params': current_form_data,
                'api_url': self.list_api_url,
                'reference_id': keyword,
                'success': False,
                'error_message': str(e),
                'parent_crawl_id': parent_crawl_id
            }
    
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
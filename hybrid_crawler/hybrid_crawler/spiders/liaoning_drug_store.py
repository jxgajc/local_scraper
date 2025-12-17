from .base_spiders import BaseRequestSpider
from ..items import HybridCrawlerItem
from ..models.liaoning_drug import LiaoningDrugItem
import json
import scrapy
import pandas as pd
from scrapy.http import JsonRequest, FormRequest  # 记得导入这个
# 跑一天
# http://ggzy.ln.gov.cn/yphc/gzcx/
import os
# 获取脚本所在目录的绝对路径
script_dir = os.path.dirname(os.path.abspath(__file__))
# 构建Excel文件的绝对路径
excel_path = os.path.join(script_dir, "../../关键字采集(2).xlsx")
# 读取Excel文件
df_name = pd.read_excel(excel_path)
product_list = df_name.loc[:, "采集关键字"].to_list()

class LiaoningDrugSpider(BaseRequestSpider):
    """
    辽宁药店数据爬虫
    目标: 爬取辽宁医保局药店信息
    """
    name = "liaoning_drug_store"
    # 药品列表API URL
    list_api_url = " https://ggzy.ln.gov.cn/medical"

    custom_settings = {
        'CONCURRENT_REQUESTS': 8,
        'DOWNLOAD_DELAY': 3,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        # 使用专门的国家医保药品数据管道
        'ITEM_PIPELINES': {
            'hybrid_crawler.pipelines.DataCleaningPipeline': 300,        # 清洗
            'hybrid_crawler.pipelines.LiaoningDrugPipeline': 400,           # 入库
        }
    }

    def start_requests(self):
        """构造初始的POST请求，使用application/x-www-form-urlencoded格式"""
        # 遍历product_list中的每个关键词，为每个关键词生成初始请求
        for product in product_list:
            # 构造初始表单数据
            form_data = {
                # "druglistCode":"",
                "apiName":"GetYPYYCG",
                "product": product,
                "company":"",
                "pageNum":1
                }
            form_data_str = {k: str(v) for k, v in form_data.items()}
            # 发起第一页请求
            yield FormRequest(
                url=self.list_api_url,
                method='POST',
                formdata=form_data_str,
                callback=self.parse_logic,
                meta={'form_data': form_data}, # 传递 form_data 以便后续翻页使用
                dont_filter=True
            )
    def parse_logic(self, response):
        """处理药品列表响应：处理当前页数据 + 生成后续页码请求"""
        try:
            res_json = json.loads(response.text)
            
            # 获取数据和分页信息
            rows = res_json.get("data",{}).get("data", [])
            total = res_json.get("data",{}).get("totalPage", 0)
            page = response.meta['form_data'].get("pageNum", 1)
            records = res_json.get("data",{}).get("totalData", 0)
            
            self.logger.info(f"当前页: {page}, 每页记录数: {len(rows)}, 总记录数: {records}, 总页数: {total}")

            # 1. 处理当前页的每一条药品数据
            for drug_item in rows:
                yield self._create_item(drug_item, page)

            # 2. 生成剩余页码请求 (从第2页开始)
            # 只有在处理第1页时才生成所有后续页码请求，避免重复请求
            current_form_data = response.meta['form_data']
            if page == 1 and page < total:
                for next_page in range(page + 1, total + 1):
                    next_form_data = current_form_data.copy()
                    next_form_data['pageNum'] = str(next_page)
                    next_form_data_str = {k: str(v) for k, v in next_form_data.items()}
                    yield FormRequest(
                        url=self.list_api_url,
                        method='POST',
                        formdata=next_form_data_str,
                        callback=self.parse_logic,
                        meta={'form_data': next_form_data}, # 传递 form_data 以便后续翻页使用
                        dont_filter=True
                    )

        except Exception as e:
            self.logger.error(f"列表页解析失败: {e} | Response: {response.text[:200]}")
    
    def parse_page(self, response):
        """处理每个页码的响应"""
        try:
            returned_data = json.loads(response.text)
            ret_data = returned_data["data"]["data"]
            
            for item in ret_data:
                yield self._create_item(item)
                
            self.logger.info(f"成功处理页面: {response.url}")
            
        except Exception as e:
            self.logger.error(f"页面处理失败: {e} | URL: {response.url}")
    
    def _create_item(self, drug_item, page_num):
        """
        构建 LiaoningDrugItem
        :param drug_item: 请求获取的药品信息 (Dict)
        :param page_num: 采集页码
        """
        item = LiaoningDrugItem()
        
        # 直接使用API返回的字段名（驼峰命名）
        for field_name in item.fields:
            if field_name in ['id', 'collect_time', 'url', 'url_hash']:
                continue  # 跳过需要单独处理的字段
            item[field_name] = drug_item.get(field_name, '')
        
        # 设置URL字段
        item['url'] = f"https://nhsa.drug/{drug_item.get('goodscode', '')}"
        
        # 设置页码
        item['page_num'] = page_num
        
        # 生成MD5唯一ID和采集时间
        item.generate_md5_id()
        
        return item
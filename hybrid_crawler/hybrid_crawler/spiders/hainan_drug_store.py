from .base_spiders import BaseRequestSpider
from ..items import HybridCrawlerItem
import json
import scrapy

class HainanDrugStoreSpider(BaseRequestSpider):
    """
    海南药店数据爬虫
    目标: 爬取海南医保局药店信息
    """
    name = "hainan_drug_store"
    start_urls = ["https://ybj.hainan.gov.cn/tps-local/local/web/std/drugStore/getDrugStore?current=1&size=50"]
    
    custom_settings = {
        'CONCURRENT_REQUESTS': 16,
        'DOWNLOAD_DELAY': 3,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    }
    
    def parse_logic(self, response):
        """处理初始请求，获取总页数并生成所有页码请求"""
        try:
            returned_data = json.loads(response.text)
            pages = int(returned_data["data"]["pages"])
            self.logger.info(f"总页数: {pages}")
            
            # 获取第一页数据
            ret_data = returned_data["data"]["records"]
            for item in ret_data:
                yield self._create_item(item)
            
            # 生成剩余页码请求
            for page in range(2, pages + 1):
                url = f'https://ybj.hainan.gov.cn/tps-local/local/web/std/drugStore/getDrugStore?current={page}&size=50'
                meta = {'request_type': 'http'}
                yield scrapy.Request(url, meta=meta, callback=self.parse_page)
                
        except Exception as e:
            self.logger.error(f"初始请求处理失败: {e}")
    
    def parse_page(self, response):
        """处理每个页码的响应"""
        try:
            returned_data = json.loads(response.text)
            ret_data = returned_data["data"]["records"]
            
            for item in ret_data:
                yield self._create_item(item)
                
            self.logger.info(f"成功处理页面: {response.url}")
            
        except Exception as e:
            self.logger.error(f"页面处理失败: {e} | URL: {response.url}")
    
    def _create_item(self, data):
        """根据数据创建Item"""
        # 使用HybridCrawlerItem存储数据
        item = HybridCrawlerItem()
        
        # 为每个药品生成唯一的URL，使用prodCode作为唯一标识
        prod_code = data.get("prodCode", "")
        unique_url = f"https://ybj.hainan.gov.cn/tps-local/local/web/std/drugStore/getDrugStore?prodCode={prod_code}"
        
        item["url"] = unique_url
        item["title"] = data.get("prodName", "")
        item["content"] = str(data)
        item["status_code"] = 200
        item["request_type"] = "http"
        item["source"] = "hainan_drug_store"
        item["meta_info"] = data
        
        return item

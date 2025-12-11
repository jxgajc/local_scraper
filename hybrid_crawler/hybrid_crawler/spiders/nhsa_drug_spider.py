from .base_spiders import BaseRequestSpider
from ..models.nhsa_drug import NhsaDrugItem
import json
import scrapy
import time

class NhsaDrugSpider(BaseRequestSpider):
    """
    国家医保药品数据爬虫
    目标: 采集国家医保药品数据API，获取药品信息
    """
    name = "nhsa_drug_spider"
    
    # 药品列表API URL
    list_api_url = "https://code.nhsa.gov.cn/yp/getPublishGoodsDataInfo.html" 

    custom_settings = {
        'CONCURRENT_REQUESTS': 1, # 降低并发数，避免触发反爬
        'DOWNLOAD_DELAY': 15, # 增加延迟，降低请求频率
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh-Hans;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': 'JSESSIONID=1F42D848E37CC3D56CA2B0BE3CCA994D;acw_tc=1a0c65ca17650958291751175eb31be38dda41a39d53d696605dc27af75da9',
            'Origin': 'https://code.nhsa.gov.cn',
            'Priority': 'u=3, i',
            'Referer': 'https://code.nhsa.gov.cn/yp/toPublishGoodsData.html?batchNumber=20251201',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15',
            'X-Requested-With': 'XMLHttpRequest',
        },
        # 使用专门的国家医保药品数据管道
        'ITEM_PIPELINES': {
            'hybrid_crawler.pipelines.DataCleaningPipeline': 300,        # 清洗
            'hybrid_crawler.pipelines.NhsaDrugPipeline': 400,           # 入库
        }
    }

    def start_requests(self):
        """构造初始的POST请求，使用application/x-www-form-urlencoded格式"""
        # 构造初始表单数据
        form_data = {
            'goodsCode': '',
            'companyNameSc': '',
            'registeredProductName': '',
            'approvalCode': '',
            'batchNumber': '20251201',
            '_search': 'false',
            'nd': str(int(time.time() * 1000)),
            'rows': '1000',
            'page': '1',
            'sidx': '',
            'sord': 'asc'
        }
        
        # 发起第一页请求
        yield scrapy.FormRequest(
            url=self.list_api_url,
            method='POST',
            formdata=form_data,
            callback=self.parse_logic,
            meta={'form_data': form_data}, # 传递 form_data 以便后续翻页使用
            dont_filter=True
        )

    def parse_logic(self, response):
        """处理药品列表响应：处理当前页数据 + 生成后续页码请求"""
        try:
            res_json = json.loads(response.text)
            
            # 获取数据和分页信息
            rows = res_json.get("rows", [])
            total = res_json.get("total", 0)
            page = res_json.get("page", 1)
            records = res_json.get("records", 0)
            
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
                    next_form_data['page'] = str(next_page)
                    
                    yield scrapy.FormRequest(
                        url=self.list_api_url,
                        method='POST',
                        formdata=next_form_data,
                        callback=self.parse_logic,
                        meta={'form_data': next_form_data},
                        dont_filter=True
                    )

        except Exception as e:
            self.logger.error(f"列表页解析失败: {e} | Response: {response.text[:200]}")

    def _create_item(self, drug_item, page_num):
        """
        构建 NhsaDrugItem
        :param drug_item: 请求获取的药品信息 (Dict)
        :param page_num: 采集页码
        """
        item = NhsaDrugItem()
        
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
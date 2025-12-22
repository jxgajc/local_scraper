from .base_spiders import BaseRequestSpider
from ..models.nhsa_drug import NhsaDrugItem
from ..utils.logger_utils import get_spider_logger
import json
import scrapy
import time
import uuid

class NhsaDrugSpider(BaseRequestSpider):
    """
    国家医保药品数据爬虫
    目标: 采集国家医保药品数据API，获取药品信息
    Target: https://code.nhsa.gov.cn
    """
    name = "nhsa_drug_spider"
    
    # 药品列表API URL
    list_api_url = "https://code.nhsa.gov.cn/yp/getPublishGoodsDataInfo.html" 
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = get_spider_logger(self.name)
        self.crawl_id = str(uuid.uuid4())
        self.logger.info(f"🚀 爬虫初始化完成，crawl_id: {self.crawl_id}")

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
            'hybrid_crawler.pipelines.CrawlStatusPipeline': 350,         # 状态监控 (新增)
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
        
        self.logger.info(f"📋 开始采集国家医保药品数据，Batch: {form_data['batchNumber']}")
        
        # 上报开始采集状态
        yield {
            '_status_': True,
            'crawl_id': self.crawl_id,
            'stage': 'start_requests',
            'page_no': 1,
            'params': form_data,
            'api_url': self.list_api_url,
            'success': True
        }
        
        # 发起第一页请求
        yield scrapy.FormRequest(
            url=self.list_api_url,
            method='POST',
            formdata=form_data,
            callback=self.parse_logic,
            meta={'form_data': form_data, 'crawl_id': self.crawl_id}, # 传递 form_data 以便后续翻页使用
            dont_filter=True
        )

    def parse_logic(self, response):
        """处理药品列表响应：处理第一页数据 + 生成后续页码请求"""
        page_crawl_id = str(uuid.uuid4())
        parent_crawl_id = response.meta['crawl_id']
        current_form_data = response.meta['form_data']
        
        try:
            res_json = json.loads(response.text)
            
            # 获取数据和分页信息
            rows = res_json.get("rows", [])
            total_pages = int(res_json.get("total", 0))
            current_page = int(res_json.get("page", 1))
            total_records = int(res_json.get("records", 0))
            
            self.logger.info(f"📄 列表页面 [{current_page}/{total_pages}] - 发现 {len(rows)} 条记录 (总计: {total_records})")

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
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }

            # 2. 生成剩余页码请求 (从第2页开始)
            # 只有在处理第1页时才生成所有后续页码请求
            if current_page == 1 and current_page < total_pages:
                self.logger.info(f"🔄 准备采集后续页面 (2-{total_pages})")
                
                for next_page in range(current_page + 1, total_pages + 1):
                    next_form_data = current_form_data.copy()
                    next_form_data['page'] = str(next_page)
                    next_form_data['nd'] = str(int(time.time() * 1000))
                    
                    yield scrapy.FormRequest(
                        url=self.list_api_url,
                        method='POST',
                        formdata=next_form_data,
                        callback=self.parse_list_page, # 使用独立回调处理后续页面
                        meta={
                            'form_data': next_form_data, 
                            'parent_crawl_id': parent_crawl_id,
                            'page_num': next_page
                        },
                        dont_filter=True
                    )

        except Exception as e:
            self.logger.error(f"❌ 列表页解析失败 (Page 1): {e}", exc_info=True)
            
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': 1,
                'params': current_form_data,
                'api_url': self.list_api_url,
                'success': False,
                'error_message': str(e),
                'parent_crawl_id': parent_crawl_id
            }

    def parse_list_page(self, response):
        """处理后续页面的响应"""
        page_crawl_id = str(uuid.uuid4())
        parent_crawl_id = response.meta['parent_crawl_id']
        current_form_data = response.meta['form_data']
        page_num = response.meta['page_num']

        try:
            res_json = json.loads(response.text)
            
            rows = res_json.get("rows", [])
            total_pages = int(res_json.get("total", 0))
            
            self.logger.info(f"📄 列表页面 [{page_num}/{total_pages}] - 发现 {len(rows)} 条记录")
            
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
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }
            
            item_count = 0
            for drug_item in rows:
                yield self._create_item(drug_item, page_num)
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
                'success': True,
                'parent_crawl_id': parent_crawl_id
            }

        except Exception as e:
            self.logger.error(f"❌ 页面处理失败 (Page {page_num}): {e}", exc_info=True)
            
            yield {
                '_status_': True,
                'crawl_id': page_crawl_id,
                'stage': 'list_page',
                'page_no': page_num,
                'params': current_form_data,
                'api_url': self.list_api_url,
                'success': False,
                'error_message': str(e),
                'parent_crawl_id': parent_crawl_id
            }

    def _create_item(self, drug_item, page_num):
        """
        构建 NhsaDrugItem
        :param drug_item: 请求获取的药品信息 (Dict)
        :param page_num: 采集页码
        """
        item = NhsaDrugItem()
        
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
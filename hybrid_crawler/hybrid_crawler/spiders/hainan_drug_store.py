import scrapy
import json
import uuid
from urllib.parse import urlencode
from ..models.hainan_drug import HainanDrugItem
from ..utils.logger_utils import get_spider_logger
import pandas as pd
import os
from .mixins import SpiderStatusMixin

# 获取脚本所在目录的绝对路径
script_dir = os.path.dirname(os.path.abspath(__file__))
# 构建Excel文件的绝对路径
excel_path = os.path.join(script_dir, "../../关键字采集(2).xlsx")

class HainanDrugSpider(SpiderStatusMixin, scrapy.Spider):
    """
    海南省医保服务平台 - 药品门店查询爬虫
    Target: https://ybj.hainan.gov.cn
    """
    name = "hainan_drug_spider"
    
    # API Endpoints
    list_api_base = "https://ybj.hainan.gov.cn/tps-local/local/web/std/drugStore/getDrugStore"
    detail_api_base = "https://ybj.hainan.gov.cn/tps-local/local/web/std/drugStore/getDrugStoreDetl"
    
    def __init__(self, recrawl_ids=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spider_log = get_spider_logger(self.name)
        self.crawl_id = str(uuid.uuid4())

        # 补采模式：只采集指定的 drug_code
        self.recrawl_ids = set(recrawl_ids.split(',')) if recrawl_ids else None
        self.recrawl_mode = self.recrawl_ids is not None

        # 加载关键词
        try:
            df_name = pd.read_excel(excel_path)
            self.keywords = df_name.loc[:, "采集关键字"].to_list()
            mode_str = f"补采模式，目标 {len(self.recrawl_ids)} 条" if self.recrawl_mode else "全量采集"
            self.spider_log.info(f"🚀 爬虫初始化完成，crawl_id: {self.crawl_id}，模式: {mode_str}，加载关键词: {len(self.keywords)} 个")
        except Exception as e:
            self.spider_log.error(f"❌ 关键词文件加载失败: {e}")
            self.keywords = []

    custom_settings = {
        'CONCURRENT_REQUESTS': 5,
        'DOWNLOAD_DELAY': 3,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Referer': 'https://ybj.hainan.gov.cn/tps-local/b/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            # 'User-Agent': Handled by RandomUserAgentMiddleware
            'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"'
        },
        # Pipeline 配置已移至全局 settings.py
    }

    def start_requests(self):
        """遍历关键词，发起列表请求"""
        self.spider_log.info(f"📋 开始采集，共 {len(self.keywords)} 个关键词")
        
        for keyword in self.keywords:
            params = {
                'current': 1,
                'size': 500,
                'prodName': keyword
            }
            url = f"{self.list_api_base}?{urlencode(params)}"
            
            self.spider_log.info(f"🔍 正在采集关键词: {keyword}")

            yield scrapy.Request(
                url=url,
                callback=self.parse_list,
                meta={
                    'keyword': keyword, 
                    'current_page': 1, 
                    'page_size': 500, 
                    'crawl_id': self.crawl_id
                }
            )

    def parse_list(self, response):
        """解析药品列表并处理翻页"""
        page_crawl_id = str(uuid.uuid4())
        keyword = response.meta['keyword']
        current_page = response.meta['current_page']
        parent_crawl_id = response.meta['crawl_id']
        
        try:
            res_json = json.loads(response.text)
            if res_json.get("code") != 0:
                error_msg = res_json.get('msg', 'Unknown Error')
                self.spider_log.error(f"❌ 关键词 [{keyword}] 列表API错误 (Page {current_page}): {error_msg}")
                
                yield self.report_error(
                    stage='list_page',
                    error_msg=error_msg,
                    crawl_id=page_crawl_id,
                    params={'prodName': keyword, 'current': current_page},
                    api_url=self.list_api_base,
                    parent_crawl_id=parent_crawl_id
                )
                return

            data = res_json.get("data", {})
            records = data.get("records", [])
            total_pages = data.get("pages", 0)
            page_size = data.get("size", 500)

            self.spider_log.info(f"📄 关键词 [{keyword}] 列表页面 [{current_page}/{total_pages}] - 发现 {len(records)} 条药品记录")
            
            yield self.report_list_page(
                crawl_id=page_crawl_id,
                page_no=current_page,
                total_pages=total_pages,
                items_found=len(records),
                params={'prodName': keyword, 'current': current_page},
                api_url=self.list_api_base,
                parent_crawl_id=parent_crawl_id,
                page_size=page_size
            )

            item_count = 0
            for record in records:
                drug_code = record.get('prodCode')
                # 补采模式：跳过不在目标列表中的记录
                if self.recrawl_mode:
                    if drug_code not in self.recrawl_ids:
                        continue
                    self.recrawl_ids.discard(drug_code)  # 已处理，从列表移除

                # 1. 提取药品基础信息
                base_info = {
                    'drug_code': record.get('prodCode'),
                    'prod_name': record.get('prodName'),
                    'dosform': record.get('dosform'),
                    'spec': record.get('prodSpec'),
                    'pac': record.get('prodPac'),
                    'conv_rat': record.get('convrat'),
                    'prod_entp': record.get('prodentpName'),
                    'dcla_entp': record.get('dclaEntpName'),
                    'aprv_no': record.get('aprvno'),
                    'source_data': json.dumps(record, ensure_ascii=False)
                }

                # 2. 如果有药品编码，查询门店详情
                drug_code = record.get('prodCode')
                if drug_code:
                    detail_params = {
                        'current': 1,
                        'size': 20,
                        'drugCode': drug_code
                    }
                    detail_url = f"{self.detail_api_base}?{urlencode(detail_params)}"
                    
                    yield scrapy.Request(
                        url=detail_url,
                        callback=self.parse_detail,
                        meta={
                            'base_info': base_info,
                            'current_page': 1,
                            'page_size': 20,
                            'drug_code': drug_code,
                            'parent_crawl_id': page_crawl_id
                        }
                    )
                    item_count += 1
                else:
                    # 无编码，直接保存
                    item = HainanDrugItem()
                    item.update(base_info)
                    item['has_shop_record'] = False
                    item.generate_md5_id()
                    yield item
                    item_count += 1

            # 更新页面采集状态，记录成功存储的条数（包含触发的子请求）
            yield self.report_list_page(
                crawl_id=page_crawl_id,
                page_no=current_page,
                total_pages=total_pages,
                items_found=len(records),
                params={'prodName': keyword, 'current': current_page},
                api_url=self.list_api_base,
                parent_crawl_id=parent_crawl_id,
                page_size=page_size,
                items_stored=item_count
            )

            # 3. 列表页翻页
            if current_page < total_pages:
                # 补采模式：如果所有目标都已采集完成，提前结束
                if self.recrawl_mode and not self.recrawl_ids:
                    self.spider_log.info(f"✅ 补采模式：所有目标数据已采集完成")
                    return

                self.spider_log.info(f"🔄 准备采集关键词 [{keyword}] 下一页列表 [{current_page + 1}/{total_pages}]")
                next_page = current_page + 1
                params = {
                    'current': next_page,
                    'size': page_size,
                    'prodName': keyword
                }
                url = f"{self.list_api_base}?{urlencode(params)}"
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_list,
                    meta={
                        'keyword': keyword,
                        'current_page': next_page,
                        'page_size': page_size,
                        'crawl_id': parent_crawl_id
                    }
                )
            else:
                self.spider_log.info(f"✅ 关键词 [{keyword}] 列表采集完成，共 {total_pages} 页")

        except Exception as e:
            self.spider_log.error(f"❌ 解析关键词 [{keyword}] 列表失败 (Page {current_page}): {e}", exc_info=True)
            
            yield self.report_error(
                stage='list_page',
                error_msg=e,
                crawl_id=page_crawl_id,
                params={'prodName': keyword, 'current': current_page},
                api_url=self.list_api_base,
                parent_crawl_id=parent_crawl_id
            )

    def parse_detail(self, response):
        """解析门店/医院详情并处理翻页"""
        base_info = response.meta['base_info']
        current_page = response.meta['current_page']
        drug_code = response.meta['drug_code']
        parent_crawl_id = response.meta['parent_crawl_id']
        prod_name = base_info.get('prod_name', 'Unknown')
        detail_crawl_id = str(uuid.uuid4())

        try:
            res_json = json.loads(response.text)
            if res_json.get("code") != 0:
                error_msg = res_json.get('msg', 'Unknown Error')
                self.spider_log.warning(f"⚠️ 药品 [{prod_name}] 详情API错误 (Page {current_page}): {error_msg}")
                
                yield self.report_error(
                    stage='detail_page',
                    error_msg=error_msg,
                    crawl_id=detail_crawl_id,
                    params={'drugCode': drug_code, 'current': current_page},
                    api_url=self.detail_api_base,
                    parent_crawl_id=parent_crawl_id,
                    reference_id=drug_code
                )
                return

            data = res_json.get("data", {})
            records = data.get("records", [])
            total_pages = data.get("pages", 0)
            page_size = data.get("size", 20)

            self.spider_log.info(f"🏥 药品 [{prod_name}] 详情页面 [{current_page}/{total_pages}] - 发现 {len(records)} 条门店记录")
            
            # 优化：移除冗余状态上报

            item_count = 0
            if records:
                for shop in records:
                    item = HainanDrugItem()
                    item.update(base_info)
                    
                    # 注入门店信息
                    item['has_shop_record'] = True
                    item['shop_name'] = shop.get('medinsName')
                    item['shop_code'] = shop.get('medinsCode')
                    item['shop_type_memo'] = shop.get('memo')
                    item['price'] = shop.get('pric')
                    item['inventory'] = shop.get('invCnt')
                    item['update_time'] = shop.get('invChgTime')
                    item['hilist_name'] = shop.get('fixmedinsHilistName')
                    
                    # 更新 source_data 包含两部分信息
                    full_source = {
                        "drug_info": json.loads(base_info['source_data']),
                        "shop_info": shop
                    }
                    item['source_data'] = json.dumps(full_source, ensure_ascii=False)
                    
                    item.generate_md5_id()
                    yield item
                    item_count += 1
                
                # 详情页翻页
                if current_page < total_pages:
                    self.spider_log.info(f"🔄 准备采集药品 [{prod_name}] 下一页详情 [{current_page + 1}/{total_pages}]")
                    next_page = current_page + 1
                    params = {
                        'current': next_page,
                        'size': page_size,
                        'drugCode': drug_code
                    }
                    url = f"{self.detail_api_base}?{urlencode(params)}"
                    
                    yield scrapy.Request(
                        url=url,
                        callback=self.parse_detail,
                        meta={
                            'base_info': base_info,
                            'current_page': next_page,
                            'page_size': page_size,
                            'drug_code': drug_code,
                            'parent_crawl_id': parent_crawl_id
                        }
                    )
            elif current_page == 1:
                # 第一页就没数据，说明该药没库存记录，保存一条基础信息
                self.spider_log.info(f"📋 药品 [{prod_name}] 没有门店记录")
                
                item = HainanDrugItem()
                item.update(base_info)
                item['has_shop_record'] = False
                item.generate_md5_id()
                yield item
                item_count += 1

            # 更新页面采集状态，记录成功存储的条数
            yield self.report_detail_page(
                crawl_id=detail_crawl_id,
                page_no=current_page,
                items_found=len(records),
                params={'drugCode': drug_code, 'current': current_page},
                api_url=self.detail_api_base,
                parent_crawl_id=parent_crawl_id,
                reference_id=drug_code,
                total_pages=total_pages,
                items_stored=item_count
            )

        except Exception as e:
            self.spider_log.error(f"❌ 解析药品 [{prod_name}] 详情失败 (Page {current_page}): {e}", exc_info=True)
            
            yield self.report_error(
                stage='detail_page',
                error_msg=e,
                crawl_id=detail_crawl_id,
                params={'drugCode': drug_code, 'current': current_page},
                api_url=self.detail_api_base,
                parent_crawl_id=parent_crawl_id,
                reference_id=drug_code
            )

import os
import sys
import threading

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

# 设置 Scrapy 配置文件路径
os.environ['SCRAPY_SETTINGS_MODULE'] = 'hybrid_crawler.settings'

# 导入爬虫 (需确保已安装 Twisted Asyncio Reactor)
from hybrid_crawler.spiders.example import HackerNewsSpider, DynamicQuotesSpider

from hybrid_crawler.spiders.fujian_drug_store import FujianDrugSpider
from hybrid_crawler.spiders.hainan_drug_store import HainanDrugSpider
from hybrid_crawler.spiders.hebei_drug_store import HebeiDrugSpider
from hybrid_crawler.spiders.liaoning_drug_store import LiaoningDrugSpider
from hybrid_crawler.spiders.ningxia_drug_store import NingxiaDrugSpider
from hybrid_crawler.spiders.nhsa_drug_spider import NhsaDrugSpider
from hybrid_crawler.spiders.shandong_drug_store import ShandongDrugSpider
from hybrid_crawler.spiders.guangdong_drug_store import GuangdongDrugSpider
from hybrid_crawler.spiders.tianjin_drug_store import TianjinDrugSpider
# 爬虫映射表
SPIDER_MAP = {
    # 'hn_simple': HackerNewsSpider,
    # 'quotes_dynamic': DynamicQuotesSpider,

    'fujian_drug_store': FujianDrugSpider,
    'hainan_drug_store': HainanDrugSpider,
    'hebei_drug_store': HebeiDrugSpider,
    'liaoning_drug_store': LiaoningDrugSpider,
    'ningxia_drug_store': NingxiaDrugSpider,
    # 'shandong_drug_store': ShandongDrugSpider,
    'guangdong_drug_spider': GuangdongDrugSpider,
    'tianjin_drug_spider': TianjinDrugSpider,
    # 'nhsa_drug_spider': NhsaDrugSpider,
}

def run_spider(spider_cls, spider_name, is_debug):
    """在单个线程中运行指定的爬虫"""
    settings = get_project_settings()
    
    if is_debug:
        settings.set('LOG_LEVEL', 'DEBUG')
    
    # 为每个爬虫设置单独的日志文件
    settings.set('LOG_FILE', os.path.join(os.getcwd(), 'log', f'{spider_name}.log'))
    
    process = CrawlerProcess(settings)
    process.crawl(spider_cls)
    process.start(stop_after_crawl=True)


def run():
    print(">>> 正在启动混合爬虫系统...")
    
    # 简单的参数解析，用于开启 Debug 模式和指定爬虫
    is_debug = 'debug' in sys.argv
    
    # 获取要运行的爬虫名称
    spider_name = None
    for arg in sys.argv[1:]:
        if arg != 'debug' and arg in SPIDER_MAP:
            spider_name = arg
            break
    
    if is_debug:
        print(">>> 🐞 Debug 模式已开启: 日志级别 DEBUG")
    
    if spider_name:
        # 运行指定的爬虫
        print(f">>> 正在运行爬虫: {spider_name}")
        run_spider(SPIDER_MAP[spider_name], spider_name, is_debug)
    else:
        # 并行运行所有爬虫
        print(">>> 正在并行运行所有爬虫")
        threads = []
        
        for name, spider_cls in SPIDER_MAP.items():
            print(f">>> 启动爬虫线程: {name}")
            thread = threading.Thread(target=run_spider, args=(spider_cls, name, is_debug))
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
    
    print(">>> 所有爬虫运行完成")

if __name__ == '__main__':
    run()
import os
import sys

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

# 设置 Scrapy 配置文件路径
os.environ['SCRAPY_SETTINGS_MODULE'] = 'hybrid_crawler.settings'

# 导入爬虫 (需确保已安装 Twisted Asyncio Reactor)
from hybrid_crawler.spiders.example import HackerNewsSpider, DynamicQuotesSpider
from hybrid_crawler.spiders.hainan_drug_store import HainanDrugStoreSpider
from hybrid_crawler.spiders.nhsa_drug_spider import NhsaDrugSpider
# 爬虫映射表
SPIDER_MAP = {
    # 'hn_simple': HackerNewsSpider,
    # 'quotes_dynamic': DynamicQuotesSpider,
    # 'hainan_drug_store': HainanDrugStoreSpider,
    # 'hainan_drug_store': HainanDrugStoreSpider,
    'nhsa_drug_spider': NhsaDrugSpider,
}

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
    
    settings = get_project_settings()
    
    if is_debug:
        print(">>> 🐞 Debug 模式已开启: 日志级别 DEBUG")
        settings.set('LOG_LEVEL', 'DEBUG')
    
    process = CrawlerProcess(settings)
    
    if spider_name:
        # 运行指定的爬虫
        print(f">>> 正在运行爬虫: {spider_name}")
        process.crawl(SPIDER_MAP[spider_name])
    else:
        # 默认运行所有爬虫
        print(">>> 正在运行所有爬虫")
        for spider in SPIDER_MAP.values():
            process.crawl(spider)
    
    process.start()

if __name__ == '__main__':
    run()
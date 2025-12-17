import os
import sys

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
    'guangdong_drug_spider': GuangdongDrugSpider,
    'tianjin_drug_spider': TianjinDrugSpider,
    # 'nhsa_drug_spider': NhsaDrugSpider,

    # 'shandong_drug_store': ShandongDrugSpider,    
}

def run_spider(spider_cls, spider_name, is_debug):
    """在单个线程中运行指定的爬虫"""
    settings = get_project_settings()
    
    if is_debug:
        settings.set('LOG_LEVEL', 'DEBUG')
    
    # 获取脚本所在目录的绝对路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 为每个爬虫设置单独的日志文件路径
    log_dir = os.path.join(script_dir, 'log')
    log_file = os.path.join(log_dir, f'{spider_name}.log')
    
    # 确保日志目录存在
    os.makedirs(log_dir, exist_ok=True)
    
    settings.set('LOG_FILE', log_file)
    
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
    
    # 获取项目设置
    settings = get_project_settings()
    
    if is_debug:
        settings.set('LOG_LEVEL', 'DEBUG')
    
    # 获取脚本所在目录的绝对路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(script_dir, 'log')
    os.makedirs(log_dir, exist_ok=True)
    
    # 创建单个 CrawlerProcess 实例
    process = CrawlerProcess(settings)
    
    if spider_name:
        # 运行指定的爬虫
        print(f">>> 正在运行爬虫: {spider_name}")
        spider_cls = SPIDER_MAP[spider_name]
        # 为单个爬虫设置日志文件
        settings.set('LOG_FILE', os.path.join(log_dir, f'{spider_name}.log'))
        process.crawl(spider_cls)
    else:
        # 添加所有爬虫到同一个进程
        print(">>> 正在添加所有爬虫到运行队列")
        
        for name, spider_cls in SPIDER_MAP.items():
            print(f">>> 添加爬虫: {name}")
            # 为每个爬虫设置独立的日志文件
            # 注意: 当运行多个爬虫时，日志会合并到一个文件
            # 如果需要分离日志，需要更复杂的配置
            process.crawl(spider_cls)
    
    # 启动进程，所有爬虫将同时运行
    print(">>> 正在启动所有爬虫...")
    process.start(stop_after_crawl=True)
    
    print(">>> 所有爬虫运行完成")

if __name__ == '__main__':
    run()
import os
import sys
import time

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
    'nhsa_drug_spider': NhsaDrugSpider,

    # 'shandong_drug_store': ShandongDrugSpider,    
}

def generate_summary_report(spider_stats):
    """生成采集总结报告"""
    summary = """\n========================================
        采集任务完成总结
========================================\n"""
    
    total_items_scraped = 0
    total_requests_made = 0
    total_requests_failed = 0
    
    for spider_name, stats in spider_stats.items():
        items_scraped = stats.get('item_scraped_count', 0)
        requests_made = stats.get('downloader/request_count', 0)
        requests_failed = stats.get('downloader/request_failed_count', 0)
        requests_succeeded = requests_made - requests_failed
        
        # 计算成功率
        success_rate = 0
        if requests_made > 0:
            success_rate = (requests_succeeded / requests_made) * 100
        
        summary += f"\n【{spider_name}】\n"
        summary += f"  采集状态: {'完成' if items_scraped > 0 else '未采集到数据'}\n"
        summary += f"  总请求数: {requests_made}\n"
        summary += f"  成功请求: {requests_succeeded}\n"
        summary += f"  失败请求: {requests_failed}\n"
        summary += f"  请求成功率: {success_rate:.2f}%\n"
        summary += f"  采集数据量: {items_scraped}\n"
        
        # 检查是否有遗漏
        if requests_failed > 0:
            summary += f"  ⚠️  警告: 存在 {requests_failed} 个失败请求，可能存在数据遗漏\n"
        elif requests_made == 0:
            summary += f"  ⚠️  警告: 未发起任何请求，可能爬虫配置有问题\n"
        
        total_items_scraped += items_scraped
        total_requests_made += requests_made
        total_requests_failed += requests_failed
    
    # 总体统计
    total_requests_succeeded = total_requests_made - total_requests_failed
    total_success_rate = 0
    if total_requests_made > 0:
        total_success_rate = (total_requests_succeeded / total_requests_made) * 100
    
    summary += "\n========================================\n"
    summary += f"【总体统计】\n"
    summary += f"  总爬虫数: {len(spider_stats)}\n"
    summary += f"  总请求数: {total_requests_made}\n"
    summary += f"  成功请求: {total_requests_succeeded}\n"
    summary += f"  失败请求: {total_requests_failed}\n"
    summary += f"  总体请求成功率: {total_success_rate:.2f}%\n"
    summary += f"  总采集数据量: {total_items_scraped}\n"
    
    # 总体状态判断
    if total_requests_failed == 0 and total_items_scraped > 0:
        summary += f"  ✅ 总体状态: 采集完成，无数据遗漏\n"
    elif total_requests_failed > 0:
        summary += f"  ⚠️  总体状态: 采集完成，但存在数据遗漏\n"
    else:
        summary += f"  ❌ 总体状态: 采集失败，未采集到任何数据\n"
    
    summary += "========================================\n"
    
    return summary


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
    
    # 创建一个CrawlerProcess来运行所有爬虫
    process = CrawlerProcess(settings)
    
    # 收集爬虫实例和名称映射
    crawlers = []
    
    if spider_name:
        # 运行指定的爬虫
        print(f">>> 正在添加爬虫: {spider_name}")
        crawler_cls = SPIDER_MAP[spider_name]
        crawler = process.create_crawler(crawler_cls)
        process.crawl(crawler)
        crawlers.append((spider_name, crawler))
    else:
        # 为每个爬虫创建独立的进程和日志文件
        print(">>> 正在添加所有爬虫")
        
        for name, spider_cls in SPIDER_MAP.items():
            print(f">>> 正在添加爬虫: {name}")
            crawler = process.create_crawler(spider_cls)
            process.crawl(crawler)
            crawlers.append((name, crawler))
    
    # 启动爬虫，设置stop_after_crawl=True，让爬虫完成后自动停止
    process.start(stop_after_crawl=True)
    
    # 收集爬虫统计信息
    spider_stats = {}
    for name, crawler in crawlers:
        stats = crawler.stats.get_stats()
        spider_stats[name] = stats
    
    # 生成并打印总结报告
    summary_report = generate_summary_report(spider_stats)
    print(summary_report)
    
    # 将总结报告写入日志文件
    summary_log_file = os.path.join(log_dir, 'crawl_summary.log')
    with open(summary_log_file, 'a') as f:
        f.write('\n' + '='*50 + '\n')
        f.write(f'采集总结报告 - {time.strftime("%Y-%m-%d %H:%M:%S")}\n')
        f.write('='*50 + '\n')
        f.write(summary_report)
        f.write('\n\n')
    
    print(">>> 所有爬虫运行完成，总结报告已生成")

if __name__ == '__main__':
    run()
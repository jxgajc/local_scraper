import os

BOT_NAME = 'hybrid_crawler'
SPIDER_MODULES = ['hybrid_crawler.spiders']
NEWSPIDER_MODULE = 'hybrid_crawler.spiders'

# =============================================================================
# 核心并发配置
# =============================================================================
CONCURRENT_REQUESTS = 32
DOWNLOAD_DELAY = 0

# =============================================================================
# 中间件管道配置
# =============================================================================
DOWNLOADER_MIDDLEWARES = {
    'hybrid_crawler.middlewares.StrategyRoutingMiddleware': 100, # 路由策略
    'hybrid_crawler.middlewares.SmartRetryMiddleware': 550,      # 智能重试
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,  # 禁用默认重试
}

ITEM_PIPELINES = {
    'hybrid_crawler.pipelines.DataCleaningPipeline': 300,        # 清洗
    'hybrid_crawler.pipelines.CrawlStatusPipeline': 350,         # 采集状态记录
    'hybrid_crawler.pipelines.AsyncBatchWritePipeline': 400,     # 入库
}

# =============================================================================
# 异步写入缓冲配置
# =============================================================================
BUFFER_THRESHOLD = 500  # 积攒 50 条写入一次
BUFFER_TIMEOUT_SEC = 1.5 # 或最长等待 1.5 秒写入一次

# =============================================================================
# Playwright 专用配置
# =============================================================================
# try:
#     from scrapy_playwright.handler import ScrapyPlaywrightDownloadHandler
#     DOWNLOAD_HANDLERS = {
#         "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
#         "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
#     }
#     # 必须使用 Asyncio 反应堆
#     TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
#     # 💡 Debug: 将 headless 改为 False 可看到浏览器
#     PLAYWRIGHT_LAUNCH_OPTIONS = {
#         'headless': True,
#         'args': ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
#         'timeout': 30000, # 启动超时时间
#     }
# except ImportError:
#     # 如果无法导入 scrapy_playwright，使用默认的下载处理器
#     DOWNLOAD_HANDLERS = {}
#     # 不需要设置 Asyncio 反应堆
#     pass

LOG_LEVEL = 'INFO'
# 日志配置
LOG_ENABLED = True
LOG_ENCODING = 'utf-8'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'

# 日志保存路径
script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
log_dir = os.path.join(script_dir, 'log')
os.makedirs(log_dir, exist_ok=True)

# 禁用 Scrapy 默认的文件日志，使用我们的自定义日志管理器
LOG_FILE = None

# 日志处理器配置
LOG_STDOUT = True

# 为不同模块设置日志级别
LOG_LEVELS = {
    'scrapy': 'WARNING',
    'twisted': 'WARNING',
    'hybrid_crawler': 'INFO',
}


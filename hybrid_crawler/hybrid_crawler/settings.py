import os

BOT_NAME = 'hybrid_crawler'
SPIDER_MODULES = ['hybrid_crawler.spiders']
NEWSPIDER_MODULE = 'hybrid_crawler.spiders'

# =============================================================================
# 数据库配置
# =============================================================================
# 优先从环境变量获取，否则使用默认值
DATABASE_URL = os.getenv('DATABASE_URL', 'mysql+pymysql://xf:xf666@192.168.0.141:3306/spiderweb')
# 将其注入到环境变量中，以便 models 模块（非 Scrapy 上下文）也能获取
os.environ['DATABASE_URL'] = DATABASE_URL

# =============================================================================
# 核心并发配置
# =============================================================================
CONCURRENT_REQUESTS = 32
DOWNLOAD_DELAY = 0

# 【关键优化】增加线程池大小，防止数据库 I/O 耗尽线程导致死锁
# 默认只有 10，对于并发 32 的爬虫来说太小
REACTOR_THREADPOOL_MAXSIZE = 50

# 【关键优化】设置下载超时，防止坏代理或慢响应卡住 Slot
DOWNLOAD_TIMEOUT = 15

# =============================================================================
# 自动限速配置 (AutoThrottle)
# =============================================================================
# 启用自动限速，根据负载动态调整延迟
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 0.5
AUTOTHROTTLE_MAX_DELAY = 60
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0 # 保持每个远程服务器平均 1 个并发请求 (配合 CONCURRENT_REQUESTS 全局限制)
# 调试时可开启
# AUTOTHROTTLE_DEBUG = True

# =============================================================================
# 重试配置
# =============================================================================
RETRY_ENABLED = True # 确保基础配置开启
RETRY_TIMES = 3      # 重试 3 次

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


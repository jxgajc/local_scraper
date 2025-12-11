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
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}
# 必须使用 Asyncio 反应堆
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

# 💡 Debug: 将 headless 改为 False 可看到浏览器
PLAYWRIGHT_LAUNCH_OPTIONS = {
    'headless': True,
    'args': ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
    'timeout': 30000, # 启动超时时间
}

LOG_LEVEL = 'INFO'
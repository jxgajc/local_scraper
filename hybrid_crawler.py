import os
import sys

PROJECT_NAME = "hybrid_crawler"

def write_file(path, content):
    filepath = os.path.join(PROJECT_NAME, path)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content.strip())
    print(f"[+] Created: {filepath}")

def main():
    if not os.path.exists(PROJECT_NAME):
        os.makedirs(PROJECT_NAME)
    
    # =========================================================================
    # 0. 项目文档 README.md (新增)
    # =========================================================================
    write_file("README.md", f"""
# 高性能混合架构爬虫系统 (Hybrid Crawler)

这是一个企业级的 Scrapy 爬虫脚手架，集成了 HTTP 高并发采集与 Playwright 动态渲染采集。

## ✨ 核心特性

1.  **混合架构**：同时支持轻量级 HTTP 请求（`BaseRequestSpider`）和重量级浏览器渲染（`BasePlaywrightSpider`）。
2.  **智能重试**：
    * **网络错误**：指数退避（等待时间翻倍）。
    * **逻辑错误**：净室重试（销毁浏览器 Context，清理 Cookie 后重试）。
3.  **高可用管道**：
    * **异步 IO**：数据库写入操作在独立线程池中执行，不阻塞爬虫主循环。
    * **降级策略**：批量写入失败时自动拆包，逐条写入，隔离脏数据。
4.  **资源隔离**：浏览器上下文（Context）基于 URL 哈希隔离，防止会话污染。

## 🚀 快速开始

### 1. 环境安装

需要 Python 3.9+。

```bash
# 安装 Python 依赖
pip install -r requirements.txt

# 安装 Playwright 浏览器内核 (必须)
playwright install chromium
```

### 2. 数据库配置

本项目默认使用 MySQL。请确保本地已安装 MySQL 或使用 Docker 启动。

修改 `settings.py` 或设置环境变量：

```bash
export DATABASE_URL="mysql+pymysql://root:password@localhost:3306/spider_db"
```s

### 3. 运行爬虫

**普通模式运行：**
```bash
python run.py
```

**调试模式运行 (输出详细日志)：**
```bash
python run.py debug
```

## 🛠️ Debug 指南

### Q1: 如何看到浏览器界面？
修改 `settings.py` 中的 `PLAYWRIGHT_LAUNCH_OPTIONS`：
```python
'headless': False,  # 改为 False 即可看到浏览器操作
'slow_mo': 500,     # 增加慢动作延迟，方便人眼观察
```

### Q2: 数据库写入报错怎么办？
在 `models/__init__.py` 中开启 SQL 回显：
```python
engine = create_engine(..., echo=True) # 设置为 True 可在控制台看到所有 SQL 语句
```

### Q3: Playwright 报错 "Target closed"
通常是因为内存不足或并发过高。
1. 降低 `settings.py` 中的 `CONCURRENT_REQUESTS`。
2. 确保 `base_spiders.py` 中的 `page.close()` 逻辑正确执行。

## 📂 目录结构

* `spiders/base_spiders.py`: **核心**。定义了 HTTP 和 Playwright 的基类。
* `middlewares.py`: 定义了智能重试逻辑和请求路由。
* `pipelines.py`: 定义了异步批量写入和降级逻辑。
* `models/`: 定义了 SQLAlchemy 数据模型。
""")

    # =========================================================================
    # 1. 基础依赖 requirements.txt
    # =========================================================================
    write_file("requirements.txt", """
Scrapy>=2.11.0
scrapy-playwright>=0.0.33
SQLAlchemy>=2.0.0
PyMySQL>=1.1.0
twisted>=23.8.0
cryptography
itemadapter
psutil
""")

    write_file("scrapy.cfg", f"""
[settings]
default = {PROJECT_NAME}.settings

[deploy]
project = {PROJECT_NAME}
""")

    write_file(f"{PROJECT_NAME}/__init__.py", "")

    # =========================================================================
    # 2. 异常体系 exceptions.py
    # =========================================================================
    write_file(f"{PROJECT_NAME}/exceptions.py", """
\"\"\"
异常分类体系
用于指导中间件进行不同的重试策略
\"\"\"

class CrawlerNetworkError(IOError):
    \"\"\"
    [网络层错误]
    场景：连接超时、DNS失败、TCP重置。
    策略：触发指数退避重试 (Exponential Backoff)。
    \"\"\"
    pass

class ElementNotFoundError(ValueError):
    \"\"\"
    [逻辑层错误]
    场景：页面加载成功但关键元素未找到（可能遇到验证码或布局变更）。
    策略：触发净室重试（Clean Slate Retry），销毁 Context 重启。
    \"\"\"
    pass

class BrowserCrashError(RuntimeError):
    \"\"\"
    [运行时错误]
    场景：Playwright Page 对象崩溃或 Target Closed。
    策略：触发净室重试。
    \"\"\"
    pass

class DataValidationError(ValueError):
    \"\"\"
    [数据层错误]
    场景：清洗管道发现缺少必填字段。
    策略：直接丢弃 Item 并记录警告，不重试。
    \"\"\"
    pass
""")

    # =========================================================================
    # 3. 数据模型 models/
    # =========================================================================
    write_file(f"{PROJECT_NAME}/models/__init__.py", """
import os
from sqlalchemy import create_engine, Column, Integer, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime

# 💡 Debug提示: 将 echo=False 改为 True 可以查看生成的 SQL 语句
DATABASE_URL = os.getenv('DATABASE_URL', 'mysql+pymysql://root:password@localhost:3306/spider_db')

engine = create_engine(
    DATABASE_URL,
    pool_size=20,           # 核心连接数：保持常驻的连接数量
    max_overflow=40,        # 突发连接数：高并发时允许临时创建的连接
    pool_recycle=3600,      # 连接回收：防止 MySQL 8小时断开问题
    pool_timeout=30,
    echo=False              # 生产环境建议关闭 SQL 日志
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class BaseModel(Base):
    \"\"\"所有模型的基类，包含通用审计字段\"\"\"
    __abstract__ = True
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)

def init_db():
    \"\"\"初始化数据库表结构\"\"\"
    Base.metadata.create_all(bind=engine)
""")

    write_file(f"{PROJECT_NAME}/models/crawl_data.py", """
from sqlalchemy import Column, String, Text, Integer, JSON
from . import BaseModel

class CrawlData(BaseModel):
    __tablename__ = 'crawl_data'
    
    url = Column(String(768), nullable=False, index=True, comment="URL")
    url_hash = Column(String(64), unique=True, index=True, comment="指纹用于去重")
    title = Column(String(512), nullable=True)
    content = Column(Text, nullable=True)
    meta_info = Column(JSON, nullable=True, comment="存储额外的JSON元数据")
    status_code = Column(Integer, default=200)
    source = Column(String(64), index=True, comment="数据来源标识")
""")

    # =========================================================================
    # 4. Items items.py
    # =========================================================================
    write_file(f"{PROJECT_NAME}/items.py", """
import scrapy

class HybridCrawlerItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    status_code = scrapy.Field()
    request_type = scrapy.Field()
    source = scrapy.Field()
    meta_info = scrapy.Field()
    # 内部使用字段
    url_hash = scrapy.Field()
""")

    # =========================================================================
    # 5. 中间件 middlewares.py
    # =========================================================================
    write_file(f"{PROJECT_NAME}/middlewares.py", """
import time
import random
import logging
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from twisted.internet.error import (
    ConnectionRefusedError, DNSLookupError, TimeoutError, TCPTimedOutError
)
from .exceptions import CrawlerNetworkError, ElementNotFoundError, BrowserCrashError

logger = logging.getLogger(__name__)

class StrategyRoutingMiddleware:
    \"\"\"
    【策略路由中间件】
    作用：在请求发出前，根据 Request 的 meta 标记，决定是否启用 Playwright。
    \"\"\"
    def process_request(self, request, spider):
        request_type = request.meta.get('request_type', 'http')
        
        # 如果标记为 playwright，则激活 scrapy-playwright 插件的参数
        if request_type == 'playwright':
            request.meta['playwright'] = True
            request.meta['dont_merge_cookies'] = True # 浏览器自己管理 Cookie，不使用 Scrapy 的 CookieJar
        return None

class SmartRetryMiddleware(RetryMiddleware):
    \"\"\"
    【智能重试中间件】
    作用：替代默认的 RetryMiddleware，实现分级重试策略。
    \"\"\"
    NETWORK_ERRORS = (ConnectionRefusedError, DNSLookupError, TimeoutError, TCPTimedOutError, CrawlerNetworkError)
    LOGIC_ERRORS = (ElementNotFoundError, BrowserCrashError)

    def process_exception(self, request, exception, spider):
        retry_times = request.meta.get('retry_times', 0) + 1
        max_retries = self.max_retry_times

        if retry_times > max_retries:
            logger.error(f"❌ 放弃请求 {request.url}: 超过最大重试次数")
            return None

        # 策略 1: 网络错误 -> 指数退避 (Wait time = 2^(n-1))
        if isinstance(exception, self.NETWORK_ERRORS):
            delay = 2 ** (retry_times - 1)
            logger.warning(f"⚠️ 网络波动 ({exception}), {delay}s 后重试: {request.url}")
            time.sleep(delay) # 注意：这里简单的sleep会阻塞线程，生产环境建议使用 twisted 的 callLater，此处为演示逻辑
            return self._retry(request, exception, spider)
            
        # 策略 2: 逻辑/渲染错误 -> 净室重试 (Clean Slate)
        elif isinstance(exception, self.LOGIC_ERRORS):
            logger.warning(f"🔄 逻辑错误 ({exception}), 触发净室重试: {request.url}")
            # 关键：标记 clean_slate，告诉 Spider 在下次请求时销毁 Context
            request.meta['clean_slate'] = True 
            request.dont_filter = True
            return self._retry(request, exception, spider)

        return super().process_exception(request, exception, spider)
""")

    # =========================================================================
    # 6. 数据管道 pipelines.py
    # =========================================================================
    write_file(f"{PROJECT_NAME}/pipelines.py", """
import logging
import time
import hashlib
from twisted.internet import threads
from itemadapter import ItemAdapter
from .models import SessionLocal, init_db
from .models.crawl_data import CrawlData
from .exceptions import DataValidationError

logger = logging.getLogger(__name__)

class DataCleaningPipeline:
    \"\"\"数据清洗与校验层\"\"\"
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        # 1. 必填项校验
        if not adapter.get('url'):
            raise DataValidationError("Drop item: Missing URL")
            
        # 2. 生成指纹
        if not adapter.get('url_hash'):
            url = adapter.get('url')
            adapter['url_hash'] = hashlib.md5(url.encode('utf-8')).hexdigest()
            
        # 3. 基础清洗 (去除首尾空格)
        for k, v in adapter.items():
            if isinstance(v, str):
                adapter[k] = v.strip()
                
        return item

class AsyncBatchWritePipeline:
    \"\"\"
    【异步批量写入层】
    核心机制：
    1. Buffer: 内存中暂存 Item。
    2. DeferToThread: 将耗时的 DB 写入操作扔到线程池，避免阻塞 Scrapy 的 Reactor。
    3. Fallback: 批量失败时自动降级。
    \"\"\"
    def __init__(self, buffer_size=50, timeout=2):
        self.buffer = []
        self.buffer_size = buffer_size
        self.timeout = timeout
        self.last_flush = time.time()
        self.session_maker = SessionLocal

    @classmethod
    def from_crawler(cls, crawler):
        init_db() # 确保表存在
        settings = crawler.settings
        return cls(
            buffer_size=settings.getint('BUFFER_THRESHOLD', 100),
            timeout=settings.getfloat('BUFFER_TIMEOUT_SEC', 2.0)
        )

    def process_item(self, item, spider):
        self.buffer.append(item)
        # 检查是否达到 数量阈值 或 时间阈值
        if self._should_flush():
            # 异步调用 _flush_buffer
            df = threads.deferToThread(self._flush_buffer, list(self.buffer))
            df.addErrback(self._handle_error)
            
            # 清空 Buffer
            self.buffer.clear()
            self.last_flush = time.time()
        return item

    def _should_flush(self):
        return (len(self.buffer) >= self.buffer_size) or \
               (time.time() - self.last_flush >= self.timeout and self.buffer)

    def _flush_buffer(self, items):
        \"\"\"在独立线程中执行\"\"\"
        session = self.session_maker()
        try:
            orm_objects = [
                CrawlData(
                    url=i['url'], url_hash=i['url_hash'], title=i.get('title'),
                    content=i.get('content'), source=i.get('source'),
                    meta_info=i.get('meta_info')
                ) for i in items
            ]
            # 尝试批量写入
            session.add_all(orm_objects)
            session.commit()
            logger.info(f"✅ 成功批量写入 {len(items)} 条数据")
        except Exception as e:
            session.rollback()
            logger.error(f"⚠️ 批量写入失败: {e}，正在尝试降级为逐条写入...")
            self._fallback_single_write(session, orm_objects)
        finally:
            session.close()

    def _fallback_single_write(self, session, objects):
        \"\"\"降级策略：逐条写入，隔离脏数据\"\"\"
        success = 0
        for obj in objects:
            try:
                session.merge(obj) # 使用 merge 避免主键重复报错
                session.commit()
                success += 1
            except Exception as e:
                session.rollback()
                logger.error(f"❌ 单条写入失败 (Hash: {obj.url_hash}): {e}")
        logger.info(f"🆗 降级写入完成: 成功 {success} / 总数 {len(objects)}")

    def _handle_error(self, failure):
        logger.error(f"🔥 异步写入线程严重异常: {failure}")

    def close_spider(self, spider):
        \"\"\"爬虫关闭时，强制刷新剩余 Buffer\"\"\"
        if self.buffer:
            self._flush_buffer(self.buffer)
""")

    # =========================================================================
    # 7. 爬虫基类 spiders/base_spiders.py
    # =========================================================================
    write_file(f"{PROJECT_NAME}/spiders/__init__.py", "")
    write_file(f"{PROJECT_NAME}/spiders/base_spiders.py", """
import scrapy
from abc import ABC, abstractmethod

class BaseRequestSpider(scrapy.Spider, ABC):
    \"\"\"
    【HTTP 采集基类】
    适用：静态页面、API 接口。
    特点：极简、高并发。
    \"\"\"
    custom_settings = {
        'CONCURRENT_REQUESTS': 32,
        'DOWNLOAD_DELAY': 0.1,
    }

    def make_request(self, url, meta=None):
        meta = meta or {}
        meta['request_type'] = 'http'
        return scrapy.Request(url, meta=meta, callback=self.parse)

    @abstractmethod
    def parse_logic(self, response):
        \"\"\"业务逻辑，子类实现\"\"\"
        pass

    def parse(self, response):
        # HTTP 模式下，直接委托
        yield from self.parse_logic(response)


class BasePlaywrightSpider(BaseRequestSpider):
    \"\"\"
    【Playwright 采集基类】
    适用：SPA、JS 动态渲染、高反爬。
    特点：资源隔离、自动生命周期管理。
    \"\"\"
    custom_settings = {
        'CONCURRENT_REQUESTS': 4, # 浏览器内存占用大，务必限制并发
        'DOWNLOAD_DELAY': 1.0,
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True, # Debug时改为False
            'args': ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
        }
    }

    def make_request(self, url, meta=None):
        meta = meta or {}
        # 生成上下文 ID，确保同一任务复用 Context，不同任务隔离
        context_id = f"ctx_{hash(url) % 10000}"
        meta.update({
            'request_type': 'playwright',
            'playwright': True,
            'playwright_include_page': True,
            'playwright_context': context_id,
        })
        return scrapy.Request(url, meta=meta, callback=self.parse, errback=self.errback)

    async def parse(self, response):
        \"\"\"
        统一的 Playwright 解析入口。
        负责处理 '净室重试' 和 Page 关闭。
        \"\"\"
        page = response.meta.get("playwright_page")
        try:
            # 检查是否需要净室重试 (Clean Slate Retry)
            if response.meta.get('clean_slate'):
                await self._reset_context(page)

            # ⚡️ 核心：在 async def 中必须使用 async for 遍历异步生成器
            async for item in self.parse_logic(response, page):
                yield item

        except Exception as e:
            self.logger.error(f"Playwright 解析异常: {e} | URL: {response.url}")
            raise e # 抛出给中间件进行重试判断
        finally:
            if page:
                try:
                    await page.close()
                except:
                    pass

    @abstractmethod
    async def parse_logic(self, response, page):
        \"\"\"子类必须实现此方法，使用 yield 返回数据\"\"\"
        pass

    async def errback(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            try:
                await page.close()
            except:
                pass
        self.logger.error(f"Playwright 请求失败: {failure.getErrorMessage()}")

    async def _reset_context(self, page):
        \"\"\"内部方法：清理 Cookie 和 权限，模拟新用户\"\"\"
        if not page: return
        try:
            context = page.context
            await context.clear_cookies()
            await context.clear_permissions()
            self.logger.info("Context 已清理 (Cookies/Permissions)")
        except Exception as e:
            self.logger.warning(f"Context 清理失败: {e}")

    async def wait_and_scroll(self, page, steps=3):
        \"\"\"工具方法：智能等待与滚动\"\"\"
        try:
            await page.wait_for_load_state('networkidle', timeout=10000)
            for _ in range(steps):
                if page.is_closed(): break
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight/3)")
                await page.wait_for_timeout(500)
        except Exception as e:
            self.logger.warning(f"滚动交互异常 (非致命): {e}")
""")

    # =========================================================================
    # 8. 示例爬虫 spiders/example.py
    # =========================================================================
    write_file(f"{PROJECT_NAME}/spiders/example.py", """
from .base_spiders import BaseRequestSpider, BasePlaywrightSpider
from ..items import HybridCrawlerItem

class HackerNewsSpider(BaseRequestSpider):
    \"\"\"
    示例 1: 普通 HTTP 爬虫
    目标: HackerNews 首页列表
    \"\"\"
    name = "hn_simple"
    start_urls = ["https://news.ycombinator.com/"]

    def parse_logic(self, response):
        for row in response.css('tr.athing'):
            yield HybridCrawlerItem(
                url=row.css('.titleline a::attr(href)').get(),
                title=row.css('.titleline a::text').get(),
                source='HackerNews',
                request_type='http'
            )

class DynamicQuotesSpider(BasePlaywrightSpider):
    \"\"\"
    示例 2: Playwright 动态爬虫
    目标: Quotes to Scrape (JS版本)
    \"\"\"
    name = "quotes_dynamic"
    start_urls = ["https://quotes.toscrape.com/js/"]

    def start_requests(self):
        for url in self.start_urls:
            # 必须使用 self.make_request 来确保 playwright 参数正确
            yield self.make_request(url)

    async def parse_logic(self, response, page):
        # 1. 执行交互 (滚动)
        await self.wait_and_scroll(page)
        
        # 2. 提取数据 (使用 Playwright API)
        quotes = await page.query_selector_all('div.quote')
        for q in quotes:
            text_el = await q.query_selector('span.text')
            text = await text_el.inner_text() if text_el else ""
            
            yield HybridCrawlerItem(
                url=response.url,
                content=text,
                source='QuotesJS',
                request_type='playwright'
            )
""")

    # =========================================================================
    # 9. 核心配置 settings.py
    # =========================================================================
    write_file(f"{PROJECT_NAME}/settings.py", f"""
BOT_NAME = '{PROJECT_NAME}'
SPIDER_MODULES = ['{PROJECT_NAME}.spiders']
NEWSPIDER_MODULE = '{PROJECT_NAME}.spiders'

# =============================================================================
# 核心并发配置
# =============================================================================
CONCURRENT_REQUESTS = 32
DOWNLOAD_DELAY = 0

# =============================================================================
# 中间件管道配置
# =============================================================================
DOWNLOADER_MIDDLEWARES = {{
    '{PROJECT_NAME}.middlewares.StrategyRoutingMiddleware': 100, # 路由策略
    '{PROJECT_NAME}.middlewares.SmartRetryMiddleware': 550,      # 智能重试
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,  # 禁用默认重试
}}

ITEM_PIPELINES = {{
    '{PROJECT_NAME}.pipelines.DataCleaningPipeline': 300,        # 清洗
    '{PROJECT_NAME}.pipelines.AsyncBatchWritePipeline': 400,     # 入库
}}

# =============================================================================
# 异步写入缓冲配置
# =============================================================================
BUFFER_THRESHOLD = 100   # 积攒 100 条写入一次
BUFFER_TIMEOUT_SEC = 1.5 # 或最长等待 1.5 秒写入一次

# =============================================================================
# Playwright 专用配置
# =============================================================================
DOWNLOAD_HANDLERS = {{
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}}
# 必须使用 Asyncio 反应堆
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

# 💡 Debug: 将 headless 改为 False 可看到浏览器
PLAYWRIGHT_LAUNCH_OPTIONS = {{
    'headless': True,
    'args': ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
    'timeout': 30000, # 启动超时时间
}}

LOG_LEVEL = 'INFO'
""")

    # =========================================================================
    # 10. 运行入口 run.py (支持 debug 参数)
    # =========================================================================
    write_file("run.py", f"""
import os
import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

# 设置 Scrapy 配置文件路径
os.environ['SCRAPY_SETTINGS_MODULE'] = '{PROJECT_NAME}.settings'

# 导入爬虫 (需确保已安装 Twisted Asyncio Reactor)
from {PROJECT_NAME}.spiders.example import HackerNewsSpider, DynamicQuotesSpider

def run():
    print(">>> 正在启动高性能混合爬虫系统...")
    
    # 简单的参数解析，用于开启 Debug 模式
    is_debug = 'debug' in sys.argv
    
    settings = get_project_settings()
    
    if is_debug:
        print(">>> 🐞 Debug 模式已开启: 日志级别 DEBUG")
        settings.set('LOG_LEVEL', 'DEBUG')
    
    process = CrawlerProcess(settings)
    
    # 在这里选择要运行的爬虫
    # process.crawl(HackerNewsSpider)
    process.crawl(DynamicQuotesSpider) 
    
    process.start()

if __name__ == '__main__':
    run()
""")

    print(f"\\n[Fixed] 增强版项目生成完毕！包含详细文档与调试指南。")
    print(f"请阅读 {PROJECT_NAME}/README.md 开始使用。")

if __name__ == "__main__":
    main()
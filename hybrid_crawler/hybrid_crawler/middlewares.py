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
    """
    【策略路由中间件】
    作用：在请求发出前，根据 Request 的 meta 标记，决定是否启用 Playwright。
    """
    def process_request(self, request, spider):
        request_type = request.meta.get('request_type', 'http')
        
        # 如果标记为 playwright，则激活 scrapy-playwright 插件的参数
        if request_type == 'playwright':
            request.meta['playwright'] = True
            request.meta['dont_merge_cookies'] = True # 浏览器自己管理 Cookie，不使用 Scrapy 的 CookieJar
        return None

class SmartRetryMiddleware(RetryMiddleware):
    """
    【智能重试中间件】
    作用：替代默认的 RetryMiddleware，实现分级重试策略。
    """
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
            # 改良：不要使用 time.sleep(delay)，这会阻塞 reactor。
            # 正确做法是设置 download_delay，让 Scrapy 的调度器去等待。
            new_request = self._retry(request, exception, spider)
            if new_request:
                new_request.meta['download_delay'] = delay
            return new_request
            
        # 策略 2: 逻辑/渲染错误 -> 净室重试 (Clean Slate)
        elif isinstance(exception, self.LOGIC_ERRORS):
            logger.warning(f"🔄 逻辑错误 ({exception}), 触发净室重试: {request.url}")
            # 关键：标记 clean_slate，告诉 Spider 在下次请求时销毁 Context
            request.meta['clean_slate'] = True 
            request.dont_filter = True
            return self._retry(request, exception, spider)

        return super().process_exception(request, exception, spider)

class RandomUserAgentMiddleware:
    """
    【随机 User-Agent 中间件】
    每次请求自动随机切换 User-Agent，降低特征指纹。
    """
    def __init__(self, settings):
        self.ua_list = settings.get('USER_AGENT_LIST', [])
        # Fallback list if settings is empty
        if not self.ua_list:
            self.ua_list = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
            ]

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_request(self, request, spider):
        # 只有在 headers 中没有设置 User-Agent 时才添加，避免覆盖 Spider 特定的设置
        if not request.headers.get('User-Agent'):
            ua = random.choice(self.ua_list)
            if ua:
                request.headers.setdefault('User-Agent', ua)
                # logger.debug(f"User-Agent set to: {ua}")

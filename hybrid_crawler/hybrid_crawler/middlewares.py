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
import scrapy
from abc import ABC, abstractmethod

class BaseRequestSpider(scrapy.Spider, ABC):
    """
    【HTTP 采集基类】
    适用：静态页面、API 接口。
    特点：极简、高并发。
    """
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
        """业务逻辑，子类实现"""
        pass

    def parse(self, response):
        # HTTP 模式下，直接委托
        yield from self.parse_logic(response)


class BasePlaywrightSpider(BaseRequestSpider):
    """
    【Playwright 采集基类】
    适用：SPA、JS 动态渲染、高反爬。
    特点：资源隔离、自动生命周期管理。
    """
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
        """
        统一的 Playwright 解析入口。
        负责处理 '净室重试' 和 Page 关闭。
        """
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
        """子类必须实现此方法，使用 yield 返回数据"""
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
        """内部方法：清理 Cookie 和 权限，模拟新用户"""
        if not page: return
        try:
            context = page.context
            await context.clear_cookies()
            await context.clear_permissions()
            self.logger.info("Context 已清理 (Cookies/Permissions)")
        except Exception as e:
            self.logger.warning(f"Context 清理失败: {e}")

    async def wait_and_scroll(self, page, steps=3):
        """工具方法：智能等待与滚动"""
        try:
            await page.wait_for_load_state('networkidle', timeout=10000)
            for _ in range(steps):
                if page.is_closed(): break
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight/3)")
                await page.wait_for_timeout(500)
        except Exception as e:
            self.logger.warning(f"滚动交互异常 (非致命): {e}")
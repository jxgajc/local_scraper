from .base_spiders import BaseRequestSpider, BasePlaywrightSpider
from ..items import HybridCrawlerItem

class HackerNewsSpider(BaseRequestSpider):
    """
    示例 1: 普通 HTTP 爬虫
    目标: HackerNews 首页列表
    """
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
    """
    示例 2: Playwright 动态爬虫
    目标: Quotes to Scrape (JS版本)
    """
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
        for i, q in enumerate(quotes):
            text_el = await q.query_selector('span.text')
            text = await text_el.inner_text() if text_el else ""
            
            # 为每个引用生成唯一 URL，避免哈希冲突
            unique_url = f"{response.url}#quote_{i}"
            
            yield HybridCrawlerItem(
                url=unique_url,
                content=text,
                source='QuotesJS',
                request_type='playwright',
                meta_info={"original_url": response.url, "quote_index": i}
            )
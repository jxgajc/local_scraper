# Spider Generator Skill

根据目标网站自动探索并生成爬虫脚本。

## 触发条件

用户提供：
- 目标网站URL
- 需要采集的数据类型

## 工作流程

### 阶段一：页面探索（使用haiku模型）

由于浏览器返回信息量大，使用Task工具启动haiku模型的子代理进行探索：

```
Task(
    subagent_type="Explore",
    model="haiku",
    prompt="使用Playwright MCP探索目标页面，分析：
    1. 页面结构和数据展示区域
    2. 网络请求（API接口、参数、响应格式）
    3. 分页/筛选控件
    4. 反爬措施
    返回简洁的分析报告"
)
```

探索任务：
- `playwright_navigate` - 访问URL
- `playwright_screenshot` - 截图分析
- `playwright_click` - 交互测试
- `playwright_evaluate` - 执行JS获取数据
- 监控Network请求识别API

### 阶段二：需求确认（主模型）

根据探索报告，向用户确认：

1. **数据来源**
   - API接口 vs 页面渲染
   - 接口地址、方法、参数
   - 认证方式

2. **字段映射**
   - 需要采集的字段
   - 唯一ID生成规则

3. **采集策略**
   - 单层/多层采集
   - 分页方式
   - 并发数建议

### 阶段三：代码生成（主模型）

#### Model/Item 文件
位置: `hybrid_crawler/models/{region}_drug.py`

```python
import scrapy
import hashlib
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, Text
from . import BaseModel

class {Region}DrugItem(scrapy.Item):
    # 业务字段
    field1 = scrapy.Field()
    # 系统字段
    md5_id = scrapy.Field()
    collect_time = scrapy.Field()
    source_data = scrapy.Field()

    def generate_md5_id(self):
        sign_str = f"{self.get('key1')}|{self.get('key2')}"
        self['md5_id'] = hashlib.md5(sign_str.encode('utf-8')).hexdigest()
        self['collect_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def get_model_class(self):
        return {Region}Drug

class {Region}Drug(BaseModel):
    __tablename__ = 'drug_hospital_{region}'
    id = Column(Integer, primary_key=True, autoincrement=True)
    md5_id = Column(String(32), index=True)
    source_data = Column(Text)
    collect_time = Column(DateTime, default=datetime.now)
```

#### Spider 文件
位置: `hybrid_crawler/spiders/{region}_drug_store.py`

**HTTP API模式:**
```python
from .mixins import SpiderStatusMixin
import scrapy
from scrapy.http import JsonRequest

class {Region}DrugSpider(SpiderStatusMixin, scrapy.Spider):
    name = "{region}_drug_spider"

    def start_requests(self):
        yield JsonRequest(url, data=payload, callback=self.parse_list)

    def parse_list(self, response):
        # 解析、yield items、翻页
```

**Playwright模式:**
```python
from .base_spiders import BasePlaywrightSpider

class {Region}DrugSpider(BasePlaywrightSpider):
    async def parse_logic(self, response, page):
        await self.wait_and_scroll(page)
        # 提取数据
```

#### 注册爬虫
位置: `run.py`

```python
from hybrid_crawler.spiders.{region}_drug_store import {Region}DrugSpider
SPIDER_MAP['{region}_drug_spider'] = {Region}DrugSpider
```

### 阶段四：测试验证

```bash
python run.py {region}_drug_spider debug
```

Dashboard查看: http://localhost:5210

## 状态上报方法

```python
yield self.report_list_page(crawl_id, page_no, total_pages, items_found, params, api_url, parent_crawl_id)
yield self.report_detail_page(crawl_id, page_no, items_found, params, api_url, parent_crawl_id, reference_id)
yield self.report_error(stage, error_msg, crawl_id, params, api_url)
```

## 请求类型

| 场景 | 类型 |
|------|------|
| GET | `scrapy.Request` |
| POST JSON | `JsonRequest` |
| POST Form | `FormRequest` |
| JS渲染 | `BasePlaywrightSpider` |

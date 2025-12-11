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
```

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
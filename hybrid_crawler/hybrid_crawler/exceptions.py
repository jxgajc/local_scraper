"""
异常分类体系
用于指导中间件进行不同的重试策略
"""

class CrawlerNetworkError(IOError):
    """
    [网络层错误]
    场景：连接超时、DNS失败、TCP重置。
    策略：触发指数退避重试 (Exponential Backoff)。
    """
    pass

class ElementNotFoundError(ValueError):
    """
    [逻辑层错误]
    场景：页面加载成功但关键元素未找到（可能遇到验证码或布局变更）。
    策略：触发净室重试（Clean Slate Retry），销毁 Context 重启。
    """
    pass

class BrowserCrashError(RuntimeError):
    """
    [运行时错误]
    场景：Playwright Page 对象崩溃或 Target Closed。
    策略：触发净室重试。
    """
    pass

class DataValidationError(ValueError):
    """
    [数据层错误]
    场景：清洗管道发现缺少必填字段。
    策略：直接丢弃 Item 并记录警告，不重试。
    """
    pass
import os
import logging
import datetime
from logging.handlers import RotatingFileHandler

class SpiderLogger:
    """爬虫日志管理器，为每个爬虫创建独立的日志文件"""
    
    def __init__(self, spider_name, log_level=logging.INFO):
        self.spider_name = spider_name
        self.log_level = log_level
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """配置日志记录器"""
        # 创建logger
        logger = logging.getLogger(self.spider_name)
        logger.setLevel(self.log_level)
        logger.propagate = False  # 防止日志传播到根logger
        
        # 清除已存在的handler
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # 创建格式化器
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 控制台handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # 文件handler - 爬虫独立日志
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'log')
        os.makedirs(log_dir, exist_ok=True)
        
        # 日志文件名：爬虫名.log (不带日期，方便dashboard读取)
        log_filename = f"{self.spider_name}.log"
        log_filepath = os.path.join(log_dir, log_filename)
        
        # 创建RotatingFileHandler，限制单个文件大小和备份数量
        file_handler = RotatingFileHandler(
            log_filepath,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,           # 最多5个备份
            encoding='utf-8'
        )
        file_handler.setLevel(self.log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        return logger
    
    def get_logger(self):
        """获取配置好的logger"""
        return self.logger

def setup_spider_logger(spider_name, log_level=logging.INFO):
    """为爬虫设置独立日志"""
    return SpiderLogger(spider_name, log_level).get_logger()

# 为每个爬虫预创建logger字典
logger_cache = {}

def get_spider_logger(spider_name, log_level=logging.INFO):
    """获取或创建爬虫logger"""
    if spider_name not in logger_cache:
        logger_cache[spider_name] = setup_spider_logger(spider_name, log_level)
    return logger_cache[spider_name]

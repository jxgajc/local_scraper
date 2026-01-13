"""
BaseRecrawlAdapter - 补充采集适配器抽象基类
"""
import time
import logging
import requests
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Callable
from ..models import SessionLocal

logger = logging.getLogger(__name__)


class BaseRecrawlAdapter(ABC):
    """
    补充采集适配器抽象基类

    子类需要实现:
    - fetch_all_ids() - 从官网API获取所有ID
    - recrawl_by_ids() - 根据缺失ID执行补采
    """

    # 子类必须定义
    spider_name: str = None
    table_name: str = None
    unique_id: str = None

    # 可选配置
    request_delay: float = 3.0  # 请求间隔(秒)

    def __init__(self, logger_instance=None, stop_check: Callable = None):
        self.logger = logger_instance or logger
        self.stop_check = stop_check
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """创建HTTP会话"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
        })
        return session

    def _should_stop(self) -> bool:
        """检查是否应该停止"""
        return self.stop_check and self.stop_check()

    def _delay(self):
        """请求间隔延迟"""
        time.sleep(self.request_delay)

    @abstractmethod
    def fetch_all_ids(self) -> Dict[str, Any]:
        """
        从官网API获取所有ID及其基础信息

        Returns:
            {unique_id: base_info_dict} 字典
        """
        pass

    @abstractmethod
    def recrawl_by_ids(self, missing_data: Dict[str, Any], db_session) -> int:
        """
        根据缺失数据执行补采

        Args:
            missing_data: {unique_id: base_info} 字典
            db_session: 数据库会话

        Returns:
            成功补采的记录数
        """
        pass

    def find_missing(self) -> Dict[str, Any]:
        """
        查找缺失的数据

        Returns:
            {unique_id: base_info} 缺失数据字典
        """
        from sqlalchemy import text

        if not self.table_name or not self.unique_id:
            self.logger.warning(f"[{self.spider_name}] table_name 或 unique_id 未配置")
            return {}

        db = SessionLocal()
        try:
            # 获取数据库中已有的ID
            self.logger.info(f"[{self.spider_name}] 从数据库获取现有 {self.unique_id}...")
            sql = text(f"SELECT DISTINCT {self.unique_id} FROM {self.table_name}")
            result = db.execute(sql)
            existing_ids = {str(row[0]) for row in result if row[0] is not None}
            self.logger.info(f"[{self.spider_name}] 数据库中已有 {len(existing_ids)} 条记录")

            # 从API获取所有ID
            self.logger.info(f"[{self.spider_name}] 从官网API获取所有 {self.unique_id}...")
            api_data = self.fetch_all_ids()
            self.logger.info(f"[{self.spider_name}] 官网API共有 {len(api_data)} 条记录")

            # 计算缺失
            missing_data = {k: v for k, v in api_data.items() if k not in existing_ids}
            self.logger.info(f"[{self.spider_name}] 发现 {len(missing_data)} 条缺失数据")

            return missing_data

        except Exception as e:
            self.logger.error(f"[{self.spider_name}] 检查缺失数据失败: {e}")
            return {}
        finally:
            db.close()

    def recrawl(self, missing_ids=None) -> int:
        """
        执行补充采集

        Args:
            missing_ids: 可选，指定要补采的ID。如果为None则自动查找缺失数据

        Returns:
            成功补采的记录数
        """
        # 如果没有指定missing_ids，自动查找
        if missing_ids is None:
            missing_data = self.find_missing()
        elif isinstance(missing_ids, (list, set, tuple)):
            # 如果传入的是ID列表，需要重新获取完整信息
            self.logger.info(f"[{self.spider_name}] 收到ID列表，正在获取完整信息...")
            all_data = self.fetch_all_ids()
            missing_data = {k: v for k, v in all_data.items() if k in missing_ids}
        else:
            # 假设是字典格式
            missing_data = missing_ids

        if not missing_data:
            self.logger.info(f"[{self.spider_name}] 没有需要补采的数据")
            return 0

        db = SessionLocal()
        try:
            count = self.recrawl_by_ids(missing_data, db)
            self.logger.info(f"[{self.spider_name}] 补采完成，成功 {count} 条")
            return count
        except Exception as e:
            self.logger.error(f"[{self.spider_name}] 补采执行失败: {e}")
            return 0
        finally:
            db.close()

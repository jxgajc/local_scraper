#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬虫数据补采脚本
功能：通过官网API确定缺失数据并进行补采
"""

import os
import sys
import json
import argparse
import logging
import requests
from datetime import datetime
from sqlalchemy import create_engine, text

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# 确保能找到 hybrid_crawler 包
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('recrawl.log'),
        logging.StreamHandler()
    ]
)

# 全局请求会话，用于复用连接
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
})

try:
    from hybrid_crawler.models import SessionLocal
    from hybrid_crawler.models.spider_progress import SpiderProgress
except ImportError:
    # 尝试调整路径再次导入
    sys.path.insert(0, os.path.join(project_root, 'hybrid_crawler'))
    from hybrid_crawler.models import SessionLocal
    from hybrid_crawler.models.spider_progress import SpiderProgress

from sqlalchemy import func
from sqlalchemy.dialects.mysql import insert

class BaseRecrawler:
    """补采基类"""
    def __init__(self):
        self.db_session = SessionLocal()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.table_name = None
        self.unique_id = None
        self.list_api = None
        self.detail_api = None
        # 新增: 爬虫名称，用于更新进度
        self.spider_name = "unknown"
        # 新增: 停止标志
        self.stop_requested = False

    def stop(self):
        """请求停止任务"""
        self.stop_requested = True
        self.logger.info(f"[{self.spider_name}] 收到停止信号...")

    def _update_progress(self, current, total, status_text="Checking"):
        """更新前端进度条 (SpiderProgress)"""
        if self.spider_name == "unknown": return

        try:
            # 使用独立Session防止干扰主逻辑
            session = SessionLocal()
            try:
                progress_percent = 0
                if total > 0:
                    progress_percent = round((current / total) * 100, 2)
                
                # 构造数据
                data = {
                    'spider_name': self.spider_name,
                    'run_id': f"recrawl_{datetime.now().strftime('%Y%m%d')}",
                    'status': 'running',
                    'completed_tasks': current,
                    'total_tasks': total,
                    'progress_percent': progress_percent,
                    'current_stage': 'recrawl_check',
                    'current_item': f"{status_text} | {current}/{total}",
                    'updated_at': func.now()
                }

                # Upsert
                stmt = insert(SpiderProgress).values(data)
                update_dict = {
                    'status': stmt.inserted.status,
                    'completed_tasks': stmt.inserted.completed_tasks,
                    'total_tasks': stmt.inserted.total_tasks,
                    'progress_percent': stmt.inserted.progress_percent,
                    'current_stage': stmt.inserted.current_stage,
                    'current_item': stmt.inserted.current_item,
                    'updated_at': func.now()
                }
                upsert_stmt = stmt.on_duplicate_key_update(**update_dict)
                session.execute(upsert_stmt)
                session.commit()
            except Exception as e:
                # 进度更新失败不应影响主流程
                # self.logger.warning(f"进度更新失败: {e}") 
                pass
            finally:
                session.close()
        except:
            pass

    
    def get_existing_ids(self):
        """从数据库获取已采集的唯一标识"""
        try:
            self.logger.info(f"从数据库获取{self.table_name}表中的现有{self.unique_id}...")
            # 修正：直接查询 self.table_name (_test表)，作为待检查的原始表格
            sql = text(f"SELECT DISTINCT {self.unique_id} FROM {self.table_name}")
            result = self.db_session.execute(sql)
            existing_ids = {row[0] for row in result if row[0] is not None}
            self.logger.info(f"成功获取{len(existing_ids)}个现有{self.unique_id}")
            return existing_ids
        except Exception as e:
            self.logger.error(f"获取现有ID失败: {e}")
            raise
    
    def sync_to_production(self):
        """将_test表的数据同步到正式表"""
        if not self.table_name.endswith('_test'):
            self.logger.warning(f"表名 {self.table_name} 不以 _test 结尾，跳过同步")
            return 0
            
        prod_table = self.table_name.replace('_test', '')
        try:
            self.logger.info(f"开始将数据从 {self.table_name} 同步到 {prod_table}...")
            # 使用 INSERT IGNORE ... SELECT 进行增量同步并去重
            sql = text(f"INSERT IGNORE INTO {prod_table} SELECT * FROM {self.table_name}")
            result = self.db_session.execute(sql)
            self.db_session.commit()
            
            self.logger.info(f"同步完成，{prod_table} 受影响行数: {result.rowcount}")
            return result.rowcount
        except Exception as e:
            self.db_session.rollback()
            self.logger.error(f"同步到正式表失败: {e}")
            raise
    
    def fetch_api_total(self):
        """从官网API获取总数和所有唯一标识"""
        raise NotImplementedError("子类必须实现fetch_api_total方法")
    
    def find_missing(self):
        """找出缺失的唯一标识"""
        try:
            self.logger.info("开始查找缺失数据...")
            existing_ids = self.get_existing_ids()
            api_ids = self.fetch_api_total()
            
            missing_ids = api_ids - existing_ids
            self.logger.info(f"共发现{len(missing_ids)}个缺失的{self.unique_id}")
            return missing_ids
        except Exception as e:
            self.logger.error(f"查找缺失数据失败: {e}")
            raise
    
    def recrawl(self):
        """执行补采"""
        raise NotImplementedError("子类必须实现recrawl方法")
    
    def close(self):
        """关闭数据库连接"""
        self.db_session.close()

class FujianRecrawler(BaseRecrawler):
    """福建爬虫补采类"""
    def __init__(self):
        super().__init__()
        self.spider_name = "fujian_drug_store" # 对应 SPIDER_MAP 中的名字
        self.table_name = "drug_hospital_fujian_test"
        self.unique_id = "ext_code"
        self.list_api = "https://open.ybj.fujian.gov.cn:10013/tps-local/web/tender/plus/item-cfg-info/list"
        self.detail_api = "https://open.ybj.fujian.gov.cn:10013/tps-local/web/trans/api/open/v2/queryHospital"
    
    def fetch_api_total(self):
        """从福建API获取所有ext_code"""
        self.logger.info(f"开始从福建API获取所有{self.unique_id}...")
        api_ids = set()
        current = 1
        page_size = 1000
        
        while True:
            if self.stop_requested:
                self.logger.warning(f"[{self.spider_name}] 任务被用户中止")
                break

            try:
                payload = {
                    "druglistName": "",
                    "druglistCode": "",
                    "drugName": "",
                    "ruteName": "",
                    "dosformName": "",
                    "specName": "",
                    "pac": "",
                    "prodentpName": "",
                    "current": current,
                    "size": page_size,
                    "tenditmType": ""
                }
                
                self.logger.info(f"请求福建API第{current}页...")
                response = session.post(self.list_api, json=payload, timeout=30)
                response.raise_for_status()
                res_json = response.json()
                
                if res_json.get("code") != 0:
                    self.logger.error(f"福建API返回错误: {res_json.get('message', 'Unknown error')}")
                    break
                
                data_block = res_json.get("data", {})
                records = data_block.get("records", [])
                total_pages = data_block.get("pages", 0)
                
                self.logger.info(f"福建API第{current}/{total_pages}页，获取{len(records)}条记录")
                
                # 更新前端进度
                self._update_progress(current, total_pages, f"Fetching page {current}")

                # 提取ext_code
                for record in records:
                    ext_code = record.get('extCode')
                    if ext_code:
                        api_ids.add(ext_code)
                
                if current >= total_pages:
                    break
                
                current += 1
            except Exception as e:
                self.logger.error(f"请求福建API失败: {e}")
                break
        
        self.logger.info(f"成功从福建API获取{len(api_ids)}个{self.unique_id}")
        return api_ids
    
    def recrawl(self):
        """执行福建爬虫补采"""
        self.logger.info("开始执行福建爬虫补采...")
        missing_ids = self.find_missing()
        
        if not missing_ids:
            self.logger.info("没有缺失数据，无需补采")
            return
        
        # 这里需要实现具体的补采逻辑
        # 由于补采逻辑比较复杂，需要复用现有爬虫的解析逻辑
        # 暂时只输出缺失的ID
        self.logger.info(f"福建爬虫需要补采{len(missing_ids)}条数据")
        self.logger.info(f"缺失的{self.unique_id}示例: {list(missing_ids)[:10]}")
        
        return len(missing_ids)

class GuangdongRecrawler(BaseRecrawler):
    """广东爬虫补采类"""
    def __init__(self):
        super().__init__()
        self.spider_name = "guangdong_drug_spider"
        self.table_name = "drug_hospital_guangdong_test"
        self.unique_id = "drug_code"
        self.list_api = "https://igi.hsa.gd.gov.cn/tps_local_bd/web/publicity/pubonlnPublicity/queryPubonlnPage"
        self.detail_api = "https://igi.hsa.gd.gov.cn/tps_local_bd/web/publicity/pubonlnPublicity/getPurcHospitalInfoListNew"
    
    def fetch_api_total(self):
        """从广东API获取所有drug_code"""
        self.logger.info(f"开始从广东API获取所有{self.unique_id}...")
        api_ids = set()
        current = 1
        page_size = 500
        
        while True:
            if self.stop_requested:
                self.logger.warning(f"[{self.spider_name}] 任务被用户中止")
                break

            try:
                payload = {
                    "current": current,
                    "size": page_size,
                    "searchCount": True
                }
                
                self.logger.info(f"请求广东API第{current}页...")
                response = session.post(self.list_api, json=payload, timeout=30)
                response.raise_for_status()
                res_json = response.json()
                
                data_block = res_json.get("data", {})
                records = data_block.get("records", [])
                total_pages = data_block.get("pages", 0)
                
                self.logger.info(f"广东API第{current}/{total_pages}页，获取{len(records)}条记录")
                
                # 更新前端进度
                self._update_progress(current, total_pages, f"Fetching page {current}")

                # 提取drug_code
                for record in records:
                    drug_code = record.get('drugCode')
                    if drug_code:
                        api_ids.add(drug_code)
                
                if current >= total_pages:
                    break
                
                current += 1
            except Exception as e:
                self.logger.error(f"请求广东API失败: {e}")
                break
        
        self.logger.info(f"成功从广东API获取{len(api_ids)}个{self.unique_id}")
        return api_ids
    
    def recrawl(self):
        """执行广东爬虫补采"""
        self.logger.info("开始执行广东爬虫补采...")
        missing_ids = self.find_missing()
        
        if not missing_ids:
            self.logger.info("没有缺失数据，无需补采")
            return
        
        self.logger.info(f"广东爬虫需要补采{len(missing_ids)}条数据")
        self.logger.info(f"缺失的{self.unique_id}示例: {list(missing_ids)[:10]}")
        
        return len(missing_ids)

class HainanRecrawler(BaseRecrawler):
    """海南爬虫补采类"""
    def __init__(self):
        super().__init__()
        self.spider_name = "hainan_drug_store"
        self.table_name = "drug_shop_hainan_test"
        self.unique_id = "drug_code"
        self.list_api = "https://ybj.hainan.gov.cn/tps-local/local/web/std/drugStore/getDrugStore"
        self.detail_api = "https://ybj.hainan.gov.cn/tps-local/local/web/std/drugStore/getDrugStoreDetl"
    
    def fetch_api_total(self):
        """从海南API获取所有drug_code"""
        self.logger.info(f"开始从海南API获取所有{self.unique_id}...")
        api_ids = set()
        current = 1
        page_size = 500
        
        while True:
            if self.stop_requested:
                self.logger.warning(f"[{self.spider_name}] 任务被用户中止")
                break

            try:
                params = {
                    "current": current,
                    "size": page_size,
                    "prodName": ""
                }
                
                self.logger.info(f"请求海南API第{current}页...")
                response = session.get(self.list_api, params=params, timeout=30)
                response.raise_for_status()
                res_json = response.json()
                
                data_block = res_json.get("data", {})
                records = data_block.get("records", [])
                total_pages = data_block.get("pages", 0)
                
                self.logger.info(f"海南API第{current}/{total_pages}页，获取{len(records)}条记录")
                
                # 更新前端进度
                self._update_progress(current, total_pages, f"Fetching page {current}")

                # 提取drug_code (prodCode)
                for record in records:
                    drug_code = record.get('prodCode')
                    if drug_code:
                        api_ids.add(drug_code)
                
                if current >= total_pages:
                    break
                
                current += 1
            except Exception as e:
                self.logger.error(f"请求海南API失败: {e}")
                break
        
        self.logger.info(f"成功从海南API获取{len(api_ids)}个{self.unique_id}")
        return api_ids
    
    def recrawl(self):
        """执行海南爬虫补采"""
        self.logger.info("开始执行海南爬虫补采...")
        missing_ids = self.find_missing()
        
        if not missing_ids:
            self.logger.info("没有缺失数据，无需补采")
            return
        
        self.logger.info(f"海南爬虫需要补采{len(missing_ids)}条数据")
        self.logger.info(f"缺失的{self.unique_id}示例: {list(missing_ids)[:10]}")
        
        return len(missing_ids)

class TianjinRecrawler(BaseRecrawler):
    """天津爬虫补采类"""
    def __init__(self):
        super().__init__()
        self.spider_name = "tianjin_drug_spider"
        self.table_name = "drug_hospital_tianjin_test"
        self.unique_id = "med_id"
        self.list_api = "https://tps.ylbz.tj.gov.cn/csb/1.0.0/guideGetMedList"
        self.detail_api = "https://tps.ylbz.tj.gov.cn/csb/1.0.0/guideGetHosp"
    
    def fetch_api_total(self):
        """从天津API获取所有med_id"""
        self.logger.info(f"开始从天津API获取所有{self.unique_id}...")
        api_ids = set()
        
        # 天津爬虫需要验证码，暂时只返回空集合
        # 实际实现需要处理验证码问题
        self.logger.warning("天津API需要验证码，暂时无法自动获取所有med_id")
        
        return api_ids
    
    def recrawl(self):
        """执行天津爬虫补采"""
        self.logger.info("开始执行天津爬虫补采...")
        self.logger.warning("天津爬虫需要验证码，暂时无法自动补采")
        return 0

class LiaoningRecrawler(BaseRecrawler):
    """辽宁爬虫补采类"""
    def __init__(self):
        super().__init__()
        self.spider_name = "liaoning_drug_store"
        self.table_name = "drug_hospital_liaoning_test"
        self.unique_id = "ProductName"  # 使用药品名称作为标识
        self.list_api = "https://ggzy.ln.gov.cn/medical"
        self.detail_api = None
    
    def fetch_api_total(self):
        """从辽宁API获取所有ProductName"""
        self.logger.info(f"开始从辽宁API获取所有{self.unique_id}...")
        api_ids = set()
        
        # 辽宁爬虫是基于关键词的，暂时只返回空集合
        # 实际实现需要读取关键词文件并逐个请求
        self.logger.warning("辽宁API是基于关键词的，暂时无法自动获取所有ProductName")
        
        return api_ids
    
    def get_existing_ids(self):
        """从数据库获取已采集的药品名称"""
        try:
            self.logger.info(f"从数据库获取{self.table_name}表中的现有{self.unique_id}...")
            sql = text(f"SELECT DISTINCT ProductName FROM {self.table_name}")
            result = self.db_session.execute(sql)
            existing_ids = {row[0] for row in result if row[0] is not None}
            self.logger.info(f"成功获取{len(existing_ids)}个现有{self.unique_id}")
            return existing_ids
        except Exception as e:
            self.logger.error(f"获取现有ID失败: {e}")
            # 返回空集合，避免补采失败
            return set()
    
    def recrawl(self):
        """执行辽宁爬虫补采"""
        self.logger.info("开始执行辽宁爬虫补采...")
        self.logger.warning("辽宁爬虫是基于关键词的，暂时无法自动补采")
        return 0

class NingxiaRecrawler(BaseRecrawler):
    """宁夏爬虫补采类"""
    def __init__(self):
        super().__init__()
        self.spider_name = "ningxia_drug_store"
        self.table_name = "drug_hospital_ningxia_test"
        self.unique_id = "procurecatalogId"
        self.list_api = "https://nxyp.ylbz.nx.gov.cn/cms/recentPurchaseDetail/getRecentPurchaseDetailData.html"
        self.detail_api = "https://nxyp.ylbz.nx.gov.cn/cms/recentPurchaseDetail/getDrugDetailDate.html"
    
    def fetch_api_total(self):
        """从宁夏API获取所有procurecatalogId"""
        self.logger.info(f"开始从宁夏API获取所有{self.unique_id}...")
        api_ids = set()
        current = 1
        page_size = 100
        
        while True:
            if self.stop_requested:
                self.logger.warning(f"[{self.spider_name}] 任务被用户中止")
                break

            try:
                form_data = {
                    "_search": "false",
                    "page": str(current),
                    "rows": str(page_size),
                    "sidx": "",
                    "sord": "asc"
                }
                
                self.logger.info(f"请求宁夏API第{current}页...")
                response = session.post(self.list_api, data=form_data, timeout=30)
                response.raise_for_status()
                res_json = response.json()
                
                total_records = res_json.get("total", 0)
                records = res_json.get("rows", [])
                
                self.logger.info(f"宁夏API第{current}页，获取{len(records)}条记录，总计{total_records}条")
                
                # 更新前端进度
                # 计算总页数
                total_pages = (total_records + page_size - 1) // page_size if page_size > 0 else 1
                self._update_progress(current, total_pages, f"Fetching page {current}")

                # 提取procurecatalogId
                for record in records:
                    procurecatalog_id = record.get('procurecatalogId')
                    if procurecatalog_id:
                        api_ids.add(procurecatalog_id)
                
                if len(api_ids) >= total_records:
                    break
                
                current += 1
            except Exception as e:
                self.logger.error(f"请求宁夏API失败: {e}")
                break
        
        self.logger.info(f"成功从宁夏API获取{len(api_ids)}个{self.unique_id}")
        return api_ids
    
    def recrawl(self):
        """执行宁夏爬虫补采"""
        self.logger.info("开始执行宁夏爬虫补采...")
        missing_ids = self.find_missing()
        
        if not missing_ids:
            self.logger.info("没有缺失数据，无需补采")
            return
        
        self.logger.info(f"宁夏爬虫需要补采{len(missing_ids)}条数据")
        self.logger.info(f"缺失的{self.unique_id}示例: {list(missing_ids)[:10]}")
        
        return len(missing_ids)

class HebeiRecrawler(BaseRecrawler):
    """河北爬虫补采类"""
    def __init__(self):
        super().__init__()
        self.spider_name = "hebei_drug_store"
        self.table_name = "drug_hospital_hebei_test"
        self.unique_id = "prodCode"
        self.list_api = "https://ylbzj.hebei.gov.cn/templates/default_pc/syyypqxjzcg/queryPubonlnDrudInfoList"
        self.detail_api = "https://ylbzj.hebei.gov.cn/templates/default_pc/syyypqxjzcg/queryProcurementMedinsList"
    
    def fetch_api_total(self):
        """从河北API获取所有prod_code"""
        self.logger.info(f"开始从河北API获取所有{self.unique_id}...")
        api_ids = set()
        current = 1
        page_size = 1000
        
        headers = {
            "prodType": "2"
        }
        
        while True:
            if self.stop_requested:
                self.logger.warning(f"[{self.spider_name}] 任务被用户中止")
                break

            try:
                params = {
                    "pageNo": current,
                    "pageSize": page_size,
                    "prodName": "",
                    "prodentpName": ""
                }
                
                self.logger.info(f"请求河北API第{current}页...")
                response = session.get(self.list_api, params=params, headers=headers, timeout=30)
                response.raise_for_status()
                res_json = response.json()
                
                data_block = res_json.get("data", {})
                records = data_block.get("list", [])
                total_records = data_block.get("total", 0)
                
                self.logger.info(f"河北API第{current}页，获取{len(records)}条记录，总计{total_records}条")
                
                # 更新前端进度
                # 计算总页数
                total_pages = (total_records + page_size - 1) // page_size if page_size > 0 else 1
                self._update_progress(current, total_pages, f"Fetching page {current}")

                # 提取prod_code
                for record in records:
                    prod_code = record.get('prodCode')
                    if prod_code:
                        api_ids.add(prod_code)
                
                if len(api_ids) >= total_records:
                    break
                
                current += 1
            except Exception as e:
                self.logger.error(f"请求河北API失败: {e}")
                break
        
        self.logger.info(f"成功从河北API获取{len(api_ids)}个{self.unique_id}")
        return api_ids
    
    def recrawl(self):
        """执行河北爬虫补采"""
        self.logger.info("开始执行河北爬虫补采...")
        missing_ids = self.find_missing()
        
        if not missing_ids:
            self.logger.info("没有缺失数据，无需补采")
            return
        
        self.logger.info(f"河北爬虫需要补采{len(missing_ids)}条数据")
        self.logger.info(f"缺失的{self.unique_id}示例: {list(missing_ids)[:10]}")
        
        return len(missing_ids)

# 爬虫映射表
SPIDER_MAPPING = {
    "fujian": FujianRecrawler,
    "guangdong": GuangdongRecrawler,
    "hainan": HainanRecrawler,
    "tianjin": TianjinRecrawler,
    "liaoning": LiaoningRecrawler,
    "ningxia": NingxiaRecrawler,
    "hebei": HebeiRecrawler
}

def check_all_spiders():
    """检查所有爬虫的缺失情况"""
    logger = logging.getLogger("check_all_spiders")
    logger.info("开始检查所有爬虫的缺失情况...")
    
    report = {}
    
    for spider_name, crawler_class in SPIDER_MAPPING.items():
        logger.info(f"\n=== 检查{spider_name}爬虫 ===")
        crawler = None
        try:
            crawler = crawler_class()
            missing_ids = crawler.find_missing()
            report[spider_name] = {
                "missing_count": len(missing_ids),
                "missing_ids": list(missing_ids)[:5]  # 只显示前5个示例
            }
        except Exception as e:
            logger.error(f"检查{spider_name}爬虫失败: {e}")
            report[spider_name] = {
                "error": str(e)
            }
        finally:
            if crawler:
                crawler.close()
    
    # 生成报告
    logger.info("\n=== 补采检查报告 ===")
    for spider_name, result in report.items():
        if "error" in result:
            logger.info(f"{spider_name}: 错误 - {result['error']}")
        else:
            logger.info(f"{spider_name}: 缺失{result['missing_count']}条数据")
            if result['missing_ids']:
                logger.info(f"  示例: {result['missing_ids']}")
    
    return report

def recrawl_spider(spider_name):
    """执行特定爬虫的补采"""
    logger = logging.getLogger("recrawl_spider")
    
    if spider_name not in SPIDER_MAPPING:
        logger.error(f"未知的爬虫名称: {spider_name}")
        logger.info(f"可用的爬虫名称: {list(SPIDER_MAPPING.keys())}")
        return False
    
    logger.info(f"开始执行{spider_name}爬虫的补采...")
    
    crawler = SPIDER_MAPPING[spider_name]()
    try:
        missing_count = crawler.recrawl()
        logger.info(f"{spider_name}爬虫补采完成，补采了{missing_count}条数据")
        
        # 同步到正式表
        if missing_count is not None:
             logger.info(f"正在将{spider_name}的新数据同步到正式表...")
             crawler.sync_to_production()
             
        return True
    except Exception as e:
        logger.error(f"执行{spider_name}爬虫补采失败: {e}")
        return False
    finally:
        crawler.close()

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="爬虫数据补采工具")
    parser.add_argument("--check", action="store_true", help="检查所有爬虫缺失情况")
    parser.add_argument("--recrawl", type=str, help="执行特定爬虫补采")
    args = parser.parse_args()
    
    if args.check:
        check_all_spiders()
    elif args.recrawl:
        recrawl_spider(args.recrawl)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()

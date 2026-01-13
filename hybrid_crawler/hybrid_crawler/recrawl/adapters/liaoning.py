"""
辽宁省补充采集适配器
"""
import json
import hashlib
import os
import aiohttp
import pandas as pd
from datetime import datetime
from typing import Dict, Any

from ..base_adapter import BaseRecrawlAdapter
from ..registry import register_adapter

# 获取关键词文件路径
script_dir = os.path.dirname(os.path.abspath(__file__))
excel_path = os.path.join(script_dir, "../../../关键字采集(2).xlsx")


@register_adapter('liaoning_drug_store')
class LiaoningRecrawlAdapter(BaseRecrawlAdapter):
    """辽宁省补充采集适配器 - POST Form + 关键词"""

    spider_name = 'liaoning_drug_store'
    table_name = 'drug_hospital_liaoning_test'
    unique_id = 'md5_id'

    list_api_url = "https://ggzy.ln.gov.cn/medical"

    def _load_keywords(self):
        """加载关键词列表"""
        try:
            df = pd.read_excel(excel_path)
            return df.loc[:, "采集关键字"].to_list()
        except Exception as e:
            self.logger.error(f"[{self.spider_name}] 关键词文件加载失败: {e}")
            return []

    def _generate_md5(self, record: Dict) -> str:
        """生成记录的MD5标识"""
        field_values = {k: v for k, v in record.items() if k not in ['md5_id', 'collect_time']}
        sorted_json = json.dumps(field_values, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(sorted_json.encode('utf-8')).hexdigest()

    async def fetch_all_ids(self) -> Dict[str, Any]:
        """从官网API获取所有数据"""
        api_data = {}
        keywords = self._load_keywords()

        if not keywords:
            return api_data

        headers = {**self.default_headers, 'Content-Type': 'application/x-www-form-urlencoded'}

        async with aiohttp.ClientSession(headers=headers) as session:
            for keyword in keywords:
                if self._should_stop():
                    break

                page_num = 1
                while True:
                    if self._should_stop():
                        break
                    try:
                        form_data = {
                            "apiName": "GetYPYYCG",
                            "product": keyword,
                            "company": "",
                            "pageNum": str(page_num)
                        }
                        async with session.post(self.list_api_url, data=form_data, timeout=30) as resp:
                            res_json = await resp.json()

                        data_block = res_json.get("data", {})
                        rows = data_block.get("data", [])
                        total_pages = int(data_block.get("totalPage", 0))

                        for record in rows:
                            md5_id = self._generate_md5(record)
                            api_data[md5_id] = record

                        self.logger.info(f"[{self.spider_name}] 关键词[{keyword}] 第{page_num}/{total_pages}页，获取{len(rows)}条")

                        if page_num >= total_pages:
                            break
                        page_num += 1
                        await self._delay()

                    except Exception as e:
                        self.logger.error(f"[{self.spider_name}] 请求API失败: {e}")
                        break

        return api_data

    async def recrawl_by_ids(self, missing_data: Dict[str, Any], db_session) -> int:
        """辽宁补采 - 直接保存缺失的数据"""
        from ...models.liaoning_drug import LiaoningDrug

        success_count = 0
        for md5_id, drug_info in missing_data.items():
            if self._should_stop():
                break
            try:
                record = LiaoningDrug(
                    md5_id=md5_id,
                    ProductName=drug_info.get('ProductName'),
                    MedicineModelName=drug_info.get('MedicineModelName'),
                    Outlookc=drug_info.get('Outlookc'),
                    HospitalName=drug_info.get('HospitalName'),
                    Pack=drug_info.get('Pack'),
                    GoodsName=drug_info.get('GoodsName'),
                    SubmiTime=drug_info.get('SubmiTime'),
                    collect_time=datetime.now()
                )
                db_session.add(record)
                success_count += 1
                self.logger.info(f"[{self.spider_name}] 补采 md5_id={md5_id[:8]}... 成功")

            except Exception as e:
                self.logger.error(f"[{self.spider_name}] 补采 md5_id={md5_id[:8]}... 失败: {e}")

            await self._delay()

        db_session.commit()
        return success_count

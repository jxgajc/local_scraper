"""
宁夏省补充采集适配器
"""
import json
import hashlib
import aiohttp
from datetime import datetime
from typing import Dict, Any

from ..base_adapter import BaseRecrawlAdapter
from ..registry import register_adapter


@register_adapter('ningxia_drug_store')
class NingxiaRecrawlAdapter(BaseRecrawlAdapter):
    """宁夏省补充采集适配器 - POST Form"""

    spider_name = 'ningxia_drug_store'
    table_name = 'drug_hospital_ningxia_test'
    unique_id = 'procurecatalogId'

    list_api_url = "https://nxyp.ylbz.nx.gov.cn/cms/recentPurchaseDetail/getRecentPurchaseDetailData.html"
    hospital_api_url = "https://nxyp.ylbz.nx.gov.cn/cms/recentPurchaseDetail/getDrugDetailDate.html"

    async def fetch_all_ids(self) -> Dict[str, Any]:
        """从官网API获取所有 procurecatalogId"""
        api_data = {}
        current = 1
        page_size = 100
        headers = {**self.default_headers, 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'}

        async with aiohttp.ClientSession(headers=headers) as session:
            while True:
                if self._should_stop():
                    break
                try:
                    form_data = {
                        "_search": "false", "page": str(current),
                        "rows": str(page_size), "sidx": "", "sord": "asc"
                    }
                    async with session.post(self.list_api_url, data=form_data, timeout=30) as resp:
                        res_json = await resp.json()

                    total_pages = int(res_json.get("total", 0))
                    current_page = int(res_json.get("page", 1))
                    rows = res_json.get("rows", [])

                    for record in rows:
                        procure_id = str(record.get('procurecatalogId', ''))
                        if procure_id:
                            api_data[procure_id] = record

                    self.logger.info(f"[{self.spider_name}] 第{current_page}/{total_pages}页，获取{len(rows)}条")

                    if current_page >= total_pages:
                        break
                    current += 1
                    await self._delay()

                except Exception as e:
                    self.logger.error(f"[{self.spider_name}] 请求API失败: {e}")
                    break

        return api_data

    async def recrawl_by_ids(self, missing_data: Dict[str, Any], db_session) -> int:
        """根据缺失的 procurecatalogId 调用医院API进行补采"""
        from ...models.ningxia_drug import NingxiaDrug

        success_count = 0
        headers = {**self.default_headers, 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'}

        async with aiohttp.ClientSession(headers=headers) as session:
            for procure_id, drug_info in missing_data.items():
                if self._should_stop():
                    break
                try:
                    detail_payload = {
                        "procurecatalogId": procure_id,
                        "_search": "false", "rows": "100", "page": "1", "sidx": "", "sord": "asc"
                    }
                    async with session.post(self.hospital_api_url, data=detail_payload, timeout=30) as resp:
                        res_json = await resp.json()

                    hospitals = res_json.get("rows", [])

                    if hospitals:
                        for hosp in hospitals:
                            record = NingxiaDrug(
                                procurecatalogId=procure_id,
                                productName=drug_info.get('productName'),
                                dosformName=drug_info.get('dosformName'),
                                specName=drug_info.get('specName'),
                                prodentpName=drug_info.get('prodentpName'),
                                hospitalName=hosp.get('hospitalName'),
                                areaName=hosp.get('areaName'),
                                collect_time=datetime.now()
                            )
                            field_values = {'procurecatalogId': procure_id, 'hospitalName': hosp.get('hospitalName')}
                            record.md5_id = hashlib.md5(
                                json.dumps(field_values, sort_keys=True, ensure_ascii=False).encode()
                            ).hexdigest()
                            self._persist_record(db_session, NingxiaDrug, record, procure_id)
                    else:
                        record = NingxiaDrug(
                            procurecatalogId=procure_id,
                            productName=drug_info.get('productName'),
                            collect_time=datetime.now()
                        )
                        record.md5_id = hashlib.md5(procure_id.encode()).hexdigest()
                        self._persist_record(db_session, NingxiaDrug, record, procure_id)

                    success_count += 1
                    self.logger.info(f"[{self.spider_name}] 补采 procurecatalogId={procure_id} 成功")

                except Exception as e:
                    self.logger.error(f"[{self.spider_name}] 补采 procurecatalogId={procure_id} 失败: {e}")

                await self._delay()

        db_session.commit()
        return success_count

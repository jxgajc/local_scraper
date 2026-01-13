"""
宁夏省补充采集适配器
"""
import json
import hashlib
from datetime import datetime
from typing import Dict, Any

from ..base_adapter import BaseRecrawlAdapter
from ..registry import register_adapter


@register_adapter('ningxia_drug_store')
class NingxiaRecrawlAdapter(BaseRecrawlAdapter):
    """
    宁夏省补充采集适配器

    特点：
    - POST Form 请求
    - 两层数据：药品列表 + 医院详情
    """

    spider_name = 'ningxia_drug_store'
    table_name = 'drug_hospital_ningxia_test'
    unique_id = 'procurecatalogId'

    list_api_url = "https://nxyp.ylbz.nx.gov.cn/cms/recentPurchaseDetail/getRecentPurchaseDetailData.html"
    hospital_api_url = "https://nxyp.ylbz.nx.gov.cn/cms/recentPurchaseDetail/getDrugDetailDate.html"

    def _create_session(self):
        session = super()._create_session()
        session.headers.update({
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        })
        return session

    def fetch_all_ids(self) -> Dict[str, Any]:
        """从官网API获取所有 procurecatalogId 及其基础信息"""
        api_data = {}
        current = 1
        page_size = 100

        while True:
            if self._should_stop():
                break

            try:
                form_data = {
                    "_search": "false", "page": str(current),
                    "rows": str(page_size), "sidx": "", "sord": "asc"
                }
                response = self.session.post(self.list_api_url, data=form_data, timeout=30)
                response.raise_for_status()
                res_json = response.json()

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
                self._delay()

            except Exception as e:
                self.logger.error(f"[{self.spider_name}] 请求API失败: {e}")
                break

        return api_data

    def recrawl_by_ids(self, missing_data: Dict[str, Any], db_session) -> int:
        """根据缺失的 procurecatalogId 调用医院API进行补采"""
        from ...models.ningxia_drug import NingxiaDrug

        success_count = 0
        for procure_id, drug_info in missing_data.items():
            if self._should_stop():
                break

            try:
                detail_payload = {
                    "procurecatalogId": procure_id,
                    "_search": "false", "rows": "100", "page": "1", "sidx": "", "sord": "asc"
                }

                resp = self.session.post(self.hospital_api_url, data=detail_payload, timeout=30)
                resp.raise_for_status()
                res_json = resp.json()

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
                        db_session.add(record)
                else:
                    record = NingxiaDrug(
                        procurecatalogId=procure_id,
                        productName=drug_info.get('productName'),
                        collect_time=datetime.now()
                    )
                    record.md5_id = hashlib.md5(procure_id.encode()).hexdigest()
                    db_session.add(record)

                success_count += 1
                self.logger.info(f"[{self.spider_name}] 补采 procurecatalogId={procure_id} 成功")

            except Exception as e:
                self.logger.error(f"[{self.spider_name}] 补采 procurecatalogId={procure_id} 失败: {e}")

            self._delay()

        db_session.commit()
        return success_count

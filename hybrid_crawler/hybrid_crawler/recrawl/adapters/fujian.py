"""
福建省补充采集适配器
"""
import json
import hashlib
import aiohttp
from datetime import datetime
from typing import Dict, Any

from ..base_adapter import BaseRecrawlAdapter
from ..registry import register_adapter


@register_adapter('fujian_drug_spider')
class FujianRecrawlAdapter(BaseRecrawlAdapter):
    """福建省补充采集适配器 - POST JSON"""

    spider_name = 'fujian_drug_spider'
    table_name = 'drug_hospital_fujian_test'
    unique_id = 'ext_code'

    list_api_url = "https://open.ybj.fujian.gov.cn:10013/tps-local/web/tender/plus/item-cfg-info/list"
    hospital_api_url = "https://open.ybj.fujian.gov.cn:10013/tps-local/web/trans/api/open/v2/queryHospital"

    async def fetch_all_ids(self) -> Dict[str, Any]:
        """从官网API获取所有 ext_code"""
        api_data = {}
        current = 1
        page_size = 1000
        headers = {**self.default_headers, 'Content-Type': 'application/json;charset=utf-8'}

        async with aiohttp.ClientSession(headers=headers) as session:
            while True:
                if self._should_stop():
                    break
                try:
                    payload = {
                        "druglistName": "", "druglistCode": "", "drugName": "",
                        "ruteName": "", "dosformName": "", "specName": "",
                        "pac": "", "prodentpName": "", "current": current,
                        "size": page_size, "tenditmType": ""
                    }
                    async with session.post(self.list_api_url, json=payload, timeout=30) as resp:
                        res_json = await resp.json()

                    if res_json.get("code") != 0:
                        break

                    data_block = res_json.get("data", {})
                    records = data_block.get("records", [])
                    total_pages = data_block.get("pages", 0)

                    for record in records:
                        ext_code = record.get('extCode')
                        if ext_code:
                            api_data[ext_code] = {
                                'ext_code': ext_code,
                                'drug_list_code': record.get('druglistCode'),
                                'drug_name': record.get('drugName'),
                                'drug_list_name': record.get('druglistName'),
                                'dosform': record.get('dosformName'),
                                'spec': record.get('specName'),
                                'pac': record.get('pac'),
                                'rute_name': record.get('ruteName'),
                                'prod_entp': record.get('prodentpName'),
                                'source_data': json.dumps(record, ensure_ascii=False)
                            }

                    self.logger.info(f"[{self.spider_name}] 第{current}/{total_pages}页，获取{len(records)}条")

                    if current >= total_pages:
                        break
                    current += 1
                    await self._delay()

                except Exception as e:
                    self.logger.error(f"[{self.spider_name}] 请求API失败: {e}")
                    break

        return api_data

    async def recrawl_by_ids(self, missing_data: Dict[str, Any], db_session) -> int:
        """根据缺失的 ext_code 调用医院API进行补采"""
        from ...models.fujian_drug import FujianDrug

        success_count = 0
        headers = {**self.default_headers, 'Content-Type': 'application/json;charset=utf-8'}

        async with aiohttp.ClientSession(headers=headers) as session:
            for ext_code, base_info in missing_data.items():
                if self._should_stop():
                    break
                try:
                    hospital_payload = {
                        "area": "", "hospitalName": "", "pageNo": 1,
                        "pageSize": 100, "productId": ext_code, "tenditmType": ""
                    }
                    async with session.post(self.hospital_api_url, json=hospital_payload, timeout=30) as resp:
                        res_json = await resp.json()

                    inner_data_str = res_json.get("data")
                    if inner_data_str and isinstance(inner_data_str, str):
                        inner_json = json.loads(inner_data_str)
                        hospitals = inner_json.get("data", [])

                        if hospitals:
                            for hosp in hospitals:
                                record = FujianDrug(
                                    ext_code=base_info.get('ext_code'),
                                    drug_list_code=base_info.get('drug_list_code'),
                                    drug_name=base_info.get('drug_name'),
                                    drug_list_name=base_info.get('drug_list_name'),
                                    dosform=base_info.get('dosform'),
                                    spec=base_info.get('spec'),
                                    pac=base_info.get('pac'),
                                    rute_name=base_info.get('rute_name'),
                                    prod_entp=base_info.get('prod_entp'),
                                    source_data=base_info.get('source_data'),
                                    has_hospital_record=True,
                                    hospital_name=hosp.get('hospitalName'),
                                    medins_code=hosp.get('medinsCode'),
                                    area_name=hosp.get('areaName'),
                                    area_code=hosp.get('areaCode'),
                                    collect_time=datetime.now()
                                )
                                field_values = {
                                    'ext_code': ext_code,
                                    'hospital_name': hosp.get('hospitalName'),
                                    'medins_code': hosp.get('medinsCode'),
                                }
                                record.md5_id = hashlib.md5(
                                    json.dumps(field_values, sort_keys=True, ensure_ascii=False).encode()
                                ).hexdigest()

                                self._persist_record(db_session, FujianDrug, record, ext_code)
                        else:
                            record = FujianDrug(
                                ext_code=base_info.get('ext_code'),
                                drug_list_code=base_info.get('drug_list_code'),
                                drug_name=base_info.get('drug_name'),
                                drug_list_name=base_info.get('drug_list_name'),
                                dosform=base_info.get('dosform'),
                                spec=base_info.get('spec'),
                                pac=base_info.get('pac'),
                                rute_name=base_info.get('rute_name'),
                                prod_entp=base_info.get('prod_entp'),
                                source_data=base_info.get('source_data'),
                                has_hospital_record=False,
                                collect_time=datetime.now()
                            )
                            record.md5_id = hashlib.md5(ext_code.encode()).hexdigest()

                            self._persist_record(db_session, FujianDrug, record, ext_code)

                    success_count += 1
                    self.logger.info(f"[{self.spider_name}] 补采 ext_code={ext_code} 成功")

                except Exception as e:
                    self.logger.error(f"[{self.spider_name}] 补采 ext_code={ext_code} 失败: {e}")

                await self._delay()

        db_session.commit()
        return success_count

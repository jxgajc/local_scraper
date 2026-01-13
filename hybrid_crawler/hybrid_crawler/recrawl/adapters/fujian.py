"""
福建省补充采集适配器
"""
import json
import hashlib
import time
from datetime import datetime
from typing import Dict, Any

from ..base_adapter import BaseRecrawlAdapter
from ..registry import register_adapter


@register_adapter('fujian_drug_spider')
class FujianRecrawlAdapter(BaseRecrawlAdapter):
    """
    福建省补充采集适配器

    特点：
    - POST JSON 请求
    - 两层数据：药品列表 + 医院详情
    """

    spider_name = 'fujian_drug_spider'
    table_name = 'drug_hospital_fujian_test'
    unique_id = 'ext_code'

    list_api_url = "https://open.ybj.fujian.gov.cn:10013/tps-local/web/tender/plus/item-cfg-info/list"
    hospital_api_url = "https://open.ybj.fujian.gov.cn:10013/tps-local/web/trans/api/open/v2/queryHospital"

    def _create_session(self):
        session = super()._create_session()
        session.headers.update({
            'Content-Type': 'application/json;charset=utf-8',
        })
        return session

    def fetch_all_ids(self) -> Dict[str, Any]:
        """
        从官网API获取所有 ext_code 及其基础信息

        Returns:
            {ext_code: base_info} 字典
        """
        api_data = {}
        current = 1
        page_size = 1000

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

                response = self.session.post(self.list_api_url, json=payload, timeout=30)
                response.raise_for_status()
                res_json = response.json()

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
                self._delay()

            except Exception as e:
                self.logger.error(f"[{self.spider_name}] 请求API失败: {e}")
                break

        return api_data

    def recrawl_by_ids(self, missing_data: Dict[str, Any], db_session) -> int:
        """
        根据缺失的 ext_code 调用医院API进行补采
        """
        from ...models.fujian_drug import FujianDrug

        success_count = 0
        for ext_code, base_info in missing_data.items():
            if self._should_stop():
                break

            try:
                hospital_payload = {
                    "area": "", "hospitalName": "", "pageNo": 1,
                    "pageSize": 100, "productId": ext_code, "tenditmType": ""
                }

                resp = self.session.post(self.hospital_api_url, json=hospital_payload, timeout=30)
                resp.raise_for_status()
                res_json = resp.json()

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
                            # 生成 md5_id
                            field_values = {
                                'ext_code': ext_code,
                                'hospital_name': hosp.get('hospitalName'),
                                'medins_code': hosp.get('medinsCode'),
                            }
                            record.md5_id = hashlib.md5(
                                json.dumps(field_values, sort_keys=True, ensure_ascii=False).encode()
                            ).hexdigest()
                            db_session.add(record)
                    else:
                        # 无医院记录
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
                        db_session.add(record)

                success_count += 1
                self.logger.info(f"[{self.spider_name}] 补采 ext_code={ext_code} 成功")

            except Exception as e:
                self.logger.error(f"[{self.spider_name}] 补采 ext_code={ext_code} 失败: {e}")

            self._delay()

        db_session.commit()
        return success_count

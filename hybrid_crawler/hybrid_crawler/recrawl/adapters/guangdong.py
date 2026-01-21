"""
广东省补充采集适配器
"""
import json
import hashlib
import aiohttp
from datetime import datetime
from typing import Dict, Any

from ..base_adapter import BaseRecrawlAdapter
from ..registry import register_adapter


@register_adapter('guangdong_drug_spider')
class GuangdongRecrawlAdapter(BaseRecrawlAdapter):
    """广东省补充采集适配器 - POST JSON"""

    spider_name = 'guangdong_drug_spider'
    table_name = 'drug_hospital_guangdong_test'
    unique_id = 'drug_code'

    list_api_url = "https://igi.hsa.gd.gov.cn/tps_local_bd/web/publicity/pubonlnPublicity/queryPubonlnPage"
    hospital_api_url = "https://igi.hsa.gd.gov.cn/tps_local_bd/web/publicity/pubonlnPublicity/getPurcHospitalInfoListNew"

    async def fetch_all_ids(self) -> Dict[str, Any]:
        """从官网API获取所有 drug_code"""
        api_data = {}
        current = 1
        page_size = 500
        headers = {**self.default_headers, 'Content-Type': 'application/json'}

        async with aiohttp.ClientSession(headers=headers) as session:
            while True:
                if self._should_stop():
                    break
                try:
                    payload = {"current": current, "size": page_size, "searchCount": True}
                    async with session.post(self.list_api_url, json=payload, timeout=30) as resp:
                        res_json = await resp.json()

                    data_block = res_json.get("data", {})
                    records = data_block.get("records", [])
                    total_pages = data_block.get("pages", 0)

                    for record in records:
                        drug_code = record.get('drugCode')
                        if drug_code:
                            api_data[drug_code] = {
                                'drug_id': record.get('drugId'),
                                'drug_code': drug_code,
                                'gen_name': record.get('genname'),
                                'trade_name': record.get('tradeName'),
                                'dosform_name': record.get('dosformName'),
                                'spec_name': record.get('specName'),
                                'prod_entp_name': record.get('prodentpName'),
                                'price': record.get('minPacPubonlnPric'),
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
        """根据缺失的 drug_code 调用医院API进行补采"""
        from ...models.guangdong_drug import GuangdongDrug

        success_count = 0
        headers = {**self.default_headers, 'Content-Type': 'application/json'}

        async with aiohttp.ClientSession(headers=headers) as session:
            for drug_code, base_info in missing_data.items():
                if self._should_stop():
                    break
                try:
                    hospital_payload = {
                        "current": 1, "size": 50, "searchCount": True, "drugCode": drug_code
                    }
                    async with session.post(self.hospital_api_url, json=hospital_payload, timeout=30) as resp:
                        res_json = await resp.json()

                    data = res_json.get("data", {})
                    hospitals = data.get("records", [])

                    if hospitals:
                        # 针对 update_only 模式的优化：避免在一对多关系中重复执行全量更新
                        if self.update_only:
                            updated = self._touch_by_unique_id(db_session, GuangdongDrug, drug_code)
                            if updated > 0:
                                self.logger.info(f"[{self.spider_name}] 批量更新 drug_code={drug_code} 完成，共 {updated} 条")
                            
                            # 提交事务
                            db_session.commit()
                            success_count += 1
                            await self._delay()
                            continue

                        for hosp in hospitals:
                            record = GuangdongDrug(
                                drug_id=base_info.get('drug_id'),
                                drug_code=base_info.get('drug_code'),
                                gen_name=base_info.get('gen_name'),
                                trade_name=base_info.get('trade_name'),
                                dosform_name=base_info.get('dosform_name'),
                                spec_name=base_info.get('spec_name'),
                                prod_entp_name=base_info.get('prod_entp_name'),
                                price=base_info.get('price'),
                                source_data=base_info.get('source_data'),
                                has_hospital_record=True,
                                medins_code=hosp.get('medinsCode'),
                                medins_name=hosp.get('medinsName'),
                                hosp_type=hosp.get('type'),
                                admdvs_name=hosp.get('admdvsName'),
                                collect_time=datetime.now()
                            )
                            field_values = {'drug_code': drug_code, 'medins_code': hosp.get('medinsCode')}
                            record.md5_id = hashlib.md5(
                                json.dumps(field_values, sort_keys=True, ensure_ascii=False).encode()
                            ).hexdigest()
                            self._persist_record(db_session, GuangdongDrug, record, drug_code)
                    else:
                        record = GuangdongDrug(
                            drug_id=base_info.get('drug_id'),
                            drug_code=base_info.get('drug_code'),
                            gen_name=base_info.get('gen_name'),
                            trade_name=base_info.get('trade_name'),
                            dosform_name=base_info.get('dosform_name'),
                            spec_name=base_info.get('spec_name'),
                            prod_entp_name=base_info.get('prod_entp_name'),
                            price=base_info.get('price'),
                            source_data=base_info.get('source_data'),
                            has_hospital_record=False,
                            collect_time=datetime.now()
                        )
                        record.md5_id = hashlib.md5(drug_code.encode()).hexdigest()
                        self._persist_record(db_session, GuangdongDrug, record, drug_code)

                    success_count += 1
                    self.logger.info(f"[{self.spider_name}] 补采 drug_code={drug_code} 成功")

                except Exception as e:
                    self.logger.error(f"[{self.spider_name}] 补采 drug_code={drug_code} 失败: {e}")

                await self._delay()

        db_session.commit()
        return success_count

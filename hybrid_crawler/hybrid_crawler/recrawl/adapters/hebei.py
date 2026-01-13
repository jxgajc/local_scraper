"""
河北省补充采集适配器
"""
import json
import hashlib
import aiohttp
from datetime import datetime
from typing import Dict, Any

from ..base_adapter import BaseRecrawlAdapter
from ..registry import register_adapter


@register_adapter('hebei_drug_spider')
class HebeiRecrawlAdapter(BaseRecrawlAdapter):
    """河北省补充采集适配器 - GET请求"""

    spider_name = 'hebei_drug_spider'
    table_name = 'drug_hospital_hebei_test'
    unique_id = 'prodCode'

    list_api_url = "https://ylbzj.hebei.gov.cn/templates/default_pc/syyypqxjzcg/queryPubonlnDrudInfoList"
    hospital_api_url = "https://ylbzj.hebei.gov.cn/templates/default_pc/syyypqxjzcg/queryProcurementMedinsList"

    async def fetch_all_ids(self) -> Dict[str, Any]:
        """从官网API获取所有 prodCode"""
        api_data = {}
        current = 1
        page_size = 1000
        headers = {**self.default_headers, 'Accept': '*/*', 'prodType': '2'}

        async with aiohttp.ClientSession(headers=headers) as session:
            while True:
                if self._should_stop():
                    break
                try:
                    params = {"pageNo": current, "pageSize": page_size, "prodName": "", "prodentpName": ""}
                    async with session.get(self.list_api_url, params=params, timeout=30) as resp:
                        res_json = await resp.json()

                    data_block = res_json.get("data", {})
                    records = data_block.get("list", [])
                    total_pages = int(data_block.get("pages", 0))
                    current_page = int(data_block.get("pageNo", 1))

                    for record in records:
                        prod_code = record.get('prodCode')
                        if prod_code:
                            api_data[prod_code] = record

                    self.logger.info(f"[{self.spider_name}] 第{current_page}/{total_pages}页，获取{len(records)}条")

                    if current_page >= total_pages:
                        break
                    current += 1
                    await self._delay()

                except Exception as e:
                    self.logger.error(f"[{self.spider_name}] 请求API失败: {e}")
                    break

        return api_data

    async def recrawl_by_ids(self, missing_data: Dict[str, Any], db_session) -> int:
        """根据缺失的 prodCode 调用医院API进行补采"""
        from ...models.hebei_drug import HebeiDrug

        success_count = 0
        headers = {**self.default_headers, 'Accept': '*/*', 'prodType': '2'}

        async with aiohttp.ClientSession(headers=headers) as session:
            for prod_code, drug_info in missing_data.items():
                if self._should_stop():
                    break
                try:
                    prodentp_code = drug_info.get("prodentpCode")
                    if not prodentp_code:
                        self.logger.warning(f"[{self.spider_name}] prodCode={prod_code} 缺少 prodentpCode，跳过")
                        continue

                    params = {
                        "pageNo": 1, "pageSize": 1000,
                        "prodCode": prod_code, "prodEntpCode": prodentp_code, "isPublicHospitals": ""
                    }
                    async with session.get(self.hospital_api_url, params=params, timeout=30) as resp:
                        res_json = await resp.json()

                    hospital_list = res_json.get("list", []) or []

                    record = HebeiDrug(
                        prodCode=prod_code,
                        prodName=drug_info.get('prodName'),
                        prodentpName=drug_info.get('prodentpName'),
                        prodentpCode=prodentp_code,
                        hospital_purchases=hospital_list,
                        collect_time=datetime.now()
                    )
                    record.md5_id = hashlib.md5(prod_code.encode()).hexdigest()
                    db_session.add(record)

                    success_count += 1
                    self.logger.info(f"[{self.spider_name}] 补采 prodCode={prod_code} 成功，医院数: {len(hospital_list)}")

                except Exception as e:
                    self.logger.error(f"[{self.spider_name}] 补采 prodCode={prod_code} 失败: {e}")

                await self._delay()

        db_session.commit()
        return success_count

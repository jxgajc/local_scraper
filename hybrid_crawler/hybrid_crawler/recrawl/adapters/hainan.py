"""
海南省补充采集适配器
"""
import json
import hashlib
from datetime import datetime
from typing import Dict, Any

from ..base_adapter import BaseRecrawlAdapter
from ..registry import register_adapter


@register_adapter('hainan_drug_spider')
class HainanRecrawlAdapter(BaseRecrawlAdapter):
    """
    海南省补充采集适配器

    特点：
    - GET 请求
    - 两层数据：药品列表 + 门店详情
    """

    spider_name = 'hainan_drug_spider'
    table_name = 'drug_shop_hainan_test'
    unique_id = 'drug_code'

    list_api_url = "https://ybj.hainan.gov.cn/tps-local/local/web/std/drugStore/getDrugStore"
    detail_api_url = "https://ybj.hainan.gov.cn/tps-local/local/web/std/drugStore/getDrugStoreDetl"

    def fetch_all_ids(self) -> Dict[str, Any]:
        """从官网API获取所有 drug_code 及其基础信息"""
        api_data = {}
        current = 1
        page_size = 500

        while True:
            if self._should_stop():
                break

            try:
                params = {"current": current, "size": page_size, "prodName": ""}
                response = self.session.get(self.list_api_url, params=params, timeout=30)
                response.raise_for_status()
                res_json = response.json()

                data_block = res_json.get("data", {})
                records = data_block.get("records", [])
                total_pages = data_block.get("pages", 0)

                for record in records:
                    drug_code = record.get('prodCode')
                    if drug_code:
                        api_data[drug_code] = {
                            'drug_code': drug_code,
                            'prod_name': record.get('prodName'),
                            'dosform': record.get('dosform'),
                            'spec': record.get('prodSpec'),
                            'pac': record.get('prodPac'),
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
        """根据缺失的 drug_code 调用门店API进行补采"""
        from ...models.hainan_drug import HainanDrug

        success_count = 0
        for drug_code, base_info in missing_data.items():
            if self._should_stop():
                break

            try:
                params = {"current": 1, "size": 20, "drugCode": drug_code}
                resp = self.session.get(self.detail_api_url, params=params, timeout=30)
                resp.raise_for_status()
                res_json = resp.json()

                data = res_json.get("data", {})
                shops = data.get("records", [])

                if shops:
                    for shop in shops:
                        record = HainanDrug(
                            drug_code=base_info.get('drug_code'),
                            prod_name=base_info.get('prod_name'),
                            dosform=base_info.get('dosform'),
                            spec=base_info.get('spec'),
                            pac=base_info.get('pac'),
                            prod_entp=base_info.get('prod_entp'),
                            source_data=base_info.get('source_data'),
                            has_shop_record=True,
                            shop_name=shop.get('medinsName'),
                            shop_code=shop.get('medinsCode'),
                            price=shop.get('pric'),
                            inventory=shop.get('invCnt'),
                            collect_time=datetime.now()
                        )
                        field_values = {'drug_code': drug_code, 'shop_code': shop.get('medinsCode')}
                        record.md5_id = hashlib.md5(
                            json.dumps(field_values, sort_keys=True, ensure_ascii=False).encode()
                        ).hexdigest()
                        db_session.add(record)
                else:
                    record = HainanDrug(
                        drug_code=base_info.get('drug_code'),
                        prod_name=base_info.get('prod_name'),
                        dosform=base_info.get('dosform'),
                        spec=base_info.get('spec'),
                        pac=base_info.get('pac'),
                        prod_entp=base_info.get('prod_entp'),
                        source_data=base_info.get('source_data'),
                        has_shop_record=False,
                        collect_time=datetime.now()
                    )
                    record.md5_id = hashlib.md5(drug_code.encode()).hexdigest()
                    db_session.add(record)

                success_count += 1
                self.logger.info(f"[{self.spider_name}] 补采 drug_code={drug_code} 成功")

            except Exception as e:
                self.logger.error(f"[{self.spider_name}] 补采 drug_code={drug_code} 失败: {e}")

            self._delay()

        db_session.commit()
        return success_count

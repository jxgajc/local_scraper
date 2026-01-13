"""
天津市补充采集适配器
"""
import json
import hashlib
import random
import string
import time
import os
import pandas as pd
from datetime import datetime
from typing import Dict, Any

from ..base_adapter import BaseRecrawlAdapter
from ..registry import register_adapter

# 获取关键词文件路径
script_dir = os.path.dirname(os.path.abspath(__file__))
excel_path = os.path.join(script_dir, "../../../关键字采集(2).xlsx")


@register_adapter('tianjin_drug_spider')
class TianjinRecrawlAdapter(BaseRecrawlAdapter):
    """
    天津市补充采集适配器

    特点：
    - POST JSON 请求
    - 需要验证码
    - 基于关键词的采集
    - 两层数据：药品列表 + 医院详情
    """

    spider_name = 'tianjin_drug_spider'
    table_name = 'drug_hospital_tianjin_test'
    unique_id = 'med_id'

    drug_list_url = "https://tps.ylbz.tj.gov.cn/csb/1.0.0/guideGetMedList"
    hospital_list_url = "https://tps.ylbz.tj.gov.cn/csb/1.0.0/guideGetHosp"

    def _create_session(self):
        session = super()._create_session()
        session.headers.update({
            'Content-Type': 'application/json',
        })
        return session

    @staticmethod
    def _get_verification_code():
        """生成4位随机字母数字混合验证码"""
        chars = string.ascii_lowercase + string.digits
        return ''.join(random.choices(chars, k=4))

    def _load_keywords(self):
        """加载关键词列表"""
        try:
            df = pd.read_excel(excel_path)
            return df.loc[:, "采集关键字"].to_list()
        except Exception as e:
            self.logger.error(f"[{self.spider_name}] 关键词文件加载失败: {e}")
            return []

    def fetch_all_ids(self) -> Dict[str, Any]:
        """从官网API获取所有 med_id 及其基础信息"""
        api_data = {}
        keywords = self._load_keywords()

        if not keywords:
            return api_data

        for keyword in keywords:
            if self._should_stop():
                break

            try:
                payload = {
                    "verificationCode": self._get_verification_code(),
                    "content": keyword
                }
                response = self.session.post(self.drug_list_url, json=payload, timeout=30)
                response.raise_for_status()
                res_json = response.json()

                if res_json.get("code") != 200:
                    continue

                data = res_json.get("data", {})
                drug_list = data.get("list", [])

                for drug in drug_list:
                    med_id = drug.get('medid')
                    if med_id:
                        api_data[med_id] = {
                            'med_id': med_id,
                            'gen_name': drug.get('genname'),
                            'prod_name': drug.get('prodname'),
                            'dosform': drug.get('dosform'),
                            'spec': drug.get('spec'),
                            'pac': drug.get('pac'),
                            'prod_entp': drug.get('prodentp'),
                            'source_data': json.dumps(drug, ensure_ascii=False)
                        }

                self.logger.info(f"[{self.spider_name}] 关键词[{keyword}] 获取{len(drug_list)}条")

            except Exception as e:
                self.logger.error(f"[{self.spider_name}] 请求API失败: {e}")

            self._delay()

        return api_data

    def recrawl_by_ids(self, missing_data: Dict[str, Any], db_session) -> int:
        """根据缺失的 med_id 调用医院API进行补采"""
        from ...models.tianjin_drug import TianjinDrug

        success_count = 0
        for med_id, base_info in missing_data.items():
            if self._should_stop():
                break

            try:
                hospital_payload = {
                    "verificationCode": self._get_verification_code(),
                    "genname": base_info.get('gen_name'),
                    "dosform": base_info.get('dosform'),
                    "spec": base_info.get('spec'),
                    "pac": base_info.get('pac')
                }

                resp = self.session.post(self.hospital_list_url, json=hospital_payload, timeout=30)
                resp.raise_for_status()
                res_json = resp.json()

                if res_json.get("code") != 200:
                    continue

                data = res_json.get("data", {})
                hosp_list = data.get("list", [])

                if hosp_list:
                    for hosp in hosp_list:
                        record = TianjinDrug(
                            med_id=base_info.get('med_id'),
                            gen_name=base_info.get('gen_name'),
                            prod_name=base_info.get('prod_name'),
                            dosform=base_info.get('dosform'),
                            spec=base_info.get('spec'),
                            pac=base_info.get('pac'),
                            prod_entp=base_info.get('prod_entp'),
                            source_data=base_info.get('source_data'),
                            has_hospital_record=True,
                            hs_name=hosp.get('hsname'),
                            hs_lav=hosp.get('hslav'),
                            got_time=hosp.get('gottime'),
                            collect_time=datetime.now()
                        )
                        field_values = {'med_id': med_id, 'hs_name': hosp.get('hsname')}
                        record.md5_id = hashlib.md5(
                            json.dumps(field_values, sort_keys=True, ensure_ascii=False).encode()
                        ).hexdigest()
                        db_session.add(record)
                else:
                    record = TianjinDrug(
                        med_id=base_info.get('med_id'),
                        gen_name=base_info.get('gen_name'),
                        prod_name=base_info.get('prod_name'),
                        dosform=base_info.get('dosform'),
                        spec=base_info.get('spec'),
                        pac=base_info.get('pac'),
                        prod_entp=base_info.get('prod_entp'),
                        source_data=base_info.get('source_data'),
                        has_hospital_record=False,
                        collect_time=datetime.now()
                    )
                    record.md5_id = hashlib.md5(med_id.encode()).hexdigest()
                    db_session.add(record)

                success_count += 1
                self.logger.info(f"[{self.spider_name}] 补采 med_id={med_id} 成功")

            except Exception as e:
                self.logger.error(f"[{self.spider_name}] 补采 med_id={med_id} 失败: {e}")

            self._delay()

        db_session.commit()
        return success_count

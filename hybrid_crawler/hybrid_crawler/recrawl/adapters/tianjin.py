"""
天津市补充采集适配器
"""
import json
import hashlib
import random
import string
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


@register_adapter('tianjin_drug_spider')
class TianjinRecrawlAdapter(BaseRecrawlAdapter):
    """天津市补充采集适配器 - POST JSON + 验证码"""

    spider_name = 'tianjin_drug_spider'
    table_name = 'drug_hospital_tianjin_test'
    unique_id = 'med_id'

    drug_list_url = "https://tps.ylbz.tj.gov.cn/csb/1.0.0/guideGetMedList"
    hospital_list_url = "https://tps.ylbz.tj.gov.cn/csb/1.0.0/guideGetHosp"

    @staticmethod
    def _get_verification_code():
        """生成4位随机字母数字混合验证码"""
        chars = string.ascii_lowercase + string.digits
        return ''.join(random.choices(chars, k=4))

    def _load_keywords(self):
        """加载关键词列表"""
        from scrapy.utils.project import get_project_settings
        settings = get_project_settings()
        path = settings.get('KEYWORD_FILE_PATH', '关键字采集(2).xlsx')
        if not os.path.isabs(path):
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
            path = os.path.join(base_dir, path)
            
        try:
            df = pd.read_excel(path)
            return df.loc[:, "采集关键字"].to_list()
        except Exception as e:
            self.logger.error(f"[{self.spider_name}] 关键词文件加载失败: {e} (Path: {path})")
            return []

    async def fetch_all_ids(self) -> Dict[str, Any]:
        """从官网API获取所有 med_id"""
        api_data = {}
        keywords = self._load_keywords()

        if not keywords:
            return api_data

        headers = {**self.default_headers, 'Content-Type': 'application/json'}

        async with aiohttp.ClientSession(headers=headers) as session:
            for keyword in keywords:
                if self._should_stop():
                    break
                try:
                    payload = {
                        "verificationCode": self._get_verification_code(),
                        "content": keyword
                    }
                    async with session.post(self.drug_list_url, json=payload, timeout=30) as resp:
                        res_json = await resp.json()

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

                await self._delay()

        return api_data

    async def recrawl_by_ids(self, missing_data: Dict[str, Any], db_session) -> int:
        """根据缺失的 med_id 调用医院API进行补采"""
        from ...models.tianjin_drug import TianjinDrug, TianjinDrugItem

        success_count = 0
        headers = {**self.default_headers, 'Content-Type': 'application/json'}

        async with aiohttp.ClientSession(headers=headers) as session:
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
                    async with session.post(self.hospital_list_url, json=hospital_payload, timeout=30) as resp:
                        res_json = await resp.json()

                    if res_json.get("code") != 200:
                        continue

                    data = res_json.get("data", {})
                    hosp_list = data.get("list", [])

                    if hosp_list:
                        # 针对 update_only 模式的优化：避免在一对多关系中重复执行全量更新
                        if self.update_only:
                            updated = self._touch_by_unique_id(db_session, TianjinDrug, med_id)
                            if updated > 0:
                                self.logger.info(f"[{self.spider_name}] 批量更新 med_id={med_id} 完成，共 {updated} 条")
                            
                            # 提交事务
                            db_session.commit()
                            success_count += 1
                            await self._delay()
                            continue

                        for hosp in hosp_list:
                            # 构造Item以生成MD5
                            item = TianjinDrugItem()
                            # 仅更新Item中定义的字段
                            for field in item.fields:
                                if field in base_info:
                                    item[field] = base_info[field]
                                    
                            item['has_hospital_record'] = True
                            item['hs_name'] = hosp.get('hsname')
                            item['hs_lav'] = hosp.get('hslav')
                            item['got_time'] = hosp.get('gottime')
                            item.generate_md5_id()
                            
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
                                collect_time=datetime.now(),
                                md5_id=item['md5_id']
                            )
                            self._persist_record(db_session, TianjinDrug, record, med_id)
                    else:
                        item = TianjinDrugItem()
                        # 仅更新Item中定义的字段
                        for field in item.fields:
                            if field in base_info:
                                item[field] = base_info[field]
                                
                        item['has_hospital_record'] = False
                        item.generate_md5_id()
                        
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
                            collect_time=datetime.now(),
                            md5_id=item['md5_id']
                        )
                        self._persist_record(db_session, TianjinDrug, record, med_id)

                    success_count += 1
                    self.logger.info(f"[{self.spider_name}] 补采 med_id={med_id} 成功")

                except Exception as e:
                    self.logger.error(f"[{self.spider_name}] 补采 med_id={med_id} 失败: {e}")

                await self._delay()

        db_session.commit()
        return success_count

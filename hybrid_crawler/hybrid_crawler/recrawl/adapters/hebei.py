"""
河北省补充采集适配器
"""
import json
import hashlib
import asyncio
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
        from ...models.hebei_drug import HebeiDrug, HebeiDrugItem

        success_count = 0
        headers = {
            **self.default_headers,
            'Accept': '*/*',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'prodType': '2',
        }

        timeout_cfg = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(headers=headers, timeout=timeout_cfg) as session:
            try:
                list_params = {"pageNo": 1, "pageSize": 1000, "prodName": "", "prodentpName": ""}
                async with session.get(self.list_api_url, params=list_params) as resp:
                    await resp.text()
            except Exception as e:
                self.logger.warning(f"[{self.spider_name}] 初始化列表请求失败: {type(e).__name__} {e}")

            for prod_code, drug_info in missing_data.items():
                if self._should_stop():
                    break
                resp_text = ""
                try:
                    prodentp_code = drug_info.get("prodentpCode")
                    if not prodentp_code:
                        self.logger.warning(f"[{self.spider_name}] prodCode={prod_code} 缺少 prodentpCode，跳过")
                        continue

                    params = {
                        "pageNo": 1, "pageSize": 1000,
                        "prodCode": prod_code, "prodEntpCode": prodentp_code, "isPublicHospitals": ""
                    }
                    res_json = None
                    last_error = None
                    for _ in range(3):
                        try:
                            async with session.get(self.hospital_api_url, params=params) as resp:
                                resp_text = await resp.text()
                                if resp.status != 200:
                                    raise ValueError(f"HTTP {resp.status}: {resp_text[:200]}")
                                try:
                                    res_json = json.loads(resp_text)
                                except json.JSONDecodeError:
                                    raise ValueError(f"JSON解析失败: {resp_text[:200]}")
                                last_error = None
                                break
                        except asyncio.TimeoutError as e:
                            last_error = e
                            await self._delay()
                        except Exception as e:
                            last_error = e
                            break

                    if last_error:
                        raise last_error

                    if not isinstance(res_json, dict):
                        raise ValueError(f"响应非对象: {res_json}")

                    if "list" in res_json:
                        hospital_list = res_json.get("list", []) or []
                    elif isinstance(res_json.get("data"), dict):
                        hospital_list = res_json["data"].get("list", []) or []
                    else:
                        raise ValueError(f"响应结构异常: {res_json}")
                    url = f"{self.hospital_api_url}?pageNo=1&pageSize=1000&prodCode={prod_code}&prodEntpCode={prodentp_code}"

                    # 针对 update_only 模式的优化：避免在一对多关系中重复执行全量更新
                    if self.update_only:
                        updated = self._touch_by_unique_id(db_session, HebeiDrug, prod_code)
                        if updated > 0:
                            self.logger.info(f"[{self.spider_name}] 批量更新 prodCode={prod_code} 完成，共 {updated} 条")
                        
                        # 提交事务
                        db_session.commit()
                        success_count += 1
                        await self._delay()
                        continue

                    if hospital_list:
                        for hosp in hospital_list:
                            item = HebeiDrugItem()
                            for field_name in item.fields:
                                if field_name in ['md5_id', 'collect_time', 'url', 'url_hash', 'hospital_purchases', 'page_num', 'hospital_name', 'hospital_admdvs', 'hospital_shp_cnt', 'hospital_shp_time', 'hospital_is_public']:
                                    continue
                                if field_name in drug_info:
                                    item[field_name] = drug_info[field_name]
                            item['hospital_purchases'] = hosp
                            item['hospital_name'] = hosp.get('prodEntpName') or hosp.get('hospitalName') or hosp.get('medinsName')
                            item['hospital_admdvs'] = hosp.get('prodEntpAdmdvs') or hosp.get('admdvsName')
                            item['hospital_shp_cnt'] = hosp.get('shpCnt')
                            item['hospital_shp_time'] = hosp.get('shpTimeFormat')
                            item['hospital_is_public'] = hosp.get('isPublicHospitals')
                            item['url'] = url
                            item['page_num'] = 1
                            item.generate_md5_id()

                            record_data = {
                                "prodId": drug_info.get("prodId"),
                                "prodCode": prod_code,
                                "prodName": drug_info.get("prodName"),
                                "dosform": drug_info.get("dosform"),
                                "prodSpec": drug_info.get("prodSpec"),
                                "prodPac": drug_info.get("prodPac"),
                                "prodentpName": drug_info.get("prodentpName"),
                                "prodentpCode": prodentp_code,
                                "pubonlnPric": drug_info.get("pubonlnPric"),
                                "isMedicare": drug_info.get("isMedicare"),
                                "hospital_purchases": hosp,
                                "hospital_name": item.get("hospital_name"),
                                "hospital_admdvs": item.get("hospital_admdvs"),
                                "hospital_shp_cnt": item.get("hospital_shp_cnt"),
                                "hospital_shp_time": item.get("hospital_shp_time"),
                                "hospital_is_public": item.get("hospital_is_public"),
                                "collect_time": datetime.now(),
                                "url": url,
                                "page_num": 1,
                            }
                            model_columns = set(HebeiDrug.__table__.columns.keys())
                            record = HebeiDrug(**{k: v for k, v in record_data.items() if k in model_columns})
                            record.md5_id = item.get('md5_id')
                            self._persist_record(db_session, HebeiDrug, record, prod_code)
                    else:
                        item = HebeiDrugItem()
                        for field_name in item.fields:
                            if field_name in ['md5_id', 'collect_time', 'url', 'url_hash', 'hospital_purchases', 'page_num', 'hospital_name', 'hospital_admdvs', 'hospital_shp_cnt', 'hospital_shp_time', 'hospital_is_public']:
                                continue
                            if field_name in drug_info:
                                item[field_name] = drug_info[field_name]
                        item['hospital_purchases'] = None
                        item['hospital_name'] = None
                        item['hospital_admdvs'] = None
                        item['hospital_shp_cnt'] = None
                        item['hospital_shp_time'] = None
                        item['hospital_is_public'] = None
                        item['url'] = url
                        item['page_num'] = 1
                        item.generate_md5_id()

                        record_data = {
                            "prodId": drug_info.get("prodId"),
                            "prodCode": prod_code,
                            "prodName": drug_info.get("prodName"),
                            "dosform": drug_info.get("dosform"),
                            "prodSpec": drug_info.get("prodSpec"),
                            "prodPac": drug_info.get("prodPac"),
                            "prodentpName": drug_info.get("prodentpName"),
                            "prodentpCode": prodentp_code,
                            "pubonlnPric": drug_info.get("pubonlnPric"),
                            "isMedicare": drug_info.get("isMedicare"),
                            "hospital_purchases": None,
                            "hospital_name": None,
                            "hospital_admdvs": None,
                            "hospital_shp_cnt": None,
                            "hospital_shp_time": None,
                            "hospital_is_public": None,
                            "collect_time": datetime.now(),
                            "url": url,
                            "page_num": 1,
                        }
                        model_columns = set(HebeiDrug.__table__.columns.keys())
                        record = HebeiDrug(**{k: v for k, v in record_data.items() if k in model_columns})
                        record.md5_id = item.get('md5_id')
                        self._persist_record(db_session, HebeiDrug, record, prod_code)

                    success_count += 1
                    self.logger.info(f"[{self.spider_name}] 补采 prodCode={prod_code} 成功，医院数: {len(hospital_list)}")

                except Exception as e:
                    err_text = resp_text[:500] if resp_text else ""
                    self.logger.error(
                        f"[{self.spider_name}] 补采 prodCode={prod_code} 失败: {type(e).__name__} {e} 响应片段: {err_text}"
                    )

                await self._delay()

        db_session.commit()
        return success_count

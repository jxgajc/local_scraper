# Crawl Rule Designer Skill（采集规则制定器）

为当前工作区（Scrapy + SQLAlchemy + 可选 Recrawl 补采）制定“可落地的采集规则”，并给出对应的代码改动点与验证清单。

## 触发条件

用户提出任一需求时调用：
- 新增一个省份/站点 Spider
- 改造现有 Spider（单层列表 → 列表+详情/医院）
- 需要明确字段映射/表结构/去重（md5_id）/补采（unique_id）
- 需要输出“规则+实现步骤+验收口径”

## 工作区全链路速查（从运行到入库）

### 运行入口

- 入口：`python run.py`（通过 `SPIDER_MAP` 选择要跑的 Spider）
- Spider 注册：`run.py` 的 `SPIDER_MAP`
- Scrapy 配置：`hybrid_crawler/settings.py`

### Spider 请求/解析入口

- 多数 Spider 继承 `BaseRequestSpider`
- 基类 `parse()` 统一转发到 `parse_logic()`
- 分层建议：`parse_list_page / parse_detail_page / parse_hospital_page`
- 状态上报：混入 `SpiderStatusMixin`，关键节点 `yield self.report_*()` 产出状态 Item（dict）

### Pipeline → Storage → DB

- 普通 Item：进入 `UniversalBatchWritePipeline`（缓冲 + 线程池批量写入）
- 状态 Item（dict 且 `_status_==True`）：进入 `CrawlStatusPipeline`
  - 写入 `crawl_status`
  - upsert `spider_progress`
- MySQL 写入规则：
  - 按 `item.get_model_class()` 分表（决定落哪个 SQLAlchemy Model）
  - 默认按 `md5_id` 查重后插入；冲突降级逐条写
- DB 连接：SQLAlchemy engine/session 来自 `DATABASE_URL` 环境变量（有默认 fallback）

### 补采链路（非 Scrapy）

- `RecrawlManager` 调用各 adapter 的 `find_missing/recrawl`
- adapter 直接用 DB session 回填（独立于 Scrapy pipeline）

## 各爬虫“完整路径”速查（Spider → API → 表 → 补采）

说明：表名以各 Model 的 `__tablename__` 为准；补采以 `recrawl/adapters/*.py` 是否存在为准。

| Spider(name) | 列表 API | 详情/医院 API | 表（__tablename__） | 补采 Adapter / unique_id |
|---|---|---|---|---|
| fujian_drug_spider | `fujian_drug_store.py` 中 `item-cfg-info/list` | `queryHospital`（extCode） | drug_hospital_fujian_test | 有：recrawl/adapters/fujian.py |
| hainan_drug_spider | `getDrugStore`（关键词/分页） | `getDrugStoreDetl`（prodCode/分页） | drug_shop_hainan_test | 有：recrawl/adapters/hainan.py |
| hebei_drug_spider | `queryPubonlnDrudInfoList` | `queryProcurementMedinsList`（prodCode） | drug_hospital_hebei_test | 有：recrawl/adapters/hebei.py / prodCode |
| liaoning_drug_store | POST `https://ggzy.ln.gov.cn/medical` | 无（单层列表） | drug_hospital_liaoning_test | 有：recrawl/adapters/liaoning.py / md5_id |
| ningxia_drug_store | `getRecentPurchaseDetailData.html` | `getDrugDetailDate.html`（procurecatalogId） | drug_hospital_ningxia_test | 有：recrawl/adapters/ningxia.py / procurecatalogId |
| guangdong_drug_spider | `queryPubonlnPage` | `getPurcHospitalInfoListNew`（drugCode） | drug_hospital_guangdong_test | 有：recrawl/adapters/guangdong.py |
| tianjin_drug_spider | `guideGetMedList` | `guideGetHosp`（med_id） | drug_hospital_tianjin_test | 有：recrawl/adapters/tianjin.py / med_id |
| nhsa_drug_spider | `yp/getPublishGoodsDataInfo.html` | 无（单层列表） | nhsa_drug_test | 暂无（可按模板补齐） |
| drug_hosipital_shandong | `listDrug`（需验证码） | `listHospital`（pubonlnId） | drug_hospital_shandong_test | 未加入 SPIDER_MAP；补采未配置 |

## 采集规则输出模板（交付给用户/需求方）

对每个 Spider 输出以下内容（可直接复制进任务卡）：

1) **目标与粒度**
- 目标数据：药品/医院/门店/价格库存
- 粒度：一条记录 =（药品×医院）/（药品×门店）/（药品）
- 目标表：`__tablename__`

2) **接口与分页**
- 列表接口：method、url、headers/cookies/token、入参、退出条件
- 详情/医院接口：关联键（prodCode/extCode/procurecatalogId/pubonlnId 等）、退出条件
- 异常策略：限流、重试、验证码、初始化 cookie

3) **字段映射（最重要）**
- 原始字段 → Item 字段 → DB 列
- `source_data` 是否保留（建议保留关键原始 JSON）

4) **去重与唯一键**
- `md5_id`：MD5(稳定业务字段组合)
  - 单层列表：可用（站点ID + 规格/企业 + 关键词）
  - 医院/门店：必须包含医院/门店标识
- 补采 `unique_id`：优先站点稳定 ID（prodCode/med_id/procurecatalogId）

5) **状态上报（推荐）**
- `start_requests`：初始化一次
- `list_page`：每页上报（page_no/total_pages/items_found/items_stored）
- `detail/hospital`：只上报错误或关键节点，避免覆盖主进度

6) **补采规则（可选）**
- 缺失检测：官网全量 id - DB 已有 id
- 回填策略：回补缺失行 / 或 update_only 更新时间戳
- 终止机制：支持 stop_check（超时/手动停止）

## 实现落地清单（把规则变成代码）

1. Model：`hybrid_crawler/models/<region>_drug.py`（表名与字段）
2. Item：实现 `generate_md5_id()` + `get_model_class()`
3. Spider：实现 `start_requests/parse_logic` 与分页函数
4. 注册：把 Spider 加入 `run.py` 的 `SPIDER_MAP`
5. 补采（如需）：新增 `recrawl/adapters/<region>.py` 并 `@register_adapter(spider_name)`
6. 验证：小样本运行 + DB 对账（count、distinct、非空率）+ 补采回填验证


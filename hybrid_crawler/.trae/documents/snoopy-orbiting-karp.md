# 爬虫数据补采计划

## 目标
为所有爬虫创建补采脚本，通过官网 API 确定缺失数据并进行补采

## 各爬虫 API 汇总

| 爬虫 | 列表 API | 详情 API | 采集方式 | 数据表 | 唯一标识 |
|------|---------|---------|---------|--------|---------|
| fujian | POST /item-cfg-info/list | POST /queryHospital | 全量分页 | drug_hospital_fujian | ext_code |
| hainan | GET /getDrugStore | GET /getDrugStoreDetl | 关键词+分页 | drug_shop_hainan | drug_code |
| guangdong | POST /queryPubonlnPage | POST /getPurcHospitalInfoListNew | 全量分页 | drug_hospital_guangdong | drug_code |
| tianjin | POST /guideGetMedList | POST /guideGetHosp | 关键词 | drug_hospital_tianjin | med_id |
| liaoning | POST /GetYPYYCG | 无 | 关键词+分页 | drug_hospital_liaoning | goodscode |
| ningxia | POST /getRecentPurchaseDetailData | POST /getDrugDetailDate | 全量分页 | drug_hospital_ningxia | procurecatalogId |
| hebei | GET /queryPubonlnDrudInfoList | GET /queryProcurementMedinsList | 全量分页 | drug_hospital_hebei | prodCode |

## 实现步骤

### Step 1: 创建统一补采脚本
位置：`/Users/jcagito/xxl_job/xxl-job/myspider/local_scraper/hybrid_crawler/recrawl_checker.py`

### Step 2: 各爬虫补采逻辑

#### 1. fujian_drug_spider
```python
# 列表 API
url = "https://open.ybj.fujian.gov.cn:10013/tps-local/web/tender/plus/item-cfg-info/list"
payload = {"current": 1, "size": 1000, ...}

# 详情 API
url = "https://open.ybj.fujian.gov.cn:10013/tps-local/web/trans/api/open/v2/queryHospital"
payload = {"productId": ext_code, "pageNo": 1, "pageSize": 100}

# 唯一标识: ext_code
# 数据表: drug_hospital_fujian
```

#### 2. hainan_drug_spider
```python
# 列表 API
url = "https://ybj.hainan.gov.cn/tps-local/local/web/std/drugStore/getDrugStore"
params = {"current": 1, "size": 500, "prodName": keyword}

# 详情 API
url = "https://ybj.hainan.gov.cn/tps-local/local/web/std/drugStore/getDrugStoreDetl"
params = {"current": 1, "size": 20, "drugCode": drug_code}

# 唯一标识: drug_code (prodCode)
# 数据表: drug_shop_hainan
# 特点: 基于关键词采集
```

#### 3. guangdong_drug_spider
```python
# 列表 API
url = "https://igi.hsa.gd.gov.cn/tps_local_bd/web/publicity/pubonlnPublicity/queryPubonlnPage"
payload = {"current": 1, "size": 500, "searchCount": True}

# 详情 API
url = "https://igi.hsa.gd.gov.cn/tps_local_bd/web/publicity/pubonlnPublicity/getPurcHospitalInfoListNew"
payload = {"current": 1, "size": 50, "searchCount": True, "drugCode": drug_code}

# 唯一标识: drug_code (drugCode)
# 数据表: drug_hospital_guangdong
```

#### 4. tianjin_drug_spider
```python
# 列表 API
url = "https://tps.ylbz.tj.gov.cn/csb/1.0.0/guideGetMedList"
payload = {"verificationCode": "xxxx", "content": keyword}

# 详情 API
url = "https://tps.ylbz.tj.gov.cn/csb/1.0.0/guideGetHosp"
payload = {"verificationCode": "xxxx", "genname": ..., "dosform": ..., "spec": ..., "pac": ...}

# 唯一标识: med_id (medid)
# 数据表: drug_hospital_tianjin
# 特点: 需要验证码(4位随机字符)
```

#### 5. liaoning_drug_spider
```python
# 列表 API (无详情API，直接返回完整数据)
url = "https://ggzy.ln.gov.cn/medical"
form_data = {"apiName": "GetYPYYCG", "product": keyword, "company": "", "pageNum": "1"}

# 唯一标识: goodscode
# 数据表: drug_hospital_liaoning
# 特点: 基于关键词+分页，无需详情请求
```

#### 6. ningxia_drug_spider
```python
# 列表 API
url = "https://nxyp.ylbz.nx.gov.cn/cms/recentPurchaseDetail/getRecentPurchaseDetailData.html"
form_data = {"_search": "false", "page": "1", "rows": "100", "sidx": "", "sord": "asc"}

# 详情 API
url = "https://nxyp.ylbz.nx.gov.cn/cms/recentPurchaseDetail/getDrugDetailDate.html"
form_data = {"procurecatalogId": id, "_search": "false", "rows": "100", "page": "1"}

# 唯一标识: procurecatalogId
# 数据表: drug_hospital_ningxia
```

#### 7. hebei_drug_spider
```python
# 列表 API (注意: 请求1000条但返回500条是正常的)
url = "https://ylbzj.hebei.gov.cn/templates/default_pc/syyypqxjzcg/queryPubonlnDrudInfoList"
params = {"pageNo": 1, "pageSize": 1000, "prodName": "", "prodentpName": ""}
headers = {"prodType": "2"}

# 详情 API
url = "https://ylbzj.hebei.gov.cn/templates/default_pc/syyypqxjzcg/queryProcurementMedinsList"
params = {"pageNo": 1, "pageSize": 1000, "prodCode": "xxx", "prodEntpCode": "xxx", "isPublicHospitals": ""}

# 唯一标识: prodCode
# 数据表: drug_hospital_hebei
# 特点: 每页最多返回500条
```

### Step 3: 脚本结构
```
recrawl_checker.py
├── class BaseRecrawler           # 基类
│   ├── get_existing_ids()        # 从DB获取已采集ID
│   ├── fetch_api_total()         # 从官网获取总数
│   ├── find_missing()            # 找出遗漏
│   └── recrawl()                 # 执行补采
│
├── class FujianRecrawler         # 福建补采
├── class HainanRecrawler         # 海南补采
├── class GuangdongRecrawler      # 广东补采
├── class TianjinRecrawler        # 天津补采
├── class LiaoningRecrawler       # 辽宁补采
├── class NingxiaRecrawler        # 宁夏补采
├── class HebeiRecrawler          # 河北补采
│
└── main()                        # 主流程：检查所有爬虫并生成报告
```

### Step 4: 运行方式
```bash
# 在 141 机器上运行
source /opt/anaconda3/bin/activate base
cd /Users/jcagito/xxl_job/xxl-job/myspider/local_scraper/hybrid_crawler

# 检查所有爬虫缺失情况
python recrawl_checker.py --check

# 执行特定爬虫补采
python recrawl_checker.py --recrawl fujian
python recrawl_checker.py --recrawl guangdong
```

## 验证方式
1. 运行 `--check` 模式查看各爬虫缺失情况
2. 运行前后对比各数据表记录数
3. 生成补采报告

## 关键文件
- 爬虫实现: `/Users/jcagito/xxl_job/xxl-job/myspider/local_scraper/hybrid_crawler/hybrid_crawler/spiders/`
- 数据模型: `/Users/jcagito/xxl_job/xxl-job/myspider/local_scraper/hybrid_crawler/hybrid_crawler/models/`
- 关键词文件: `/Users/jcagito/xxl_job/xxl-job/myspider/local_scraper/hybrid_crawler/关键字采集(2).xlsx`

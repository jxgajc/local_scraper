import streamlit as st
import json
import time
import requests
from datetime import datetime

# streamlit run spider_studio_app.py
# --- 配置 ---
# 如果您有 Gemini API Key，请填入此处；留空则隐藏 AI 功能
GEMINI_API_KEY = "" 

# --- 工具函数 ---

def to_pascal_case(s):
    """ snake_case -> PascalCase """
    return ''.join(x.title() for x in s.split('_'))

def to_snake_case(s):
    """ PascalCase -> snake_case """
    import re
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', s)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def call_gemini_smart_parse(json_str):
    """ 调用 Gemini API 进行智能解析 """
    if not GEMINI_API_KEY:
        return None
    
    prompt = f"""
    Analyze this JSON API response for a web scraper. 
    1. Find root list path (e.g. "data.records").
    2. Find total page count path (e.g. "total").
    3. Extract up to 5 key fields. Suggest SQLAlchemy types.
    
    Return JSON ONLY: {{ "rootPath": "...", "totalPath": "...", "mappings": [{{ "modelField": "name", "sourcePath": "name", "type": "String(256)", "desc": "description" }}] }}
    
    JSON Sample: {json_str[:3000]}
    """
    
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key={GEMINI_API_KEY}"
        resp = requests.post(url, json={"contents": [{"parts": [{"text": prompt}]}]})
        if resp.status_code != 200:
            st.error(f"AI API Error: {resp.text}")
            return None
            
        text = resp.json()['candidates'][0]['content']['parts'][0]['text']
        # 清理 markdown 标记
        text = text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        st.error(f"AI Parsing Failed: {e}")
        return None

def local_smart_guess(json_str):
    """ 本地简单规则推测 """
    try:
        data = json.loads(json_str)
        guess = {"rootPath": "rows", "totalPath": "total", "mappings": []}
        
        # 尝试寻找列表
        target_list = []
        if isinstance(data, list):
            target_list = data
            guess['rootPath'] = "Root (List)"
        elif isinstance(data, dict):
            # 常见键名探测
            for key in ['rows', 'records', 'list', 'data']:
                if key in data and isinstance(data[key], list):
                    guess['rootPath'] = key
                    target_list = data[key]
                    break
            # 尝试探测 data.rows
            if not target_list and 'data' in data and isinstance(data['data'], dict):
                for key in ['rows', 'records', 'list']:
                    if key in data['data'] and isinstance(data['data'][key], list):
                        guess['rootPath'] = f"data.{key}"
                        target_list = data['data'][key]
                        break
        
        # 从第一条数据提取字段
        if target_list and len(target_list) > 0:
            item = target_list[0]
            if isinstance(item, dict):
                for k, v in list(item.items())[:5]: # 取前5个
                    ftype = "String(256)"
                    if isinstance(v, int): ftype = "Integer"
                    guess['mappings'].append({
                        "modelField": to_snake_case(k),
                        "sourcePath": k,
                        "type": ftype,
                        "desc": k
                    })
        return guess
    except:
        return None

# --- 代码生成逻辑 ---

def generate_spider_code(cfg, mappings, headers, params):
    class_name = to_pascal_case(cfg['name'])
    model_cls = to_pascal_case(cfg['target_model'])
    model_snake = to_snake_case(cfg['target_model'])
    
    # 构建 Headers
    headers_str = ""
    for h in headers:
        if h['key']:
            headers_str += f"            '{h['key']}': '{h['value']}',\n"

    # 构建 Payload
    payload_str = ""
    for p in params:
        if p['key']:
            val = f"'{p['value']}'"
            if p['type'] == 'dynamic':
                if 'TIMESTAMP_MS' in p['value']:
                    val = "str(int(time.time() * 1000))"
            payload_str += f"            '{p['key']}': {val},\n"

    # 构建安全访问路径
    def safe_get(root, path, default):
        parts = path.split('.')
        s = root
        for i, p in enumerate(parts):
            d = default if i == len(parts)-1 else "{}"
            s += f".get('{p}', {d})"
        return s
    
    rows_access = safe_get('res_json', cfg['root_path'], '[]')
    total_access = safe_get('res_json', cfg['total_path'], '0')
    page_access = safe_get('res_json', 'page', '1')

    # 构建字段映射
    mapping_code = ""
    for m in mappings:
        if '.' in m['sourcePath']:
            # [修复] 将 safe_get 提取到 f-string 外部，避免 Python < 3.12 的反斜杠语法错误
            safe_access_str = safe_get('data_item', m['sourcePath'], "''")
            mapping_code += f"        item['{m['modelField']}'] = {safe_access_str}\n"
    
    return f"""from .base_spiders import BaseRequestSpider
from ..models.{model_snake} import {model_cls}Item
import json
import scrapy
import time

class {class_name}Spider(BaseRequestSpider):
    \"\"\"
    {cfg['description']}
    \"\"\"
    name = "{cfg['name']}"
    list_api_url = "{cfg['url']}"

    custom_settings = {{
        'CONCURRENT_REQUESTS': {cfg['concurrency']},
        'DOWNLOAD_DELAY': {cfg['delay']},
        'DEFAULT_REQUEST_HEADERS': {{
{headers_str}        }},
        'ITEM_PIPELINES': {{
            'hybrid_crawler.pipelines.DataCleaningPipeline': 300,
            'hybrid_crawler.pipelines.{cfg['pipeline']}': 400,
        }}
    }}

    def start_requests(self):
        form_data = {{
{payload_str}        }}
        
        yield scrapy.FormRequest(
            url=self.list_api_url,
            method='POST',
            formdata=form_data,
            callback=self.parse_logic,
            meta={{'form_data': form_data}},
            dont_filter=True
        )

    def parse_logic(self, response):
        try:
            res_json = json.loads(response.text)
            
            rows = {rows_access}
            total = {total_access}
            page = {page_access}
            
            self.logger.info(f"Page {{page}}/{{total}}, Rows: {{len(rows)}}")

            for data_item in rows:
                yield self._create_item(data_item, page)

            # 翻页逻辑
            current_form_data = response.meta.get('form_data')
            try:
                curr_p = int(page)
                total_p = int(total)
            except:
                curr_p, total_p = 1, 0

            if curr_p == 1 and total_p > 1:
                for next_page in range(curr_p + 1, total_p + 1):
                    next_form = current_form_data.copy()
                    next_form['page'] = str(next_page)
                    
                    # 刷新动态参数
                    if 'nd' in next_form:
                        next_form['nd'] = str(int(time.time() * 1000))

                    yield scrapy.FormRequest(
                        url=self.list_api_url,
                        method='POST',
                        formdata=next_form,
                        callback=self.parse_logic,
                        meta={{'form_data': next_form}},
                        dont_filter=True
                    )

        except Exception as e:
            self.logger.error(f"Parse Error: {{e}}")

    def _create_item(self, data_item, page_num):
        item = {model_cls}Item()
        
        # 自动映射同名字段
        for field in item.fields:
            if field not in ['md5_id', 'collect_time', 'url', 'url_hash', 'page_num']:
                item[field] = data_item.get(field, '')
        
        # 深度映射字段
{mapping_code}
        item['url'] = response.url
        item['page_num'] = page_num
        item.generate_md5_id()
        return item
"""

def generate_model_code(cfg, mappings):
    class_name = to_pascal_case(cfg['target_model'])
    
    fields_def = ""
    cols_def = ""
    for m in mappings:
        fields_def += f"    {m['modelField']} = scrapy.Field()\n"
        cols_def += f"    {m['modelField']} = Column({m['type']}, comment=\"{m['desc']}\")\n"

    return f"""from sqlalchemy import Column, String, Text, Integer, JSON, DateTime
from . import BaseModel
import scrapy
import hashlib
import json
from datetime import datetime

class {class_name}Item(scrapy.Item):
    # 自定义字段
{fields_def}
    # 系统字段
    md5_id = scrapy.Field()
    collect_time = scrapy.Field()
    page_num = scrapy.Field()
    url = scrapy.Field()
    url_hash = scrapy.Field()
    
    def generate_md5_id(self):
        field_values = {{k: self.get(k, '') for k in self.fields if k not in ['md5_id', 'collect_time']}}
        sorted_json = json.dumps(field_values, sort_keys=True, ensure_ascii=False)
        self['md5_id'] = hashlib.md5(sorted_json.encode('utf-8')).hexdigest()
        self['collect_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

class {class_name}(BaseModel):
    __tablename__ = '{to_snake_case(cfg['target_model'])}'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    md5_id = Column(String(32))
{cols_def}
    collect_time = Column(DateTime)
    page_num = Column(Integer)
    url = Column(String(1024))
    url_hash = Column(String(64), index=True)
"""

# --- Streamlit UI ---

st.set_page_config(page_title="Spider Studio (Py)", layout="wide", page_icon="🕷️")

st.title("🕷️ Spider Studio (Python Native)")
st.caption("基于 Streamlit 的轻量级爬虫代码生成器")

# 初始化 Session State
if 'headers' not in st.session_state:
    st.session_state.headers = [
        {'key': 'User-Agent', 'value': 'Mozilla/5.0...'},
        {'key': 'Referer', 'value': ''}
    ]
if 'params' not in st.session_state:
    st.session_state.params = [
        {'key': 'page', 'value': '1', 'type': 'static'},
        {'key': 'rows', 'value': '100', 'type': 'static'}
    ]
if 'mappings' not in st.session_state:
    st.session_state.mappings = [
        {'modelField': 'goods_name', 'sourcePath': 'goodsName', 'type': 'String(256)', 'desc': '商品名称'}
    ]

# 布局：左侧配置，右侧预览
col_config, col_preview = st.columns([1, 1])

with col_config:
    with st.expander("1. 基础信息 (Basic)", expanded=True):
        c1, c2 = st.columns(2)
        cfg_name = c1.text_input("爬虫名称 (Name)", "nhsa_drug")
        cfg_model = c2.text_input("模型名称 (Model)", "NhsaDrug")
        cfg_url = st.text_input("目标 URL", "https://code.nhsa.gov.cn/...")
        cfg_pipeline = st.text_input("Pipeline 类名", "NhsaDrugPipeline")
        cfg_desc = st.text_input("描述", "国家医保药品目录采集")

    with st.expander("2. 请求参数 (Request)", expanded=True):
        st.subheader("Headers")
        for i, h in enumerate(st.session_state.headers):
            c1, c2, c3 = st.columns([3, 5, 1])
            h['key'] = c1.text_input(f"Key##h{i}", h['key'], label_visibility="collapsed")
            h['value'] = c2.text_input(f"Val##h{i}", h['value'], label_visibility="collapsed")
            if c3.button("🗑️", key=f"del_h_{i}"):
                st.session_state.headers.pop(i)
                st.rerun()
        if st.button("+ 添加 Header"):
            st.session_state.headers.append({'key': '', 'value': ''})
            st.rerun()

        st.subheader("Payload (Form Data)")
        for i, p in enumerate(st.session_state.params):
            c1, c2, c3, c4 = st.columns([3, 3, 2, 1])
            p['key'] = c1.text_input(f"Key##p{i}", p['key'], label_visibility="collapsed")
            p['value'] = c2.text_input(f"Val##p{i}", p['value'], label_visibility="collapsed")
            p['type'] = c3.selectbox(f"Type##p{i}", ['static', 'dynamic'], index=0 if p['type']=='static' else 1, label_visibility="collapsed")
            if c4.button("🗑️", key=f"del_p_{i}"):
                st.session_state.params.pop(i)
                st.rerun()
        
        c_add, c_macro = st.columns([1, 2])
        if c_add.button("+ 添加参数"):
            st.session_state.params.append({'key': '', 'value': '', 'type': 'static'})
            st.rerun()
        if c_macro.button("+ 时间戳宏"):
            st.session_state.params.append({'key': 'nd', 'value': '{{TIMESTAMP_MS}}', 'type': 'dynamic'})
            st.rerun()

    with st.expander("3. 字段解析 (Parsing)", expanded=True):
        json_input = st.text_area("粘贴 JSON 样例 (用于自动分析)", height=150)
        c_guess, c_ai = st.columns(2)
        
        if c_guess.button("⚡ 本地推测 (无需 Key)"):
            guess = local_smart_guess(json_input)
            if guess:
                st.session_state.root_path = guess['rootPath']
                # 只有当 mappings 为空时才覆盖，防止误删
                if not st.session_state.mappings:
                    st.session_state.mappings = guess['mappings']
                st.success("推测完成！请检查填入的路径。")
            else:
                st.warning("JSON 格式错误或结构太复杂，无法推测。")

        if GEMINI_API_KEY and c_ai.button("✨ AI 智能分析"):
            with st.spinner("AI 正在思考..."):
                guess = call_gemini_smart_parse(json_input)
                if guess:
                    st.session_state.root_path = guess.get('rootPath', '')
                    st.session_state.total_path = guess.get('totalPath', '')
                    st.session_state.mappings = guess.get('mappings', [])
                    st.success("AI 分析完成！")
        
        c1, c2 = st.columns(2)
        cfg_root = c1.text_input("列表路径 (Root Path)", key="root_path", value="data.rows")
        cfg_total = c2.text_input("总页数路径 (Total Path)", key="total_path", value="data.total")

        st.subheader("字段映射")
        # 表头
        c1, c2, c3, c4 = st.columns([3, 3, 3, 1])
        c1.caption("Model 属性")
        c2.caption("JSON Key")
        c3.caption("SQL 类型")
        
        for i, m in enumerate(st.session_state.mappings):
            c1, c2, c3, c4 = st.columns([3, 3, 3, 1])
            m['modelField'] = c1.text_input(f"mf_{i}", m['modelField'], label_visibility="collapsed")
            m['sourcePath'] = c2.text_input(f"sp_{i}", m['sourcePath'], label_visibility="collapsed")
            m['type'] = c3.text_input(f"tp_{i}", m['type'], label_visibility="collapsed")
            if c4.button("x", key=f"del_m_{i}"):
                st.session_state.mappings.pop(i)
                st.rerun()
        
        if st.button("+ 添加字段"):
            st.session_state.mappings.append({'modelField': '', 'sourcePath': '', 'type': 'String(128)', 'desc': ''})
            st.rerun()

    with st.expander("4. 高级设置"):
        c1, c2 = st.columns(2)
        cfg_concurrency = c1.number_input("并发数", 1, 32, 1)
        cfg_delay = c2.number_input("延迟 (秒)", 0.0, 60.0, 15.0)

# --- 生成预览 ---
with col_preview:
    st.subheader("代码预览")
    
    tab1, tab2 = st.tabs(["Spider.py", "Model.py"])
    
    config_dict = {
        'name': cfg_name, 'target_model': cfg_model, 'url': cfg_url, 
        'pipeline': cfg_pipeline, 'description': cfg_desc,
        'root_path': cfg_root, 'total_path': cfg_total,
        'concurrency': cfg_concurrency, 'delay': cfg_delay
    }
    
    spider_code = generate_spider_code(config_dict, st.session_state.mappings, st.session_state.headers, st.session_state.params)
    model_code = generate_model_code(config_dict, st.session_state.mappings)
    
    with tab1:
        st.code(spider_code, language="python")
    with tab2:
        st.code(model_code, language="python")
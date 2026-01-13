import os
import sys
import time
import subprocess
import signal
import psutil
import re
from typing import List, Optional
from collections import deque
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, desc, func
from sqlalchemy.orm import sessionmaker

# ==========================================
# 路径配置
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
template_path = os.path.join(current_dir, "templates")
log_dir = os.path.join(project_root, "logs") # 统一日志目录

# 确保日志目录存在
os.makedirs(log_dir, exist_ok=True)

if project_root not in sys.path:
    sys.path.insert(0, project_root)

# ==========================================
# 模块导入
# ==========================================
try:
    from hybrid_crawler.models import Base, init_db, SessionLocal
    from hybrid_crawler.models.crawl_status import CrawlStatus
    from hybrid_crawler.models.spider_progress import SpiderProgress # 新增
    from hybrid_crawler.recrawl.manager import RecrawlManager  # 新增
    from run import SPIDER_MAP
except ImportError as e:
    print(f"⚠️ 导入警告: {e}")
    SPIDER_MAP = {}
    SessionLocal = None
    CrawlStatus = None
    SpiderProgress = None
    RecrawlManager = None

app = FastAPI(title="Crawler Command Center")
templates = Jinja2Templates(directory=template_path)

# 内存中维护运行的进程
RUNNING_PROCESSES = {}
# 内存中维护运行的补采任务 (spider_name -> crawler_instance)
RECRAWL_TASKS = {}
# 异步任务状态存储 (task_id -> status_dict)
ASYNC_TASK_STATUS = {}

class SpiderTask(BaseModel):
    spiders: List[str]

class RecrawlRequest(BaseModel):
    missing_ids: Optional[List[str]] = None

# ==========================================
# 智能日志分析器
# ==========================================
class LogParser:
    """解析日志以提取进度信息"""
    
    @staticmethod
    def parse_progress(spider_name, log_content):
        """
        根据不同的爬虫日志模式，提取进度
        返回: (progress_percent, current, total, status_text)
        """
        if not log_content:
            return 0, 0, 0, "Waiting for logs..."

        lines = log_content.split('\n')
        last_lines = lines[-50:] # 只分析最近50行，提高效率
        full_text = "\n".join(lines) # 全文用于搜索初始化信息

        # 模式1: 福建药店 (分页模式 [1/33])
        # Log: 📄 药品列表页面 [1/33]
        if "fujian" in spider_name or "分页" in log_content:
            # 搜索总页数 (从后往前找最新的进度)
            for line in reversed(last_lines):
                match = re.search(r'\[(\d+)/(\d+)\]', line)
                if match:
                    current, total = map(int, match.groups())
                    if total > 0:
                        return round((current / total) * 100, 1), current, total, f"Page {current}/{total}"
        
        # 模式2: 海南药店 (关键词模式)
        # Log: 加载关键词: 146 个 ... 正在采集关键词: 片
        if "hainan" in spider_name:
            # 1. 找总数
            total = 0
            total_match = re.search(r'加载关键词[:：]\s*(\d+)', full_text)
            if total_match:
                total = int(total_match.group(1))
            
            # 2. 找当前进度 (统计"正在采集关键词"出现的次数)
            # 注意：这种方式在日志被截断时可能不准，但在 tail 模式下我们尽量读取足够多
            # 更准确的方法是找最后一次出现的关键词索引，但这里简化处理
            if total > 0:
                # 统计已完成的关键词数量（简单通过日志行数估算，或者需在日志中打印进度索引）
                # 假设日志里每处理一个关键词会打印 "正在采集关键词"
                current = len(re.findall(r'正在采集关键词', full_text))
                # 修正：防止 current > total (重试可能导致日志重复)
                current = min(current, total)
                return round((current / total) * 100, 1), current, total, f"Keyword {current}/{total}"

        # 模式3: 通用模式 (根据 Scraped items 估算，或者无法计算)
        return 0, 0, 0, "Running..."

# ==========================================
# API 接口
# ==========================================

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/spiders")
async def get_spiders():
    """获取爬虫列表及简要状态"""
    spiders_list = []
    db = SessionLocal() if SessionLocal else None
    
    try:
        for name in SPIDER_MAP.keys():
            status = "stopped"
            pid = None
            
            # 1. 检查进程状态
            if name in RUNNING_PROCESSES:
                proc = RUNNING_PROCESSES[name]
                if proc.poll() is None:
                    status = "running"
                    pid = proc.pid
                else:
                    del RUNNING_PROCESSES[name]
            
            # 2. 从 DB 获取最后一次运行统计
            last_stats = {}
            if db and CrawlStatus:
                latest_log = db.query(CrawlStatus).filter(
                    CrawlStatus.spider_name == name
                ).order_by(desc(CrawlStatus.start_time)).first()
                if latest_log:
                    last_stats = {
                        "items": latest_log.items_stored,
                        "last_run": latest_log.start_time.strftime("%Y-%m-%d %H:%M") if latest_log.start_time else "-"
                    }
            
            # 3. 尝试从实时进度表获取更准确的状态
            if db and SpiderProgress:
                progress = db.query(SpiderProgress).filter_by(spider_name=name).first()
                if progress:
                     # 如果进程在跑，但 DB 显示 error，可能需要注意
                     if status == "running" and progress.status == "error":
                         status = "warning"
                     # 如果 DB 显示 running 但进程没了，那是意外退出
                     elif status == "stopped" and progress.status == "running":
                         # 这里可以尝试重置 DB 状态，或者显示 "dead"
                         pass
                     
                     if progress.progress_percent > 0:
                         last_stats["progress"] = f"{progress.progress_percent}%"

            spiders_list.append({
                "name": name, 
                "status": status, 
                "pid": pid,
                "stats": last_stats
            })
    finally:
        if db: db.close()
    return {"spiders": spiders_list}

@app.post("/api/start")
async def start_spiders(task: SpiderTask):
    """启动爬虫，重定向日志到文件"""
    started = []
    python_executable = sys.executable
    script_path = os.path.join(project_root, 'run.py')
    
    for name in task.spiders:
        if name in RUNNING_PROCESSES and RUNNING_PROCESSES[name].poll() is None:
            continue
            
        # 定义专属日志文件
        spider_log_file = os.path.join(log_dir, f"{name}.log")
        
        try:
            # 打开日志文件句柄 (w模式覆盖旧日志，或者a模式追加，建议w方便看单次进度)
            log_out = open(spider_log_file, "w", encoding="utf-8")
            
            proc = subprocess.Popen(
                [python_executable, script_path, name],
                cwd=project_root,
                stdout=log_out, # 标准输出重定向到文件
                stderr=subprocess.STDOUT, # 错误输出合并到标准输出
                encoding='utf-8' # 仅在 text=True 时有效，这里直接由 file handle 处理
            )
            RUNNING_PROCESSES[name] = proc
            started.append(name)
        except Exception as e:
            print(f"❌ Start Error {name}: {e}")
            
    return {"status": "ok", "started": started}

@app.post("/api/stop")
async def stop_spider(task: SpiderTask):
    stopped = []
    for name in task.spiders:
        if name in RUNNING_PROCESSES:
            proc = RUNNING_PROCESSES[name]
            # 尝试杀掉进程树
            try:
                parent = psutil.Process(proc.pid)
                for child in parent.children(recursive=True):
                    child.kill()
                parent.kill()
            except: pass
            del RUNNING_PROCESSES[name]
            stopped.append(name)
    return {"status": "ok", "stopped": stopped}

@app.get("/api/spider/{name}/monitor")
async def get_spider_monitor(name: str):
    """获取单个爬虫的实时监控数据（日志+进度）"""
    log_file = os.path.join(log_dir, f"{name}.log")
    
    # 1. 读取日志 (读取最后 10KB) - 保持不变，用于 Debug
    log_content = ""
    if os.path.exists(log_file):
        try:
            with open(log_file, 'rb') as f:
                f.seek(0, 2)
                file_size = f.tell()
                read_size = 1024 * 10
                if file_size > read_size:
                    f.seek(file_size - read_size)
                else:
                    f.seek(0)
                log_content = f.read().decode('utf-8', errors='ignore')
        except Exception as e:
            log_content = f"Error reading log: {e}"
    else:
        log_content = "Waiting for logs... (Log file not created yet)"

    # 2. 从 DB 获取精准进度
    progress = 0
    current = 0
    total = 0
    status_text = "Initializing..."
    last_status_detail = {}
    task_tree = []
    
    if SessionLocal and SpiderProgress:
        db = SessionLocal()
        try:
            # 进度信息
            sp = db.query(SpiderProgress).filter_by(spider_name=name).first()
            if sp:
                progress = sp.progress_percent
                current = sp.completed_tasks
                total = sp.total_tasks
                status_text = sp.current_item or sp.status
            
            # --- 构建任务树 (Task Tree) ---
            if CrawlStatus:
                # 获取最近的 100 条记录
                recent_logs = db.query(CrawlStatus).filter_by(spider_name=name)\
                    .order_by(desc(CrawlStatus.id)).limit(200).all()
                
                # 1. 将记录按 parent_crawl_id 分组
                nodes = {}
                children_map = {}
                
                for log in recent_logs:
                    # 简化节点信息
                    node = {
                        "id": log.crawl_id,
                        "parent_id": log.parent_crawl_id,
                        "stage": log.stage,
                        "status": "success" if log.success else "error",
                        "progress": f"{log.page_no}/{log.total_pages}" if log.total_pages > 0 else f"{log.page_no}",
                        "info": f"Found: {log.items_found} | Stored: {log.items_stored}",
                        "timestamp": log.start_time.strftime("%H:%M:%S") if log.start_time else "",
                        "error": log.error_message,
                        "children": []
                    }
                    nodes[log.crawl_id] = node
                    
                    pid = log.parent_crawl_id
                    if pid:
                        if pid not in children_map: children_map[pid] = []
                        children_map[pid].append(node)
                
                # 2. 组装树 (自底向上或者自顶向下)
                # 由于我们只查了最近 N 条，可能找不到 Root，所以我们将所有 parent_id 在本次查询中找不到的节点视为 "Visible Root"
                
                visible_roots = []
                for log in recent_logs:
                    node = nodes[log.crawl_id]
                    # 如果有子节点，挂载上去
                    if log.crawl_id in children_map:
                        node['children'] = children_map[log.crawl_id]
                    
                    # 判断是否为当前视图的根
                    # 如果没有 parent_id，或者 parent_id 不在本次查出来的节点中
                    if not log.parent_crawl_id or log.parent_crawl_id not in nodes:
                        visible_roots.append(node)
                
                # 去重 (因为 recent_logs 是按 ID 倒序的，我们可能重复添加了)
                # 这里的逻辑有点乱，简化一下：
                # 我们只遍历 visible_roots，但是 visible_roots 可能包含同一个树的多个分支（如果根节点太老没查出来）
                # 为了展示美观，我们只取最顶层的
                
                unique_roots = {}
                for r in visible_roots:
                    if r['id'] not in unique_roots:
                        unique_roots[r['id']] = r
                
                task_tree = list(unique_roots.values())
            
            # 分层状态信息 (保留旧逻辑以兼容)
            if CrawlStatus:
                # 1. 列表层 (List Page) - 主任务
                latest_list = db.query(CrawlStatus).filter_by(spider_name=name, stage='list_page')\
                    .order_by(desc(CrawlStatus.id)).first()
                
                if latest_list:
                    last_status_detail['list_layer'] = {
                        "api_url": latest_list.api_url,
                        "params": latest_list.params,
                        "items_found": latest_list.items_found,
                        "items_stored": latest_list.items_stored,
                        "page_no": latest_list.page_no,
                        "total_pages": latest_list.total_pages,
                        "timestamp": latest_list.start_time.strftime("%H:%M:%S") if latest_list.start_time else ""
                    }
                
                # 2. 详情层 (Detail Page) - 子任务
                latest_detail = db.query(CrawlStatus).filter_by(spider_name=name, stage='detail_page')\
                    .order_by(desc(CrawlStatus.id)).first()
                    
                if latest_detail:
                    last_status_detail['detail_layer'] = {
                        "api_url": latest_detail.api_url,
                        "params": latest_detail.params,
                        "items_found": latest_detail.items_found,
                        "items_stored": latest_detail.items_stored,
                        "page_no": latest_detail.page_no,
                        "total_pages": latest_detail.total_pages,
                        "error_message": latest_detail.error_message,
                        "timestamp": latest_detail.start_time.strftime("%H:%M:%S") if latest_detail.start_time else ""
                    }
                
                # 兼容旧逻辑的 fallback (如果只查到一条，或者作为总体概览)
                if not latest_list and not latest_detail:
                     latest_any = db.query(CrawlStatus).filter_by(spider_name=name).order_by(desc(CrawlStatus.id)).first()
                     if latest_any:
                         last_status_detail['general'] = {
                             "stage": latest_any.stage,
                             "api_url": latest_any.api_url,
                             "items_stored": latest_any.items_stored
                         }

        except Exception as e:
            status_text = f"DB Error: {str(e)}"
        finally:
            db.close()
    else:
        # Fallback to log parser if DB not available
        progress, current, total, status_text = LogParser.parse_progress(name, log_content)
    
    # 3. 判断运行状态
    is_running = False
    if name in RUNNING_PROCESSES and RUNNING_PROCESSES[name].poll() is None:
        is_running = True
        
    return {
        "name": name,
        "is_running": is_running,
        "progress": progress,
        "current_step": current,
        "total_steps": total,
        "status_text": status_text,
        "detail": last_status_detail,
        "task_tree": task_tree, # 新增任务树
        "logs": log_content
    }

@app.get("/api/dashboard/stats")
async def get_stats():
    # 简化的统计接口
    if not SessionLocal: return {}
    db = SessionLocal()
    try:
        total = db.query(func.sum(CrawlStatus.items_stored)).scalar() or 0
        runs = db.query(func.count(CrawlStatus.crawl_id)).scalar() or 0
        return {"total_items": total, "total_runs": runs, "chart_data": []}
    finally:
        db.close()

@app.post("/api/db/reset")
async def reset_db():
    if not SessionLocal: raise HTTPException(500, "No DB")
    try:
        from hybrid_crawler.models import engine, Base
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/recrawl/check")
async def check_recrawl_status():
    """检查所有爬虫的缺失情况"""
    if not RecrawlManager:
        return {"status": "error", "message": "RecrawlManager not available"}

    report = {}
    for name in RecrawlManager.list_spiders():
        try:
            missing_data = RecrawlManager.find_missing(name)
            report[name] = {
                "spider_name": name,
                "missing_count": len(missing_data),
                "missing_ids": list(missing_data.keys())[:10]
            }
        except Exception as e:
            report[name] = {"error": str(e)}

    return {"status": "ok", "report": report}

@app.post("/api/recrawl/stop")
async def stop_recrawl(task: SpiderTask):
    """停止补采任务"""
    stopped = []
    
    # 支持 "all" 关键字
    targets = task.spiders
    if "all" in targets:
        targets = list(RECRAWL_TASKS.keys())
    
    for name in targets:
        if name in RECRAWL_TASKS:
            try:
                # Scrapy spider object doesn't have stop(), but we can define it or just ignore
                # Since these are running in memory/background tasks, we can't easily kill them
                # unless we implemented a flag check in the loop (which we did in mixin via stop_check, but here we run in background thread)
                # For now, we just remove from tasks map
                del RECRAWL_TASKS[name]
                stopped.append(name)
            except Exception as e:
                print(f"Failed to stop {name}: {e}")
                
    return {"status": "ok", "stopped": stopped}

# 全局变量：存储检查结果（缺失的ID列表）
RECRAWL_MISSING_IDS = {}  # {spider_name: set(missing_ids)}

@app.get("/api/recrawl/check/{spider_name}")
async def check_single_recrawl(spider_name: str, background_tasks: BackgroundTasks):
    """检查特定爬虫的缺失情况 (异步执行)"""
    global RECRAWL_MISSING_IDS, ASYNC_TASK_STATUS

    if not RecrawlManager:
        return {"status": "error", "message": "RecrawlManager not available"}

    if spider_name not in RecrawlManager.list_spiders():
        return {"status": "error", "message": "无效的爬虫名称"}

    if spider_name in RECRAWL_TASKS:
        return {"status": "error", "message": "该爬虫正在执行检查或补采任务"}

    task_id = f"check_{spider_name}_{int(time.time())}"
    ASYNC_TASK_STATUS[task_id] = {
        "type": "check",
        "spider_name": spider_name,
        "status": "running",
        "message": "正在检查缺失数据...",
        "missing_count": 0
    }

    def run_check():
        try:
            RECRAWL_TASKS[spider_name] = True

            missing_ids = RecrawlManager.find_missing(spider_name)
            missing_count = len(missing_ids) if missing_ids else 0

            # 保存检查结果，供补采时使用
            RECRAWL_MISSING_IDS[spider_name] = missing_ids

            # 将 ID 列表转换为 list，方便 JSON 序列化
            missing_ids_list = list(missing_ids) if missing_ids else []
            preview_ids = missing_ids_list[:50]  # 预览前50个

            ASYNC_TASK_STATUS[task_id] = {
                "type": "check",
                "spider_name": spider_name,
                "status": "completed",
                "message": f"发现 {missing_count} 条缺失数据",
                "missing_count": missing_count,
                "missing_ids": preview_ids,
                "has_more": len(missing_ids_list) > 50
            }
        except Exception as e:
            import traceback
            traceback.print_exc()
            ASYNC_TASK_STATUS[task_id] = {
                "type": "check",
                "spider_name": spider_name,
                "status": "error",
                "message": str(e),
                "missing_count": 0
            }
        finally:
            if spider_name in RECRAWL_TASKS:
                del RECRAWL_TASKS[spider_name]

    background_tasks.add_task(run_check)
    return {"status": "ok", "task_id": task_id, "message": "检查任务已启动"}


@app.get("/api/recrawl/task/{task_id}")
async def get_task_status(task_id: str):
    """获取异步任务状态及日志"""
    if task_id not in ASYNC_TASK_STATUS:
        return {"status": "error", "message": "任务不存在"}

    task_info = ASYNC_TASK_STATUS[task_id]
    spider_name = task_info.get("spider_name", "")

    # 读取日志文件
    logs = ""
    if spider_name:
        log_file = os.path.join(project_root, "logs", f"{spider_name}.log")
        if os.path.exists(log_file):
            try:
                with open(log_file, 'rb') as f:
                    f.seek(0, 2)
                    file_size = f.tell()
                    read_size = 1024 * 20  # 读取最后20KB
                    if file_size > read_size:
                        f.seek(file_size - read_size)
                    else:
                        f.seek(0)
                    logs = f.read().decode('utf-8', errors='ignore')
            except Exception as e:
                logs = f"读取日志失败: {e}"

    return {"status": "ok", "task": task_info, "logs": logs}


@app.get("/api/recrawl/tasks")
async def get_all_tasks():
    """获取所有运行中的补采任务状态"""
    running_tasks = {k: v for k, v in ASYNC_TASK_STATUS.items() if v.get("status") == "running"}
    active_crawlers = list(RECRAWL_TASKS.keys())
    return {
        "status": "ok",
        "running_tasks": running_tasks,
        "active_crawlers": active_crawlers
    }


@app.post("/api/recrawl/start/{spider_name}")
async def start_recrawl(spider_name: str, background_tasks: BackgroundTasks, body: RecrawlRequest = None):
    """开始特定爬虫的补采 - 使用检查时保存的缺失ID (异步执行)"""
    global RECRAWL_MISSING_IDS, ASYNC_TASK_STATUS

    if not RecrawlManager:
        return {"status": "error", "message": "RecrawlManager not available"}

    if spider_name not in RecrawlManager.list_spiders():
        return {"status": "error", "message": "无效的爬虫名称"}

    if spider_name in RECRAWL_TASKS:
        return {"status": "error", "message": "该爬虫正在执行任务"}

    # 确定要补采的ID
    missing_ids = None

    # 1. 优先使用请求体中提供的 ID
    if body and body.missing_ids:
        missing_ids = body.missing_ids
        print(f"[{spider_name}] 使用用户提供的 {len(missing_ids)} 个ID进行补采")
    # 2. 其次使用后端缓存的 ID
    elif spider_name in RECRAWL_MISSING_IDS and RECRAWL_MISSING_IDS[spider_name]:
        missing_ids = RECRAWL_MISSING_IDS[spider_name]
        print(f"[{spider_name}] 使用缓存的 {len(missing_ids)} 个ID进行补采")
    else:
        return {"status": "error", "message": "请先执行检查缺失数据，或直接提供 ID 列表"}

    task_id = f"recrawl_{spider_name}_{int(time.time())}"
    total_count = len(missing_ids) if missing_ids else 0

    ASYNC_TASK_STATUS[task_id] = {
        "type": "recrawl",
        "spider_name": spider_name,
        "status": "running",
        "message": f"正在补采 {total_count} 条数据...",
        "total": total_count
    }

    def run_recrawl():
        try:
            RECRAWL_TASKS[spider_name] = True

            collected = RecrawlManager.recrawl(spider_name, missing_ids)

            # 补采完成后清除保存的缺失ID
            RECRAWL_MISSING_IDS.pop(spider_name, None)

            ASYNC_TASK_STATUS[task_id] = {
                "type": "recrawl",
                "spider_name": spider_name,
                "status": "completed",
                "message": f"补采完成，成功 {collected} 条",
                "total": total_count,
                "collected": collected
            }

        except Exception as e:
            ASYNC_TASK_STATUS[task_id] = {
                "type": "recrawl",
                "spider_name": spider_name,
                "status": "error",
                "message": str(e),
                "total": total_count
            }
        finally:
            if spider_name in RECRAWL_TASKS:
                del RECRAWL_TASKS[spider_name]

    background_tasks.add_task(run_recrawl)
    return {"status": "ok", "task_id": task_id, "message": f"已开始{spider_name}爬虫的补采任务，共{total_count}条"}

@app.post("/api/recrawl/start-all")
async def start_all_recrawl(background_tasks: BackgroundTasks):
    """一键检查并补充采集所有爬虫 (异步执行)"""
    global ASYNC_TASK_STATUS

    if not RecrawlManager:
        return {"status": "error", "message": "RecrawlManager not available"}

    task_id = f"recrawl_all_{int(time.time())}"
    ASYNC_TASK_STATUS[task_id] = {
        "type": "recrawl_all",
        "status": "running",
        "message": "正在执行一键检查并补采...",
        "completed": [],
        "failed": []
    }

    def run_all_recrawl():
        """执行所有爬虫的检查和补采"""
        completed = []
        failed = []

        for spider_name in RecrawlManager.list_spiders():
            try:
                # 检查是否已有任务在运行
                if spider_name in RECRAWL_TASKS:
                    continue

                # 更新状态
                ASYNC_TASK_STATUS[task_id]["message"] = f"正在处理 {spider_name}..."

                RECRAWL_TASKS[spider_name] = True

                # 执行完整的补采流程
                count = RecrawlManager.full_recrawl(spider_name)
                completed.append({"spider": spider_name, "count": count})

            except Exception as e:
                failed.append({"spider": spider_name, "error": str(e)})
            finally:
                if spider_name in RECRAWL_TASKS:
                    del RECRAWL_TASKS[spider_name]

        ASYNC_TASK_STATUS[task_id] = {
            "type": "recrawl_all",
            "status": "completed",
            "message": f"完成 {len(completed)} 个，失败 {len(failed)} 个",
            "completed": completed,
            "failed": failed
        }

    background_tasks.add_task(run_all_recrawl)
    return {"status": "ok", "task_id": task_id, "message": "已开始所有爬虫的一键检查和补采任务"}

if __name__ == "__main__":
    import uvicorn
    print("🚀 Dashboard running on port 5210")
    uvicorn.run(app, host="0.0.0.0", port=5210)

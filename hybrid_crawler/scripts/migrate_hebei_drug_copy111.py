import os
import sys
import json
import hashlib
import logging

from sqlalchemy import text, inspect, Table, MetaData
from sqlalchemy.dialects.mysql import insert as mysql_insert

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

env_path = os.path.join(project_root, '.env')
try:
    from dotenv import load_dotenv
    load_dotenv(env_path)
except Exception:
    pass

from hybrid_crawler.models import engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("hebei_migrate")

OLD_TABLE = "drug_hospital_hebei_test"
NEW_TABLE = "drug_hospital_hebei_test_copy111"
BATCH_SIZE = 500


def normalize_value(value):
    if value is None:
        return ""
    return str(value).strip()


def compute_md5_id(hospital_name, hospital_shp_time, prod_name, dosform, prod_spec, prod_pac, prodentp_name):
    parts = [
        normalize_value(hospital_name),
        normalize_value(hospital_shp_time),
        normalize_value(prod_name),
        normalize_value(dosform),
        normalize_value(prod_spec),
        normalize_value(prod_pac),
        normalize_value(prodentp_name),
    ]
    raw = "||".join(parts)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def parse_hospital_purchases(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        return [value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return []
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
    return []


def extract_hospital_fields(hosp):
    hospital_name = hosp.get("prodEntpName") or hosp.get("hospitalName") or hosp.get("medinsName")
    hospital_admdvs = hosp.get("prodEntpAdmdvs") or hosp.get("admdvsName")
    hospital_shp_cnt = hosp.get("shpCnt")
    hospital_shp_time = hosp.get("shpTimeFormat")
    hospital_is_public = hosp.get("isPublicHospitals")
    return {
        "hospital_name": hospital_name,
        "hospital_admdvs": hospital_admdvs,
        "hospital_shp_cnt": hospital_shp_cnt,
        "hospital_shp_time": hospital_shp_time,
        "hospital_is_public": hospital_is_public,
    }


def ensure_target_table(conn):
    inspector = inspect(conn)
    if not inspector.has_table(NEW_TABLE):
        conn.execute(text(f"CREATE TABLE {NEW_TABLE} LIKE {OLD_TABLE}"))
    columns = {col["name"] for col in inspector.get_columns(NEW_TABLE)}
    additions = []
    if "hospital_name" not in columns:
        additions.append("ADD COLUMN hospital_name VARCHAR(256)")
    if "hospital_admdvs" not in columns:
        additions.append("ADD COLUMN hospital_admdvs VARCHAR(256)")
    if "hospital_shp_cnt" not in columns:
        additions.append("ADD COLUMN hospital_shp_cnt INT")
    if "hospital_shp_time" not in columns:
        additions.append("ADD COLUMN hospital_shp_time VARCHAR(32)")
    if "hospital_is_public" not in columns:
        additions.append("ADD COLUMN hospital_is_public VARCHAR(32)")
    if additions:
        conn.execute(text(f"ALTER TABLE {NEW_TABLE} " + ", ".join(additions)))


def migrate():
    total_inserted = 0
    total_skipped = 0
    with engine.begin() as conn:
        ensure_target_table(conn)
        metadata = MetaData()
        new_table = Table(NEW_TABLE, metadata, autoload_with=conn)

        last_id = 0
        while True:
            rows = conn.execute(
                text(
                    f"SELECT * FROM {OLD_TABLE} WHERE id > :last_id ORDER BY id ASC LIMIT :limit"
                ),
                {"last_id": last_id, "limit": BATCH_SIZE},
            ).fetchall()
            if not rows:
                break

            insert_rows = []
            for row in rows:
                data = dict(row._mapping)
                if "id" in data and data["id"] is not None:
                    last_id = data["id"]

                base = {}
                for key, value in data.items():
                    if key in new_table.c and key not in {
                        "id",
                        "md5_id",
                        "hospital_purchases",
                        "hospital_name",
                        "hospital_admdvs",
                        "hospital_shp_cnt",
                        "hospital_shp_time",
                        "hospital_is_public",
                    }:
                        base[key] = value

                hospital_list = parse_hospital_purchases(data.get("hospital_purchases"))
                if hospital_list:
                    for hosp in hospital_list:
                        hosp_fields = extract_hospital_fields(hosp)
                        md5_id = compute_md5_id(
                            hosp_fields["hospital_name"],
                            hosp_fields["hospital_shp_time"],
                            base.get("prodName"),
                            base.get("dosform"),
                            base.get("prodSpec"),
                            base.get("prodPac"),
                            base.get("prodentpName"),
                        )
                        row_data = dict(base)
                        row_data.update(hosp_fields)
                        row_data["hospital_purchases"] = hosp
                        row_data["md5_id"] = md5_id
                        insert_rows.append(row_data)
                else:
                    md5_id = compute_md5_id(
                        None,
                        None,
                        base.get("prodName"),
                        base.get("dosform"),
                        base.get("prodSpec"),
                        base.get("prodPac"),
                        base.get("prodentpName"),
                    )
                    row_data = dict(base)
                    row_data.update(
                        {
                            "hospital_purchases": None,
                            "hospital_name": None,
                            "hospital_admdvs": None,
                            "hospital_shp_cnt": None,
                            "hospital_shp_time": None,
                            "hospital_is_public": None,
                            "md5_id": md5_id,
                        }
                    )
                    insert_rows.append(row_data)

            if insert_rows:
                stmt = mysql_insert(new_table).prefix_with("IGNORE")
                result = conn.execute(stmt, insert_rows)
                inserted = result.rowcount or 0
                skipped = len(insert_rows) - inserted
                total_inserted += inserted
                total_skipped += skipped
                logger.info(f"已写入 {total_inserted} 条记录，跳过重复 {total_skipped} 条")

    logger.info(f"迁移完成，共写入 {total_inserted} 条记录，跳过重复 {total_skipped} 条")


if __name__ == "__main__":
    migrate()

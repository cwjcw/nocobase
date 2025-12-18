from __future__ import annotations

import argparse
import glob
import json
import os
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from nocobase_client import NocoBaseClient


def _find_default_xlsx() -> Optional[str]:
    for pattern in ("data/*.xlsx", "data/*.xlsm", "data/*.xls"):
        matches = sorted(glob.glob(pattern))
        if matches:
            return matches[0]
    return None


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _to_python(value: Any) -> Any:
    if _is_empty(value):
        return None
    # pandas/numpy 标量转原生 python 类型
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def _convert_by_field(value: Any, field_def: Optional[Dict[str, Any]]) -> Any:
    """
    根据字段定义尽量把值转换成更合适的类型，降低后端校验失败概率。
    """

    v = _to_python(value)
    if v is None or not field_def:
        return v

    field_type = field_def.get("type")

    # 日期时间：尽量转成字符串（NocoBase 通常可接受）
    if field_type in {"date", "datetime", "datetimeNoTz"}:
        try:
            # pandas.Timestamp / datetime
            if hasattr(v, "to_pydatetime"):
                dt = v.to_pydatetime()
                return dt.isoformat(sep=" ", timespec="seconds")
            if hasattr(v, "isoformat"):
                return v.isoformat(sep=" ", timespec="seconds")  # type: ignore[arg-type]
        except Exception:
            return str(v)
        return str(v)

    # 数字类型
    if field_type in {"double", "float", "decimal"}:
        try:
            return float(v)
        except Exception:
            return v
    if field_type in {"integer", "bigInt", "sort", "snowflakeId"}:
        try:
            # 有些 Excel 数字列会读成 1.0
            if isinstance(v, float) and v.is_integer():
                return int(v)
            if isinstance(v, str) and v.strip().isdigit():
                return int(v.strip())
            return int(float(v))
        except Exception:
            return v

    return v


def _extract_fields_from_collection_get(resp: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    从 collections:get 的返回里尽量提取字段定义，返回：field_name -> field_def
    """

    data = resp.get("data")
    if not isinstance(data, dict):
        return {}

    fields = data.get("fields")
    if not isinstance(fields, list):
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for f in fields:
        if not isinstance(f, dict):
            continue
        name = f.get("name")
        if isinstance(name, str) and name:
            out[name] = f
    return out


def _field_titles(field_def: Dict[str, Any]) -> Tuple[str, ...]:
    titles = []
    for key in ("title", "label"):
        v = field_def.get(key)
        if isinstance(v, str) and v.strip():
            titles.append(v.strip())
    ui = field_def.get("uiSchema")
    if isinstance(ui, dict):
        v = ui.get("title")
        if isinstance(v, str) and v.strip():
            titles.append(v.strip())
    # 去重且保持顺序
    seen = set()
    uniq = []
    for t in titles:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return tuple(uniq)


def build_mapping(
    df: pd.DataFrame,
    *,
    collection_fields: Dict[str, Dict[str, Any]],
    explicit_mapping: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    构建 Excel 列名 -> NocoBase 字段标识 的映射。

    优先级：
    1) explicit_mapping（你手工指定的 mapping）
    2) Excel 列名 == 字段标识（field.name）
    3) Excel 列名 == 字段标题（field.title / field.uiSchema.title）
    """

    excel_to_field: Dict[str, str] = {}
    reasons: Dict[str, str] = {}

    # 1) 手工映射
    if explicit_mapping:
        for excel_col, field_name in explicit_mapping.items():
            if excel_col in df.columns and field_name in collection_fields:
                excel_to_field[excel_col] = field_name
                reasons[excel_col] = "explicit"

    # 2) 列名直接等于字段标识
    for col in df.columns:
        if col in excel_to_field:
            continue
        if isinstance(col, str) and col in collection_fields:
            excel_to_field[col] = col
            reasons[col] = "match_field_name"

    # 3) 列名等于字段标题
    title_to_name: Dict[str, str] = {}
    for name, fdef in collection_fields.items():
        for title in _field_titles(fdef):
            # 只做一对一的标题映射，避免歧义
            if title not in title_to_name:
                title_to_name[title] = name

    for col in df.columns:
        if col in excel_to_field:
            continue
        if isinstance(col, str) and col in title_to_name:
            excel_to_field[col] = title_to_name[col]
            reasons[col] = "match_field_title"

    return excel_to_field, reasons


def main() -> int:
    parser = argparse.ArgumentParser(
        description="用 pandas 读取 data/ 下 Excel，并写入 NocoBase 的 qjzb_orders 表"
    )
    parser.add_argument("--env", default=".env", help="env 文件路径（默认 .env）")
    parser.add_argument(
        "--file",
        default=None,
        help="Excel 文件路径（默认自动选择 data/ 下第一个 xlsx/xlsm/xls）",
    )
    parser.add_argument("--sheet", default=0, help="sheet 名称或序号（默认 0）")
    parser.add_argument("--collection", default="qjzb_orders", help="目标表标识（默认 qjzb_orders）")
    parser.add_argument("--limit", type=int, default=0, help="只导入前 N 行（0 表示全部）")
    parser.add_argument("--dry-run", action="store_true", help="只打印将要写入的数据，不调用 API")
    parser.add_argument(
        "--mapping-json",
        default=None,
        help="手工映射(JSON 字符串)：{\"Excel列名\":\"字段标识\"}",
    )
    parser.add_argument(
        "--mapping-file",
        default=None,
        help="手工映射(JSON 文件路径)：内容为 {\"Excel列名\":\"字段标识\"}",
    )
    args = parser.parse_args()

    path = args.file or _find_default_xlsx()
    if not path or not os.path.exists(path):
        raise SystemExit("未找到 Excel 文件，请用 --file 指定，例如 --file .\\data\\订单列表.xlsx")

    explicit_mapping: Optional[Dict[str, str]] = None
    if args.mapping_json and args.mapping_file:
        raise SystemExit("不能同时使用 --mapping-json 与 --mapping-file")
    if args.mapping_file:
        with open(args.mapping_file, "r", encoding="utf-8") as f:
            explicit_mapping = json.load(f)
    elif args.mapping_json:
        explicit_mapping = json.loads(args.mapping_json)

    client = NocoBaseClient.from_env(args.env)

    # 读 Excel
    df = pd.read_excel(path, sheet_name=args.sheet, dtype=object)
    df = df.dropna(axis=0, how="all")
    df = df.dropna(axis=1, how="all")

    if df.empty:
        raise SystemExit("Excel 为空或只有空行/空列")

    # 拉取表结构，用于“自动列名映射”
    collection = str(args.collection).strip()
    col_resp = client.collections_get(name=collection)
    fields = _extract_fields_from_collection_get(col_resp)
    if not fields:
        print("警告：未能从 collections_get 提取 fields，自动映射能力受限")

    mapping, reasons = build_mapping(df, collection_fields=fields, explicit_mapping=explicit_mapping)
    unmapped = [c for c in df.columns if c not in mapping]

    print(f"Excel: {path}")
    print(f"Sheet: {args.sheet}")
    print(f"Rows: {len(df)}  Cols: {len(df.columns)}")
    print(f"Target collection: {collection}")
    print("\n字段映射（Excel列 -> 字段标识 / 原因）：")
    for excel_col, field_name in mapping.items():
        print(f"  - {excel_col} -> {field_name} ({reasons.get(excel_col)})")
    if unmapped:
        print("\n未映射的列（将被忽略）：")
        for c in unmapped:
            print(f"  - {c}")

    total = len(df) if args.limit <= 0 else min(len(df), args.limit)
    success = 0
    failed = 0

    for i in range(total):
        row = df.iloc[i]
        values: Dict[str, Any] = {}
        for excel_col, field_name in mapping.items():
            v = _convert_by_field(row.get(excel_col), fields.get(field_name))
            if v is None:
                continue
            values[field_name] = v

        if not values:
            print(f"[SKIP] row={i+1} 映射后无可写入字段")
            continue

        if args.dry_run:
            print(f"[DRY] row={i+1} values={values}")
            success += 1
            continue

        try:
            resp = client.create(collection, values)
            new_id = resp.get("data", {}).get("id")
            print(f"[OK] row={i+1} id={new_id}")
            success += 1
        except Exception as e:
            print(f"[FAIL] row={i+1} error={e} values={values}")
            failed += 1

    print(f"\n完成：success={success} failed={failed} total={total}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())

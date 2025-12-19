from __future__ import annotations

"""
最简可运行示例：只传表名 + Excel 路径即可导入。

建议先 dry-run 看映射与预览数据，再真正写入：
  python .\\test.py --dry-run
"""

import argparse
import json

from nocobase_client import NocoBaseClient, import_excel_to_collection


def main() -> int:
    parser = argparse.ArgumentParser(description="读取 Excel 并新增到 NocoBase 指定表（collection）")
    parser.add_argument("--env", default=".env", help="env 文件路径（默认 .env）")
    parser.add_argument("--collection", default="qjzb_orders", help="目标表标识")
    parser.add_argument("--excel-path", default=r".\data\订单列表.xlsx", help="Excel 文件路径")
    parser.add_argument("--sheet", default=0, help="sheet 名称或序号（默认 0）")
    parser.add_argument("--limit", type=int, default=0, help="只导入前 N 行（0 表示全部）")
    parser.add_argument("--dry-run", action="store_true", help="只预览，不写入")
    args = parser.parse_args()

    client = NocoBaseClient.from_env(args.env)
    result = import_excel_to_collection(
        client=client,
        collection=args.collection,
        excel_path=args.excel_path,
        sheet=args.sheet,
        limit=args.limit,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("failed", 0) == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())


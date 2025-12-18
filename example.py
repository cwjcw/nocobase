"""
NocoBase API 封装可运行示例（替代 DATABASE_API.md）

使用方式（PowerShell）：
  1) 配置 .env（参考 .env.example）
  2) 运行某个示例：
     python .\example.py records-crud --collection test1

说明：
  - 本文件的每个“示例函数”都对应 `nocobase_client.py` 中的一个封装能力。
  - 默认示例只对“记录数据”做增删改查（会创建一条记录并删除它），相对安全。
  - 涉及“创建/删除数据表”等高风险操作，默认不执行，需要加 `--danger`。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Dict

from nocobase_client import NocoBaseClient
from table_utils import extract_rows, format_table


def pretty(obj: Any) -> str:
    """将返回结果格式化为可读 JSON 字符串（中文不转义）。"""

    return json.dumps(obj, ensure_ascii=False, indent=2)


# -------------------------
# Records（数据行）相关示例
# -------------------------

def example_records_create(client: NocoBaseClient, *, collection: str, values: Dict[str, Any]) -> Dict[str, Any]:
    """
    示例：创建一条记录（对应 client.create）

    参数说明：
    - client: 通过 NocoBaseClient.from_env() 创建的客户端
    - collection: 数据表标识（例如 test1）
    - values: 要写入的字段字典（例如 {"name":"张三", "f_xxx": 1.23}）

    返回：
    - NocoBase 响应 JSON（dict），通常包含 data.id
    """

    return client.create(collection, values)


def example_records_list(client: NocoBaseClient, *, collection: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    示例：查询记录列表（对应 client.list）

    参数说明：
    - params: 查询参数（不同版本支持的字段可能不同），常见用法：
      - {"page": 1, "pageSize": 20}
      - {"sort": "-createdAt"}
      - {"filter": {...}} / {"filterByTk": "..."} 等
    """

    return client.list(collection, params=params)


def example_records_get(client: NocoBaseClient, *, collection: str, pk: Any) -> Dict[str, Any]:
    """
    示例：查询单条记录（对应 client.get）

    参数说明：
    - pk: 主键值（通常是 id）
    """

    return client.get(collection, pk=pk)


def example_records_update(client: NocoBaseClient, *, collection: str, pk: Any, values: Dict[str, Any]) -> Dict[str, Any]:
    """
    示例：更新单条记录（对应 client.update）

    参数说明：
    - pk: 主键值（通常是 id）
    - values: 要更新的字段字典（只传要改的字段即可）
    """

    return client.update(collection, pk=pk, values=values)


def example_records_destroy(client: NocoBaseClient, *, collection: str, pk: Any) -> Dict[str, Any]:
    """
    示例：删除单条记录（对应 client.destroy）

    参数说明：
    - pk: 主键值（通常是 id）
    """

    return client.destroy(collection, pk=pk)


def run_records_crud(client: NocoBaseClient, *, collection: str) -> int:
    """
    一键跑通 Records CRUD（创建 -> 查询 -> 更新 -> 删除）。

    你只需要修改参数 `collection`，就可以对任意表做同样的 CRUD 验证。

    注意：
    - 本示例默认写入两个字段：
      - name：字符串
      - f_h2v1n6u8mfh：你“测试(test1)”表里的“数量”字段标识
    - 如果你换了别的表，请把 values 里的字段改成那张表实际存在的字段标识。
    """

    now = int(time.time())
    created = example_records_create(
        client,
        collection=collection,
        values={"name": f"example-{now}", "f_h2v1n6u8mfh": 1.23},
    )
    print("== records.create ==")
    print(pretty(created))

    pk = created.get("data", {}).get("id")
    if not pk:
        raise SystemExit("create 没有返回 data.id，无法继续演示")

    got = example_records_get(client, collection=collection, pk=pk)
    print("\n== records.get ==")
    print(pretty(got))

    updated = example_records_update(
        client,
        collection=collection,
        pk=pk,
        values={"name": f"example-updated-{now}"},
    )
    print("\n== records.update ==")
    print(pretty(updated))

    deleted = example_records_destroy(client, collection=collection, pk=pk)
    print("\n== records.destroy ==")
    print(pretty(deleted))

    listed = example_records_list(client, collection=collection, params={"page": 1, "pageSize": 5})
    print("\n== records.list (page 1, pageSize 5) ==")
    print(pretty(listed))
    print("\n== records.list as table ==")
    print(format_table(extract_rows(listed), columns=["id", "name", "f_h2v1n6u8mfh", "createdAt"]))
    return 0


# -----------------------------
# Collections（数据表定义）示例
# -----------------------------

def example_collections_list(client: NocoBaseClient) -> Dict[str, Any]:
    """
    示例：列出所有数据表（对应 client.collections_list）
    """

    return client.collections_list()


def example_collections_get(client: NocoBaseClient, *, name: str) -> Dict[str, Any]:
    """
    示例：获取某个数据表定义（对应 client.collections_get）

    参数说明：
    - name: 数据表标识，例如 test1
    """

    return client.collections_get(name=name)


def example_collections_create(client: NocoBaseClient, *, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    示例：创建数据表（对应 client.collections_create）

    参数说明：
    - payload: 请求体结构以官方文档为准，通常包含 name/title/fields 等。
    """

    return client.collections_create(payload)


def example_collections_update(client: NocoBaseClient, *, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    示例：更新数据表定义（对应 client.collections_update）

    参数说明：
    - payload: 请求体结构以官方文档为准。
    """

    return client.collections_update(payload)


def example_collections_destroy(client: NocoBaseClient, *, name: str) -> Dict[str, Any]:
    """
    示例：删除数据表（对应 client.collections_destroy）

    参数说明：
    - name: 数据表标识，例如 demo
    """

    return client.collections_destroy(name=name)


def run_collections_safe(client: NocoBaseClient, *, name: str) -> int:
    """
    Collections 的“安全演示”：只 list + get，不创建/删除表。
    """

    print("== collections.list ==")
    resp = example_collections_list(client)
    print(pretty(resp))
    print("\n== collections.list as table ==")
    print(format_table(extract_rows(resp), columns=["name", "title", "template", "type"]))

    print("\n== collections.get ==")
    got = example_collections_get(client, name=name)
    print(pretty(got))
    print("\n== collections.get as table ==")
    print(format_table(extract_rows(got)))
    return 0


def run_collections_danger(client: NocoBaseClient, *, name: str) -> int:
    """
    Collections 的“高风险演示”：创建表 -> 更新表 -> 删除表。

    强烈建议只在测试环境运行。
    """

    create_payload = {
        "name": name,
        "title": f"示例表-{name}",
        "fields": [{"name": "name", "type": "string"}],
    }
    print("== collections.create ==")
    print(pretty(example_collections_create(client, payload=create_payload)))

    print("\n== collections.update ==")
    print(pretty(example_collections_update(client, payload={"name": name, "title": f"示例表(已更新)-{name}"})))

    print("\n== collections.destroy ==")
    print(pretty(example_collections_destroy(client, name=name)))
    return 0


# -------------------------
# 通用 action 调用示例
# -------------------------

def run_action_example(client: NocoBaseClient, *, path: str, method: str, params_json: str | None, body_json: str | None) -> int:
    """
    示例：调用任意 action（对应 client.action）

    参数说明：
    - path: action 路径，不需要 /api 前缀，例如：
      - collections:list
      - test1:create
    - method: GET/POST...
    - params_json: query 参数的 JSON 字符串（可选）
    - body_json: body 的 JSON 字符串（可选）
    """

    params = json.loads(params_json) if params_json else None
    body = json.loads(body_json) if body_json else None
    resp = client.action(path=path, method=method.upper(), params=params, json=body)
    print(pretty(resp))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="NocoBase 封装可运行示例（替代文档）")
    parser.add_argument("--env", default=".env", help="env 文件路径（默认 .env）")

    sub = parser.add_subparsers(dest="cmd", required=True)

    r_crud = sub.add_parser("records-crud", help="跑通任意表的记录 CRUD")
    r_crud.add_argument("--collection", required=True, help="数据表标识，例如 test1")

    c_safe = sub.add_parser("collections-safe", help="Collections 安全演示（list+get）")
    c_safe.add_argument("--name", required=True, help="数据表标识，例如 test1")

    c_danger = sub.add_parser("collections-danger", help="Collections 高风险演示（create+update+destroy）")
    c_danger.add_argument("--name", required=True, help="要创建并最终删除的数据表标识，例如 demo_tmp")
    c_danger.add_argument("--danger", action="store_true", help="必须显式传入才执行")

    act = sub.add_parser("action", help="调用任意 action")
    act.add_argument("--path", required=True, help="例如 collections:list 或 test1:create")
    act.add_argument("--method", default="POST", help="默认 POST")
    act.add_argument("--params", default=None, help="query 参数 JSON 字符串")
    act.add_argument("--json", default=None, help="body JSON 字符串")

    # 用户直接运行 `python example.py` 时，打印帮助而不是报错
    if len(sys.argv) == 1:
        parser.print_help()
        print(
            "\n示例：\n"
            "  python .\\example.py records-crud --collection test1\n"
            "  python .\\example.py collections-safe --name test1\n"
            "  python .\\example.py action --method GET --path collections:list\n"
        )
        return 0

    args = parser.parse_args()

    # 客户端初始化：从 env 读取 NOCOBASE_BASE_URL / NOCOBASE_TOKEN
    os.environ.setdefault("NOCOBASE_ENV_PATH", args.env)
    client = NocoBaseClient.from_env(args.env)

    if args.cmd == "records-crud":
        return run_records_crud(client, collection=args.collection)
    if args.cmd == "collections-safe":
        return run_collections_safe(client, name=args.name)
    if args.cmd == "collections-danger":
        if not args.danger:
            raise SystemExit("高风险操作：请加上 --danger 才会执行")
        return run_collections_danger(client, name=args.name)
    if args.cmd == "action":
        return run_action_example(
            client,
            path=args.path,
            method=args.method,
            params_json=args.params,
            body_json=args.json,
        )

    raise SystemExit("未知命令")


if __name__ == "__main__":
    raise SystemExit(main())

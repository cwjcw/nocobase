import argparse
import json
from typing import Any, Dict, Optional

from nocobase_client import NocoBaseClient, NocoBaseConfig
from table_utils import extract_rows, format_table


def _coerce_value(text: str) -> Any:
    """
    将命令行的字符串值尽量转换成合适的 Python 类型。

    支持：
    - null/none -> None
    - true/false -> bool
    - 数字（int/float）
    - 以 { 或 [ 开头的 JSON
    - 其它：原始字符串
    """

    raw = text.strip()
    lowered = raw.lower()
    if lowered in {"null", "none"}:
        return None
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        if "." in raw:
            return float(raw)
        return int(raw)
    except Exception:
        pass
    if raw.startswith("{") or raw.startswith("["):
        return json.loads(raw)
    return text


def _parse_kv_pairs(pairs: Optional[list]) -> Dict[str, Any]:
    """
    解析重复参数：--set key=value 或 --param key=value
    """

    result: Dict[str, Any] = {}
    if not pairs:
        return result
    for item in pairs:
        if "=" not in item:
            raise SystemExit(f"参数格式必须是 key=value：{item}")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise SystemExit(f"参数 key 不能为空：{item}")
        result[key] = _coerce_value(value)
    return result


def _parse_json_arg(json_text: Optional[str], json_file: Optional[str]) -> Optional[Dict[str, Any]]:
    if json_text and json_file:
        raise SystemExit("不能同时使用 --json 与 --json-file")
    if json_file:
        with open(json_file, "r", encoding="utf-8") as f:
            return json.load(f)
    if json_text:
        return json.loads(json_text)
    return None


def _client_from_args(args: argparse.Namespace) -> NocoBaseClient:
    client = NocoBaseClient.from_env(args.env)
    if args.base_url:
        client = NocoBaseClient(
            config=NocoBaseConfig(
                base_url=args.base_url,
                token=client.config.token,
                timeout=client.config.timeout,
            )
        )
    return client


def main() -> int:
    parser = argparse.ArgumentParser(
        description="NocoBase 通用 CLI（改参数即可对任意表增删改查/调用任意 action）"
    )
    parser.add_argument("--env", default=".env", help="env 文件路径（默认 .env）")
    parser.add_argument("--base-url", default=None, help="覆盖 NOCOBASE_BASE_URL，例如 http://域名/api")

    sub = parser.add_subparsers(dest="cmd", required=True)

    # records
    records = sub.add_parser("records", help="对任意数据表（collection）做增删改查")
    records_sub = records.add_subparsers(dest="op", required=True)

    r_create = records_sub.add_parser("create", help="创建记录")
    r_create.add_argument("--collection", required=True, help="数据表标识，例如 test1")
    r_create.add_argument("--json", default=None, help="要写入的字段(JSON 字符串)")
    r_create.add_argument("--json-file", default=None, help="要写入的字段(JSON 文件路径)")
    r_create.add_argument(
        "--set",
        action="append",
        default=None,
        help="要写入的字段：key=value（可重复，例如 --set name=张三 --set count=1）",
    )

    r_list = records_sub.add_parser("list", help="查询列表")
    r_list.add_argument("--collection", required=True, help="数据表标识，例如 test1")
    r_list.add_argument("--params", default=None, help="查询参数(JSON 字符串)，例如 {\"page\":1}")
    r_list.add_argument("--params-file", default=None, help="查询参数(JSON 文件路径)")
    r_list.add_argument(
        "--param",
        action="append",
        default=None,
        help="查询参数：key=value（可重复，例如 --param page=1 --param pageSize=10）",
    )
    r_list.add_argument("--table", action="store_true", help="以表格形式输出 data（仅展示扁平字段）")
    r_list.add_argument(
        "--columns",
        default=None,
        help="表格列（逗号分隔），例如 id,name,createdAt",
    )

    r_get = records_sub.add_parser("get", help="查询单条（按主键）")
    r_get.add_argument("--collection", required=True, help="数据表标识，例如 test1")
    r_get.add_argument("--pk", required=True, help="主键值（一般是 id）")
    r_get.add_argument("--table", action="store_true", help="以表格形式输出 data")
    r_get.add_argument("--columns", default=None, help="表格列（逗号分隔）")

    r_update = records_sub.add_parser("update", help="更新单条（按主键）")
    r_update.add_argument("--collection", required=True, help="数据表标识，例如 test1")
    r_update.add_argument("--pk", required=True, help="主键值（一般是 id）")
    r_update.add_argument("--json", default=None, help="要更新的字段(JSON 字符串)")
    r_update.add_argument("--json-file", default=None, help="要更新的字段(JSON 文件路径)")
    r_update.add_argument(
        "--set",
        action="append",
        default=None,
        help="要更新的字段：key=value（可重复，例如 --set name=李四）",
    )

    r_destroy = records_sub.add_parser("destroy", help="删除单条（按主键）")
    r_destroy.add_argument("--collection", required=True, help="数据表标识，例如 test1")
    r_destroy.add_argument("--pk", required=True, help="主键值（一般是 id）")

    # collections
    collections = sub.add_parser("collections", help="数据表（collection）定义相关接口")
    collections_sub = collections.add_subparsers(dest="op", required=True)

    c_list = collections_sub.add_parser("list", help="列出所有数据表")
    c_list.add_argument("--params", default=None, help="查询参数(JSON 字符串)")
    c_list.add_argument("--params-file", default=None, help="查询参数(JSON 文件路径)")
    c_list.add_argument(
        "--param",
        action="append",
        default=None,
        help="查询参数：key=value（可重复）",
    )
    c_list.add_argument("--table", action="store_true", help="以表格形式输出 data")
    c_list.add_argument("--columns", default=None, help="表格列（逗号分隔）")

    c_get = collections_sub.add_parser("get", help="获取某个数据表定义")
    c_get.add_argument("--name", required=True, help="数据表标识，例如 test1")
    c_get.add_argument("--table", action="store_true", help="以表格形式输出 data")
    c_get.add_argument("--columns", default=None, help="表格列（逗号分隔）")

    c_create = collections_sub.add_parser("create", help="创建数据表（payload 见官方文档）")
    c_create.add_argument("--json", default=None, help="payload(JSON 字符串)")
    c_create.add_argument("--json-file", default=None, help="payload(JSON 文件路径)")

    c_update = collections_sub.add_parser("update", help="更新数据表（payload 见官方文档）")
    c_update.add_argument("--json", default=None, help="payload(JSON 字符串)")
    c_update.add_argument("--json-file", default=None, help="payload(JSON 文件路径)")

    c_destroy = collections_sub.add_parser("destroy", help="删除数据表")
    c_destroy.add_argument("--name", required=True, help="数据表标识，例如 test1")

    c_move = collections_sub.add_parser("move", help="移动/排序（payload 见官方文档）")
    c_move.add_argument("--json", default=None, help="payload(JSON 字符串)")
    c_move.add_argument("--json-file", default=None, help="payload(JSON 文件路径)")

    c_set_fields = collections_sub.add_parser("set-fields", help="设置字段（payload 见官方文档）")
    c_set_fields.add_argument("--json", default=None, help="payload(JSON 字符串)")
    c_set_fields.add_argument("--json-file", default=None, help="payload(JSON 文件路径)")

    # raw action
    action = sub.add_parser("action", help="调用任意 action（当封装里没有对应函数时使用）")
    action.add_argument("--path", required=True, help="例如 collections:list 或 test1:create")
    action.add_argument("--method", default="POST", help="默认 POST；GET/POST/PUT/DELETE 等")
    action.add_argument("--params", default=None, help="query 参数(JSON 字符串)")
    action.add_argument("--params-file", default=None, help="query 参数(JSON 文件路径)")
    action.add_argument("--json", default=None, help="body(JSON 字符串)")
    action.add_argument("--json-file", default=None, help="body(JSON 文件路径)")
    action.add_argument(
        "--param",
        action="append",
        default=None,
        help="query 参数：key=value（可重复）",
    )
    action.add_argument(
        "--set",
        action="append",
        default=None,
        help="body 字段：key=value（可重复）",
    )

    args = parser.parse_args()
    client = _client_from_args(args)

    if args.cmd == "records":
        if args.op == "create":
            values = _parse_json_arg(args.json, args.json_file) or _parse_kv_pairs(args.set)
            if values is None:
                raise SystemExit("records create 需要 --json/--json-file 或 --set")
            resp = client.create(args.collection, values=values)
            print(json.dumps(resp, ensure_ascii=False))
            return 0
        if args.op == "list":
            params = _parse_json_arg(args.params, args.params_file) or _parse_kv_pairs(args.param) or {}
            resp = client.list(args.collection, params=params)
            if args.table:
                cols = [c.strip() for c in (args.columns or "").split(",") if c.strip()] or None
                print(format_table(extract_rows(resp), columns=cols))
            else:
                print(json.dumps(resp, ensure_ascii=False))
            return 0
        if args.op == "get":
            resp = client.get(args.collection, pk=args.pk)
            if args.table:
                cols = [c.strip() for c in (args.columns or "").split(",") if c.strip()] or None
                print(format_table(extract_rows(resp), columns=cols))
            else:
                print(json.dumps(resp, ensure_ascii=False))
            return 0
        if args.op == "update":
            values = _parse_json_arg(args.json, args.json_file) or _parse_kv_pairs(args.set)
            if values is None:
                raise SystemExit("records update 需要 --json/--json-file 或 --set")
            resp = client.update(args.collection, pk=args.pk, values=values)
            print(json.dumps(resp, ensure_ascii=False))
            return 0
        if args.op == "destroy":
            resp = client.destroy(args.collection, pk=args.pk)
            print(json.dumps(resp, ensure_ascii=False))
            return 0

    if args.cmd == "collections":
        if args.op == "list":
            params = _parse_json_arg(args.params, args.params_file) or _parse_kv_pairs(args.param) or {}
            resp = client.collections_list(params=params)
            if args.table:
                cols = [c.strip() for c in (args.columns or "").split(",") if c.strip()] or None
                print(format_table(extract_rows(resp), columns=cols))
            else:
                print(json.dumps(resp, ensure_ascii=False))
            return 0
        if args.op == "get":
            resp = client.collections_get(name=args.name)
            if args.table:
                cols = [c.strip() for c in (args.columns or "").split(",") if c.strip()] or None
                print(format_table(extract_rows(resp), columns=cols))
            else:
                print(json.dumps(resp, ensure_ascii=False))
            return 0
        if args.op == "create":
            payload = _parse_json_arg(args.json, args.json_file)
            if payload is None:
                raise SystemExit("collections create 需要 --json 或 --json-file")
            resp = client.collections_create(payload)
            print(json.dumps(resp, ensure_ascii=False))
            return 0
        if args.op == "update":
            payload = _parse_json_arg(args.json, args.json_file)
            if payload is None:
                raise SystemExit("collections update 需要 --json 或 --json-file")
            resp = client.collections_update(payload)
            print(json.dumps(resp, ensure_ascii=False))
            return 0
        if args.op == "destroy":
            resp = client.collections_destroy(name=args.name)
            print(json.dumps(resp, ensure_ascii=False))
            return 0
        if args.op == "move":
            payload = _parse_json_arg(args.json, args.json_file)
            if payload is None:
                raise SystemExit("collections move 需要 --json 或 --json-file")
            resp = client.collections_move(payload)
            print(json.dumps(resp, ensure_ascii=False))
            return 0
        if args.op == "set-fields":
            payload = _parse_json_arg(args.json, args.json_file)
            if payload is None:
                raise SystemExit("collections set-fields 需要 --json 或 --json-file")
            resp = client.collections_set_fields(payload)
            print(json.dumps(resp, ensure_ascii=False))
            return 0

    if args.cmd == "action":
        params = _parse_json_arg(args.params, args.params_file) or _parse_kv_pairs(args.param) or None
        body = _parse_json_arg(args.json, args.json_file) or _parse_kv_pairs(args.set) or None
        resp = client.action(path=args.path, method=args.method.upper(), params=params, json=body)
        print(json.dumps(resp, ensure_ascii=False))
        return 0

    raise SystemExit("未知命令")


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    # 对 dict/list 等复杂结构，给一个简短占位，避免表格炸裂
    if isinstance(value, dict):
        return "{...}"
    if isinstance(value, list):
        return f"[{len(value)}]"
    return str(value)


def _truncate(text: str, max_width: int) -> str:
    if max_width <= 0:
        return text
    if len(text) <= max_width:
        return text
    return text[: max(0, max_width - 1)] + "…"


def _collect_columns(rows: Sequence[Dict[str, Any]]) -> List[str]:
    cols: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in cols:
                cols.append(key)
    return cols


def format_table(
    rows: Iterable[Dict[str, Any]],
    *,
    columns: Optional[Sequence[str]] = None,
    max_col_width: int = 40,
) -> str:
    """
    将“列表 + 字典行”格式化成纯文本表格（无第三方依赖）。

    参数：
    - rows: 可迭代的 dict 列表（例如 NocoBase list 返回的 data）
    - columns: 指定输出列顺序；不传则按出现顺序收集所有 key
    - max_col_width: 单列最大宽度，避免长字符串撑爆输出

    返回：
    - 可直接 print 的字符串（ASCII 表格）

    用法示例：
        from table_utils import format_table
        print(format_table(resp["data"], columns=["id", "name"]))
    """

    materialized: List[Dict[str, Any]] = list(rows)
    if not materialized:
        return "(empty)"

    cols = list(columns) if columns else _collect_columns(materialized)
    if not cols:
        return "(no columns)"

    # 生成 cell 字符串矩阵
    body: List[List[str]] = []
    for row in materialized:
        body.append([_truncate(_stringify(row.get(c)), max_col_width) for c in cols])

    header = [str(c) for c in cols]
    all_rows = [header] + body

    widths = [max(len(r[i]) for r in all_rows) for i in range(len(cols))]

    def sep(char: str = "-") -> str:
        parts = [char * (w + 2) for w in widths]
        return "+" + "+".join(parts) + "+"

    def line(items: Sequence[str]) -> str:
        cells = [f" {items[i].ljust(widths[i])} " for i in range(len(cols))]
        return "|" + "|".join(cells) + "|"

    out_lines = [sep("-"), line(header), sep("=")]
    out_lines.extend(line(r) for r in body)
    out_lines.append(sep("-"))
    return "\n".join(out_lines)


def extract_rows(response_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    从 NocoBase 常见响应结构中提取“可表格化”的行数据。

    规则：
    - 如果 response_json["data"] 是 list[dict]，直接返回
    - 如果 response_json["data"] 是 dict，则返回 [data]
    - 其它情况返回 []
    """

    data = response_json.get("data")
    if isinstance(data, list) and all(isinstance(x, dict) for x in data):
        return data  # type: ignore[return-value]
    if isinstance(data, dict):
        return [data]
    return []


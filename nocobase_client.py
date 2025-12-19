"""
NocoBase API 封装（偏 Database/Collections/Records）

该模块基于 NocoBase 的“action 风格”API（路径中包含 :action）进行封装，例如：
- 记录（数据行）:
  - /api/test1:create
  - /api/test1:list
  - /api/test1:get
  - /api/test1:update
  - /api/test1:destroy
- 数据表（collection）管理:
  - /api/collections:list
  - /api/collections:get
  - /api/collections:create
  - /api/collections:update
  - /api/collections:destroy
  - /api/collections:move
  - /api/collections:setFields

说明：
1) 不同 NocoBase 版本/插件组合，个别 action 的入参位置可能不同（query params vs json body）。
   这里做了“兼容式尝试”：在常见的两种写法之间自动回退。
2) 你当前站点在 HTTPS/443 可能被代理拦截（之前出现过 SSL/Proxy 错误），因此默认会优先尝试
   “保持原 scheme + 去掉端口”的 base_url 作为候选（例如从 http://host:13001/api 回退到 http://host/api）。
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import requests


def load_env_file(path: str = ".env") -> None:
    """
    从 .env 文件加载环境变量（只在变量不存在时写入 os.environ）。

    文件格式示例：
        NOCOBASE_BASE_URL=http://example.com/api
        NOCOBASE_TOKEN=xxxx

    注意：本函数是一个很轻量的实现，支持：
    - 空行、# 注释行
    - key=value
    - value 两侧的单/双引号
    """

    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'").strip('"')
            os.environ.setdefault(key, value)


def build_fallback_base_urls(base_url: str) -> List[str]:
    """
    基于你提供的 base_url 生成一组候选 base_url，用于“自动回退尝试”。

    为什么需要回退？
    - 现实环境中经常存在反向代理：外部访问走 80/443，而内部真实端口是 13001。
    - 你这里也出现过：`http://域名:13001/api` 会 502，但 `http://域名/api` 可以访问。
    - 同时还可能存在 http/https 的差异。

    返回列表的顺序很重要：越靠前越优先尝试。
    """

    candidates: List[str] = []

    def add(url: str) -> None:
        normalized = url.rstrip("/")
        if normalized and normalized not in candidates:
            candidates.append(normalized)

    add(base_url)

    parsed = urlparse(base_url)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        host = parsed.hostname or ""
        if host:
            # 先尝试同 scheme + 去掉端口（对反向代理最友好）
            add(urlunparse(parsed._replace(netloc=host)))

        swapped_scheme = "https" if parsed.scheme == "http" else "http"
        add(urlunparse(parsed._replace(scheme=swapped_scheme)))
        if host:
            add(urlunparse(parsed._replace(scheme=swapped_scheme, netloc=host)))

    return candidates


@dataclass(frozen=True)
class NocoBaseConfig:
    """
    NocoBase 连接配置。

    - base_url: 必须以 /api 结尾，例如：http://nocobase.xxx.com/api
    - token: API KEY（Bearer Token）
    - timeout: requests 超时秒数
    """

    base_url: str
    token: str
    timeout: int = 30


class NocoBaseClient:
    """
    NocoBase HTTP 客户端 + Database/Collections/Records 常用 API 封装。

    你可以把它当成一个“统一入口”：
    - Records（表数据行）CRUD：create/list/get/update/destroy
    - Collections（表结构/表定义）CRUD：collections_list/collections_get/...
    - 任意 action：action()
    """

    def __init__(self, *, config: NocoBaseConfig) -> None:
        if not config.token or not config.token.strip():
            raise ValueError("config.token 不能为空（NOCOBASE_TOKEN）")
        self.config = config
        self.base_urls = build_fallback_base_urls(config.base_url)

    @classmethod
    def from_env(cls, env_path: str = ".env") -> "NocoBaseClient":
        """
        从 .env/环境变量创建客户端。

        需要配置：
        - NOCOBASE_BASE_URL，例如：http://nocobase.cuixiaoyuan.cn/api
        - NOCOBASE_TOKEN，例如：eyJhbGci...
        - NOCOBASE_TIMEOUT（可选，默认 30）
        """

        load_env_file(env_path)
        base_url = os.getenv("NOCOBASE_BASE_URL", "").strip()
        token = os.getenv("NOCOBASE_TOKEN", "").strip()
        timeout = int(os.getenv("NOCOBASE_TIMEOUT", "30").strip() or "30")
        if not base_url:
            raise ValueError("缺少 NOCOBASE_BASE_URL，例如 http://xxx/api")
        return cls(config=NocoBaseConfig(base_url=base_url, token=token, timeout=timeout))

    @property
    def headers(self) -> Dict[str, str]:
        """统一的鉴权 Header：Authorization: Bearer <token>"""

        return {"Authorization": f"Bearer {self.config.token.strip()}"}

    def _request_once(
        self,
        *,
        method: str,
        base_url: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """向指定 base_url 发起一次请求（不做回退）。"""

        url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
        resp = requests.request(
            method=method,
            url=url,
            headers=self.headers,
            params=params,
            json=json,
            timeout=self.config.timeout,
        )
        if resp.ok:
            return resp.json()
        raise requests.HTTPError(f"{resp.status_code} Error for url: {resp.url}", response=resp)

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        发起请求（带 base_url 回退尝试）。

        用法示例：
            client.request("GET", "collections:list")
            client.request("POST", "test1:create", json={"name": "a"})

        参数：
        - method: "GET"/"POST"/...
        - path: 不需要写 /api 前缀，例如 "collections:list" 或 "test1:create"
        - params: QueryString 参数
        - json: JSON body

        返回：
        - 解析后的 JSON（dict）
        """

        last_exc: Optional[Exception] = None
        first_http_exc: Optional[Exception] = None

        for base_url in self.base_urls:
            try:
                return self._request_once(
                    method=method, base_url=base_url, path=path, params=params, json=json
                )
            except requests.HTTPError as exc:
                # 记录第一个 HTTPError，便于最终报错更贴近真实原因
                if first_http_exc is None:
                    first_http_exc = exc

                # 如果是明确的 4xx（非 404/405），一般说明鉴权/参数有问题，没必要继续换 base_url
                response = getattr(exc, "response", None)
                status = getattr(response, "status_code", None)
                if isinstance(status, int) and status < 500 and status not in {404, 405}:
                    raise
                last_exc = exc
            except Exception as exc:
                last_exc = exc

        raise first_http_exc or last_exc or RuntimeError("request failed")

    def action(
        self,
        *,
        path: str,
        method: str = "POST",
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        通用 action 调用封装（等价于 request，但参数名更贴合“调用 action”）。

        用法示例：
            client.action(path="collections:list", method="GET")
            client.action(path="test1:create", json={"name": "a"})
        """

        return self.request(method, path, params=params, json=json)

    # -------------------------
    # Records: 表数据（增删改查）
    # -------------------------

    def create(self, collection: str, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        创建一条记录（插入一行数据）。

        对应 action：POST /api/<collection>:create

        参数：
        - collection: 数据表标识（例如 "test1"）
        - values: 要写入的字段字典（不要传 id/createdAt 等系统字段）

        返回：
        - NocoBase 标准响应，一般形如：
          {"data": {"id": ..., ...}}
        """

        # 兼容两种常见写法：
        # 1) 顶层字段：{"name": "a"}
        # 2) values 包裹：{"values": {"name": "a"}}
        payloads = [values, {"values": values}]
        last_exc: Optional[Exception] = None
        for payload in payloads:
            try:
                return self.request("POST", f"{collection}:create", json=payload)
            except Exception as exc:
                last_exc = exc
        raise last_exc or RuntimeError("create failed")

    def list(self, collection: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        查询记录列表（分页列表）。

        对应 action：GET /api/<collection>:list

        常用 params（以实际 NocoBase 版本为准）：
        - page, pageSize
        - sort（例如："-createdAt"）
        - filter / filterByTk
        - fields / appends
        """

        return self.request("GET", f"{collection}:list", params=params or {})

    def get(self, collection: str, *, pk: Any, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        查询单条记录。

        对应 action：GET /api/<collection>:get

        参数：
        - pk: 主键值（一般是 id），会以 filterByTk 传入。
        """

        merged = {"filterByTk": pk}
        if params:
            merged.update(params)
        return self.request("GET", f"{collection}:get", params=merged)

    def update(self, collection: str, *, pk: Any, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        更新单条记录（按主键）。

        对应 action：POST /api/<collection>:update

        兼容两种常见写法：
        - QueryString 传 filterByTk，JSON 传字段：POST ...?filterByTk=<id>  body={"name":"new"}
        - QueryString 传 filterByTk，JSON 传 values：POST ...?filterByTk=<id> body={"values":{"name":"new"}}
        """

        payloads = [values, {"values": values}]
        last_exc: Optional[Exception] = None
        for payload in payloads:
            try:
                return self.request(
                    "POST",
                    f"{collection}:update",
                    params={"filterByTk": pk},
                    json=payload,
                )
            except Exception as exc:
                last_exc = exc
        raise last_exc or RuntimeError("update failed")

    def destroy(self, collection: str, *, pk: Any) -> Dict[str, Any]:
        """
        删除单条记录（按主键）。

        对应 action：POST /api/<collection>:destroy

        兼容两种常见写法：
        - JSON：{"filterByTk": <id>}
        - QueryString：?filterByTk=<id>
        """

        last_exc: Optional[Exception] = None
        for mode in ("json", "params"):
            try:
                if mode == "json":
                    return self.request(
                        "POST", f"{collection}:destroy", json={"filterByTk": pk}
                    )
                return self.request("POST", f"{collection}:destroy", params={"filterByTk": pk})
            except Exception as exc:
                last_exc = exc
        raise last_exc or RuntimeError("destroy failed")

    # -----------------------------------
    # Collections: 数据表结构（常用接口）
    # -----------------------------------

    def collections_list(self, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        获取数据表（collections）列表。

        对应 action：GET /api/collections:list

        返回示例（以实际为准）：
            {"data": [{"name":"test1", ...}, ...]}
        """

        return self.request("GET", "collections:list", params=params or {})

    def collections_get(self, *, name: str) -> Dict[str, Any]:
        """
        获取单个数据表（collection）的定义信息。

        对应 action：GET /api/collections:get

        入参通常是 query 参数；在你当前环境中，推荐使用：
        - ?filterByTk=<collectionName>
        并配合：
        - appends=fields（让响应包含字段定义，便于做列映射/校验）

        注意：有些环境下 `?name=` 可能会被忽略但仍返回 200（返回默认/第一张表），
        所以本函数会校验返回的 data.name 是否等于目标 name；不匹配会继续尝试。
        """

        attempts: List[Dict[str, Any]] = [
            {"filterByTk": name, "appends": "fields"},
            {"filterByTk": name},
            {"name": name, "appends": "fields"},
            {"name": name},
        ]
        last_exc: Optional[Exception] = None
        for params in attempts:
            try:
                resp = self.request("GET", "collections:get", params=params)
                data = resp.get("data")
                if isinstance(data, dict) and data.get("name") == name:
                    return resp
                last_exc = RuntimeError("collections_get 返回的 data.name 与期望不一致")
            except Exception as exc:
                last_exc = exc
        raise last_exc or RuntimeError("collections_get failed")

    def collections_create(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        创建一个数据表（collection）。

        对应 action：POST /api/collections:create

        payload 结构请以官方文档为准；一般会包含：
        - name（标识）
        - title（显示名）
        - fields（字段定义）等
        """

        return self.request("POST", "collections:create", json=payload)

    def collections_update(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        更新一个数据表（collection）的定义。

        对应 action：POST /api/collections:update
        payload 结构以官方文档为准。
        """

        return self.request("POST", "collections:update", json=payload)

    def collections_destroy(self, *, name: str) -> Dict[str, Any]:
        """
        删除一个数据表（collection）。

        对应 action：POST /api/collections:destroy

        兼容常见写法：
        - JSON：{"name": "..."} 或 {"filterByTk":"..."}
        - Query：?name=... 或 ?filterByTk=...
        """

        attempts: List[Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]] = [
            (None, {"name": name}),
            (None, {"filterByTk": name}),
            ({"name": name}, None),
            ({"filterByTk": name}, None),
        ]
        last_exc: Optional[Exception] = None
        for params, json_body in attempts:
            try:
                return self.request("POST", "collections:destroy", params=params, json=json_body)
            except Exception as exc:
                last_exc = exc
        raise last_exc or RuntimeError("collections_destroy failed")

    def collections_move(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        调整数据表顺序/分组（具体能力取决于 NocoBase 版本与实现）。

        对应 action：POST /api/collections:move
        payload 结构以官方文档为准。
        """

        return self.request("POST", "collections:move", json=payload)

    def collections_set_fields(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        批量设置某个数据表的字段（常用于快速同步/更新字段定义）。

        对应 action：POST /api/collections:setFields
        payload 结构以官方文档为准。
        """

        return self.request("POST", "collections:setFields", json=payload)


def _is_empty_cell(value: Any) -> bool:
    if value is None:
        return True
    try:
        import pandas as pd  # type: ignore

        return bool(pd.isna(value))
    except Exception:
        pass
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _to_python_scalar(value: Any) -> Any:
    if _is_empty_cell(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def _convert_by_field_type(value: Any, field_def: Optional[Dict[str, Any]]) -> Any:
    """
    根据字段定义尽量把 Excel 单元格值转换为更合适的类型。

    目前处理：日期时间、整数、浮点数，其它保持原样（或转为字符串）。
    """

    v = _to_python_scalar(value)
    if v is None or not field_def:
        return v

    field_type = field_def.get("type")

    # 日期时间：尽量输出为字符串（NocoBase 一般可接受）
    if field_type in {"date", "datetime", "datetimeNoTz"}:
        try:
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
            if isinstance(v, float) and v.is_integer():
                return int(v)
            if isinstance(v, str) and v.strip().isdigit():
                return int(v.strip())
            return int(float(v))
        except Exception:
            return v

    return v


def _extract_fields_from_collection_get(resp: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
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
    titles: List[str] = []
    for key in ("title", "label"):
        v = field_def.get(key)
        if isinstance(v, str) and v.strip():
            titles.append(v.strip())
    ui = field_def.get("uiSchema")
    if isinstance(ui, dict):
        v = ui.get("title")
        if isinstance(v, str) and v.strip():
            titles.append(v.strip())

    seen = set()
    uniq: List[str] = []
    for t in titles:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return tuple(uniq)


def build_excel_field_mapping(
    *,
    excel_columns: Iterable[str],
    collection_fields: Dict[str, Dict[str, Any]],
    explicit_mapping: Optional[Dict[str, str]] = None,
    exclude_field_types: Optional[Iterable[str]] = None,
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    构建 Excel 列名 -> NocoBase 字段标识 的映射。

    优先级：
    1) explicit_mapping（你手工指定的 mapping）
    2) Excel 列名 == 字段标识（field.name）
    3) Excel 列名 == 字段标题（field.title / field.uiSchema.title）

    返回：
    - mapping: {Excel列名: 字段标识}
    - reasons: {Excel列名: 命中原因}
    """

    mapping: Dict[str, str] = {}
    reasons: Dict[str, str] = {}
    excluded = set(exclude_field_types or [])

    def allowed_field(field_name: str) -> bool:
        if not excluded:
            return True
        fdef = collection_fields.get(field_name) or {}
        ftype = fdef.get("type")
        return ftype not in excluded

    # 1) 手工映射
    if explicit_mapping:
        for excel_col, field_name in explicit_mapping.items():
            if excel_col in mapping:
                continue
            if field_name in collection_fields:
                mapping[excel_col] = field_name
                reasons[excel_col] = "explicit"

    # 2) 列名直接等于字段标识
    for col in excel_columns:
        if col in mapping:
            continue
        if col in collection_fields and allowed_field(col):
            mapping[col] = col
            reasons[col] = "match_field_name"

    # 3) 列名等于字段标题（避免歧义：同标题只取第一个）
    title_to_name: Dict[str, str] = {}
    for name, fdef in collection_fields.items():
        if not allowed_field(name):
            continue
        for title in _field_titles(fdef):
            if title not in title_to_name:
                title_to_name[title] = name

    for col in excel_columns:
        if col in mapping:
            continue
        target = title_to_name.get(col)
        if target:
            mapping[col] = target
            reasons[col] = "match_field_title"

    return mapping, reasons


def import_excel_to_collection(
    *,
    client: NocoBaseClient,
    collection: str,
    excel_path: str,
    sheet: Any = 0,
    limit: int = 0,
    dry_run: bool = False,
    explicit_mapping: Optional[Dict[str, str]] = None,
    resolve_belongs_to: bool = True,
    create_missing_belongs_to: bool = False,
) -> Dict[str, Any]:
    """
    读取 Excel 并把每一行新增到指定数据表（collection）。

    你想要的“只输入表名 + Excel 地址即可导入”的入口就是这个函数：
      - collection：表标识（例如 qjzb_orders）
      - excel_path：Excel 文件路径（例如 .\\data\\订单列表.xlsx）

    参数：
    - client: NocoBaseClient 实例（已配置 base_url/token）
    - collection: 目标数据表标识（例如 qjzb_orders）
    - excel_path: Excel 路径（xlsx/xlsm/xls）
    - sheet: sheet 名称或序号（默认 0）
    - limit: 只导入前 N 行（0 表示全部）
    - dry_run: True 时不写入，只打印/返回将写入的 values
    - explicit_mapping: 手工映射（Excel列名 -> 字段标识），用于覆盖/补充自动映射
    - resolve_belongs_to: 是否把 Excel 里的字符串值解析为 belongsTo 外键 ID（默认 True）
    - create_missing_belongs_to: 当 belongsTo 目标表里找不到该字符串时，是否自动创建（默认 False）

    返回：
    - dict，包含：success/failed/total/mapping/unmapped 等统计信息。
    """

    try:
        import pandas as pd  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("缺少依赖 pandas，请先 pip install -r requirements.txt") from exc

    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"Excel 文件不存在：{excel_path}")

    df = pd.read_excel(excel_path, sheet_name=sheet, dtype=object)
    df = df.dropna(axis=0, how="all")
    df = df.dropna(axis=1, how="all")
    if df.empty:
        raise ValueError("Excel 为空或只有空行/空列")

    col_resp = client.collections_get(name=collection)
    fields = _extract_fields_from_collection_get(col_resp)

    exclude_types = None
    if not resolve_belongs_to:
        # 不解析关联字段时，默认不自动映射关联字段，避免把文本写进外键导致失败或脏数据。
        exclude_types = {"belongsTo", "belongsToMany", "hasMany", "hasOne"}

    mapping, reasons = build_excel_field_mapping(
        excel_columns=[str(c) for c in df.columns],
        collection_fields=fields,
        explicit_mapping=explicit_mapping,
        exclude_field_types=exclude_types,
    )
    unmapped = [str(c) for c in df.columns if str(c) not in mapping]

    total = len(df) if limit <= 0 else min(len(df), limit)
    success = 0
    failed = 0
    preview: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    belongs_to_cache: Dict[Tuple[str, str, str], Any] = {}

    def resolve_belongs_to_fk(*, field_def: Dict[str, Any], display_value: Any) -> Any:
        target = field_def.get("target")
        foreign_key = field_def.get("foreignKey")
        if not isinstance(target, str) or not target:
            raise RuntimeError("belongsTo 缺少 target")
        if not isinstance(foreign_key, str) or not foreign_key:
            raise RuntimeError("belongsTo 缺少 foreignKey")

        label = str(display_value).strip()
        if not label:
            return None

        # 确定 target 的“标题字段”（通常是 name/unit/title 等）
        target_def = client.collections_get(name=target).get("data") or {}
        title_field = target_def.get("titleField")
        if not isinstance(title_field, str) or not title_field:
            title_field = "name"

        cache_key = (target, title_field, label)
        if cache_key in belongs_to_cache:
            return belongs_to_cache[cache_key]

        # 为了兼容不同版本的 filter 语法，这里先用小表全量扫描（常用于字典表）
        rows = client.list(target, params={"page": 1, "pageSize": 2000}).get("data") or []
        if isinstance(rows, list):
            for r in rows:
                if isinstance(r, dict) and str(r.get(title_field, "")).strip() == label:
                    pk = r.get("id")
                    belongs_to_cache[cache_key] = pk
                    return pk

        if create_missing_belongs_to:
            created = client.create(target, {title_field: label})
            pk = (created.get("data") or {}).get("id")
            belongs_to_cache[cache_key] = pk
            return pk

        raise RuntimeError(f"belongsTo 未找到目标记录：{target}.{title_field} == {label}")

    for i in range(total):
        row = df.iloc[i]
        values: Dict[str, Any] = {}
        for excel_col, field_name in mapping.items():
            field_def = fields.get(field_name)
            raw = row.get(excel_col)

            # belongsTo：把 Excel 的字符串解析为外键 ID，写入 foreignKey
            if resolve_belongs_to and isinstance(field_def, dict) and field_def.get("type") == "belongsTo":
                try:
                    v0 = _to_python_scalar(raw)
                    if v0 is None:
                        continue
                    fk_value = resolve_belongs_to_fk(field_def=field_def, display_value=v0)
                    fk_name = field_def.get("foreignKey")
                    if fk_value is None or not isinstance(fk_name, str) or not fk_name:
                        continue
                    values[fk_name] = fk_value
                    continue
                except Exception as exc:
                    # belongsTo 解析失败，记录错误并让这一行失败
                    raise RuntimeError(f"belongsTo 字段解析失败：{field_name}({excel_col})：{exc}") from exc

            v = _convert_by_field_type(raw, field_def)
            if v is None:
                continue
            values[field_name] = v

        if not values:
            continue

        if dry_run:
            preview.append(values)
            success += 1
            continue

        try:
            resp = client.create(collection, values)
            # 有些场景后端会返回 200 但 body 含 errors；这里将其视为失败
            if isinstance(resp, dict) and resp.get("errors"):
                raise RuntimeError(resp.get("errors"))
            data = resp.get("data") if isinstance(resp, dict) else None
            if not isinstance(data, dict) or not data.get("id"):
                raise RuntimeError(f"create 返回异常：{resp}")
            success += 1
        except Exception as exc:
            failed += 1
            err: Dict[str, Any] = {"row": i + 1, "error": str(exc), "values": values}
            resp = getattr(exc, "response", None)
            if resp is not None:
                err["status_code"] = getattr(resp, "status_code", None)
                try:
                    err["response_text"] = resp.text
                except Exception:
                    pass
            errors.append(err)

    return {
        "excel_path": excel_path,
        "sheet": sheet,
        "collection": collection,
        "rows": int(len(df)),
        "cols": int(len(df.columns)),
        "mapping": mapping,
        "mapping_reasons": reasons,
        "unmapped": unmapped,
        "total": int(total),
        "success": int(success),
        "failed": int(failed),
        "errors": errors,
        "preview": preview if dry_run else None,
    }

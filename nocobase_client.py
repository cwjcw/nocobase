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

        入参通常是 query 参数，常见写法可能是：
        - ?name=<collectionName>
        - ?filterByTk=<collectionName>

        本函数会自动按上述顺序尝试。
        """

        attempts: List[Dict[str, Any]] = [{"name": name}, {"filterByTk": name}]
        last_exc: Optional[Exception] = None
        for params in attempts:
            try:
                return self.request("GET", "collections:get", params=params)
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


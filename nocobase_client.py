import os
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

import requests


def load_env_file(path: str = ".env") -> None:
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
            # Prefer same scheme first (common: external 80/443 reverse-proxy).
            add(urlunparse(parsed._replace(netloc=host)))

        swapped_scheme = "https" if parsed.scheme == "http" else "http"
        add(urlunparse(parsed._replace(scheme=swapped_scheme)))
        if host:
            add(urlunparse(parsed._replace(scheme=swapped_scheme, netloc=host)))

    return candidates


class NocoBaseClient:
    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        timeout: int = 30,
        wrap_values: bool = False,
    ) -> None:
        self.base_urls = build_fallback_base_urls(base_url)
        self.token = token.strip()
        self.timeout = timeout
        self.wrap_values = wrap_values

        if not self.token:
            raise ValueError("token is required")

    @property
    def headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        last_exc: Optional[Exception] = None
        first_http_exc: Optional[Exception] = None
        for base_url in self.base_urls:
            url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
            try:
                resp = requests.request(
                    method,
                    url,
                    headers=self.headers,
                    params=params,
                    json=json,
                    timeout=self.timeout,
                )
                if resp.ok:
                    return resp.json()

                http_exc = requests.HTTPError(
                    f"{resp.status_code} Error for url: {resp.url}", response=resp
                )

                if first_http_exc is None:
                    first_http_exc = http_exc

                # If the first candidate returns a 4xx (except 404/405), stop early:
                # This usually means auth/payload issues, not a wrong base_url.
                if resp.status_code < 500 and resp.status_code not in {404, 405}:
                    raise http_exc

                last_exc = http_exc
                continue
            except Exception as exc:
                last_exc = exc

        raise first_http_exc or last_exc or RuntimeError("request failed")

    def create(self, collection: str, values: Dict[str, Any]) -> Dict[str, Any]:
        payloads: List[Dict[str, Any]] = []
        if self.wrap_values:
            payloads.append({"values": values})
            payloads.append(values)
        else:
            payloads.append(values)
            payloads.append({"values": values})

        last_exc: Optional[Exception] = None
        for payload in payloads:
            try:
                return self._request("POST", f"{collection}:create", json=payload)
            except Exception as exc:
                last_exc = exc
        raise last_exc or RuntimeError("create failed")

    def list(
        self,
        collection: str,
        *,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self._request("GET", f"{collection}:list", params=params or {})

    def get(
        self,
        collection: str,
        *,
        pk: Any,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        merged = {"filterByTk": pk}
        if params:
            merged.update(params)
        return self._request("GET", f"{collection}:get", params=merged)

    def update(self, collection: str, *, pk: Any, values: Dict[str, Any]) -> Dict[str, Any]:
        payloads: List[Dict[str, Any]] = []
        if self.wrap_values:
            payloads.append({"values": values})
            payloads.append(values)
        else:
            payloads.append(values)
            payloads.append({"values": values})

        last_exc: Optional[Exception] = None
        for payload in payloads:
            try:
                return self._request(
                    "POST",
                    f"{collection}:update",
                    params={"filterByTk": pk},
                    json=payload,
                )
            except Exception as exc:
                last_exc = exc
        raise last_exc or RuntimeError("update failed")

    def destroy(self, collection: str, *, pk: Any) -> Dict[str, Any]:
        last_exc: Optional[Exception] = None
        for mode in ("json", "params"):
            try:
                if mode == "json":
                    return self._request(
                        "POST", f"{collection}:destroy", json={"filterByTk": pk}
                    )
                return self._request(
                    "POST", f"{collection}:destroy", params={"filterByTk": pk}
                )
            except Exception as exc:
                last_exc = exc
        raise last_exc or RuntimeError("destroy failed")

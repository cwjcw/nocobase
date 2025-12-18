# NocoBase Database API 封装说明

本仓库对 NocoBase v2 的 Database/Collections/Records 常用接口做了轻量封装，代码在：

- `nocobase_client.py`

你可以把它理解为两类能力：

1) **Records（记录/数据行）CRUD**：对某个数据表（collection）增删改查数据
2) **Collections（数据表定义）管理**：列出/获取/创建/更新/删除数据表及字段

> 说明：不同 NocoBase 版本、插件组合可能导致某些 action 的参数位置略有差异。
> 这个封装对常见差异做了“自动兼容尝试”（例如 body 顶层字段 vs body.values 包裹）。

---

## 0. 初始化

### 0.1 使用 `.env`

`.env` 只放连接信息，不放表名：

```env
NOCOBASE_BASE_URL=http://nocobase.cuixiaoyuan.cn/api
NOCOBASE_TOKEN=你的API_KEY
```

代码：

```python
from nocobase_client import NocoBaseClient

client = NocoBaseClient.from_env(".env")
```

### 0.2 手动传参

```python
from nocobase_client import NocoBaseClient, NocoBaseConfig

client = NocoBaseClient(
    config=NocoBaseConfig(
        base_url="http://nocobase.cuixiaoyuan.cn/api",
        token="你的API_KEY",
        timeout=30,
    )
)
```

---

## 1. Records（数据行）CRUD

这里的 `collection` 指“数据表标识”，例如你截图里的“测试”表标识为 `test1`。

### 1.1 `create(collection, values)`

创建一条记录（插入一行数据）。

```python
resp = client.create("test1", {"name": "张三", "f_h2v1n6u8mfh": 12.34})
new_id = resp["data"]["id"]
```

### 1.2 `list(collection, params=None)`

查询记录列表（分页/过滤/排序等由 `params` 控制，具体字段以你 NocoBase 版本为准）。

```python
resp = client.list("test1", params={"page": 1, "pageSize": 20, "sort": "-createdAt"})
rows = resp["data"]
```

### 1.3 `get(collection, pk, params=None)`

按主键查询单条记录（内部用 `filterByTk` 传主键）。

```python
resp = client.get("test1", pk=new_id)
row = resp["data"]
```

### 1.4 `update(collection, pk, values)`

按主键更新记录（内部会尝试两种常见 body 结构：顶层字段 / `values` 包裹）。

> 注意：不同版本 `update` 的返回结构可能是对象或数组，你可以按实际返回解析。

```python
resp = client.update("test1", pk=new_id, values={"name": "李四"})
```

### 1.5 `destroy(collection, pk)`

按主键删除记录。

```python
resp = client.destroy("test1", pk=new_id)
```

---

## 2. Collections（数据表定义）管理

### 2.1 `collections_list(params=None)`

获取数据表列表。

```python
resp = client.collections_list()
tables = resp["data"]
```

### 2.2 `collections_get(name)`

获取单个数据表定义信息（内部会尝试 `?name=` 与 `?filterByTk=` 两种传参）。

```python
resp = client.collections_get(name="test1")
table = resp["data"]
```

### 2.3 `collections_create(payload)`

创建数据表（payload 结构以官方文档为准）。

```python
payload = {
  "name": "demo",
  "title": "演示表",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "count", "type": "integer"},
  ],
}
resp = client.collections_create(payload)
```

### 2.4 `collections_update(payload)`

更新数据表定义（payload 结构以官方文档为准）。

```python
resp = client.collections_update({"name": "demo", "title": "演示表-改名"})
```

### 2.5 `collections_destroy(name)`

删除数据表（内部会尝试 json/query 的 `name` 与 `filterByTk` 多种写法）。

```python
resp = client.collections_destroy(name="demo")
```

### 2.6 `collections_move(payload)`

调整数据表顺序/分组（payload 结构以官方文档为准）。

```python
resp = client.collections_move({"name": "demo", "after": "test1"})
```

### 2.7 `collections_set_fields(payload)`

批量设置字段（payload 结构以官方文档为准）。

```python
resp = client.collections_set_fields({
  "name": "demo",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "count", "type": "integer"},
  ],
})
```

---

## 3. 通用调用：`action(...)`

当你在 Swagger/文档里看到某个 action 但封装里还没有对应函数时，直接用通用方法即可：

```python
resp = client.action(path="collections:list", method="GET")
resp = client.action(path="test1:create", method="POST", json={"name": "x"})
```


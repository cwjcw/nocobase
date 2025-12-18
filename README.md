# nocobase

用于存放 NocoBase API 调用示例与封装代码（Database/Collections/Records 增删改查）。

## 环境准备（建议虚拟环境）

Windows PowerShell：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 配置

复制 `.env.example` 为 `.env`，并填写：

- `NOCOBASE_BASE_URL`：例如 `http://nocobase.cuixiaoyuan.cn/api`
- `NOCOBASE_TOKEN`：你的 API KEY（Bearer Token）

## 使用示例：写入一条数据

往 `test1` 表写入 `name` 与 `数量`（字段标识 `f_h2v1n6u8mfh`）：

```powershell
python .\write_test1.py --collection test1 --name "测试数据" --quantity 12.34
```

## API 封装（CRUD）

封装文件：`nocobase_client.py`

更详细的每个函数说明见：`DATABASE_API.md`

示例代码（创建/查询/更新/删除）：

```python
import os
from nocobase_client import NocoBaseClient

client = NocoBaseClient.from_env(".env")

created = client.create("test1", {"name": "hello", "f_h2v1n6u8mfh": 1.23})
pk = created["data"]["id"]

print(client.get("test1", pk=pk))
print(client.update("test1", pk=pk, values={"name": "world"}))
print(client.destroy("test1", pk=pk))
```

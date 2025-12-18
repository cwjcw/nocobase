# nocobase

用于存放 NocoBase v2 Database API 的 Python 封装与通用命令行工具（对任意表/任意 action 增删改查）。

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

## 推荐：通用命令行（改参数即可）

### 对任意表增删改查

```powershell
# 创建
python .\nocobase_cli.py records create --collection test1 --set name=测试数据 --set f_h2v1n6u8mfh=12.34

# 查列表
python .\nocobase_cli.py records list --collection test1 --param page=1 --param pageSize=10

# 查列表并用表格输出
python .\nocobase_cli.py records list --collection test1 --param page=1 --param pageSize=10 --table --columns id,name,f_h2v1n6u8mfh,createdAt

# 查单条（按 id）
python .\nocobase_cli.py records get --collection test1 --pk 123

# 更新
python .\nocobase_cli.py records update --collection test1 --pk 123 --set name=新名字

# 删除
python .\nocobase_cli.py records destroy --collection test1 --pk 123
```

### 调用任意 action

```powershell
python .\nocobase_cli.py action --method GET --path collections:list
python .\nocobase_cli.py action --method POST --path test1:create --set name=x
```

## Python 封装（可集成到你的脚本/服务）

封装文件：`nocobase_client.py`

```python
from nocobase_client import NocoBaseClient

client = NocoBaseClient.from_env(".env")
resp = client.create("test1", {"name": "hello", "f_h2v1n6u8mfh": 1.23})
print(resp)
```

## 文档

每个函数的用法说明与可运行示例：`example.py`

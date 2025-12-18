import argparse
import os

from nocobase_client import NocoBaseClient, load_env_file


def main() -> int:
    load_env_file(".env")

    parser = argparse.ArgumentParser(description="向 NocoBase 指定数据表写入一条数据")
    parser.add_argument("--base-url", default=None, help="API Base URL，例如 http://域名/api")
    parser.add_argument("--collection", required=True, help="数据表标识，例如 test1")
    parser.add_argument("--name", required=True, help="字段：name")
    parser.add_argument("--quantity", type=float, required=True, help="字段：数量")
    args = parser.parse_args()

    base_url = args.base_url or os.getenv("NOCOBASE_BASE_URL", "http://localhost:13001/api")
    token = os.getenv("NOCOBASE_TOKEN", "").strip()
    wrap_values = os.getenv("NOCOBASE_WRAP_VALUES", "0").strip().lower() in {"1", "true", "yes", "y"}

    if not token:
        raise SystemExit("缺少 NOCOBASE_TOKEN，请在 .env 或系统环境变量中配置")

    client = NocoBaseClient(base_url=base_url, token=token, wrap_values=wrap_values)
    result = client.create(
        args.collection,
        values={
            "name": args.name,
            "f_h2v1n6u8mfh": args.quantity,
        },
    )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


import argparse

from nocobase_client import NocoBaseClient, NocoBaseConfig


def main() -> int:
    parser = argparse.ArgumentParser(description="向 NocoBase 指定数据表写入一条数据")
    parser.add_argument("--base-url", default=None, help="API Base URL，例如 http://域名/api")
    parser.add_argument("--collection", required=True, help="数据表标识，例如 test1")
    parser.add_argument("--name", required=True, help="字段：name")
    parser.add_argument("--quantity", type=float, required=True, help="字段：数量")
    args = parser.parse_args()

    client = NocoBaseClient.from_env(".env")
    if args.base_url:
        client = NocoBaseClient(
            config=NocoBaseConfig(
                base_url=args.base_url,
                token=client.config.token,
                timeout=client.config.timeout,
            )
        )

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

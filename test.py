from nocobase_client import NocoBaseClient, import_excel_to_collection

client = NocoBaseClient.from_env(".env")
result = import_excel_to_collection(
    client=client,
    collection="qjzb_orders",
    excel_path=r".\data\订单列表.xlsx",
)
print(result)


def build_parquet_file_location(destination_bucket: str, table_name: str) -> str:
    parquet_path = f"{destination_bucket}/{table_name}/{table_name}*.parquet"
    return parquet_path

def flatten_table(destination_bucket: str, project_id: str, dataset_id: str, table_name: str) -> None:
    parquet_path = build_parquet_file_location(destination_bucket, table_name)
    
    print()
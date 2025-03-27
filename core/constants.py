from enum import Enum

SERVICE_NAME = "flattener"

EXPORT_PARQUET_COMPRESSION="snappy"

DUCKDB_FORMAT_STRING = "(FORMAT 'parquet', COMPRESSION 'zstd')"
DUCKDB_MEMORY_LIMIT = "10GB"
DUCKDB_MAX_SIZE = "5000GB"
DUCKDB_THREADS = "4"

IGNORE_FIELDS = [
    '__key__', '__error__', '__has_error__', 'treeJSON', 'namespace', 'app', 'path',
     'kind', 'name', 'id', 'COMPLETED', 'COMPLETED_TS', 'sha', '569151507', 'D_726699695_V2',
     'Module2', 'undefined', 'key', 'query', 'D_726699695','D_299215535', 'D_166676176'
]

# Helps keep track of the fields with one-off/special handling
class SPECIAL_LOGIC_FIELDS(str, Enum):
    D_470862706 = "D_470862706"
    d_110349197 = "d_110349197"
    d_543608829 = "d_543608829"

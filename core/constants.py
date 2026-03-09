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

class SPECIAL_LOGIC_TABLES(str, Enum):
    BOXES = "boxes"

BOXES_BAG_FIELDS = [
    "d_650224161", "d_136341211", "d_503046679", "d_313341808", "d_668816010",
    "d_754614551", "d_174264982", "d_550020510", "d_673090642", "d_492881559",
    "d_536728814", "d_309413330", "d_357218702", "d_945294744", "d_741697447",
]

BOXES_PARENT_FIELDS = [
    "d_672863981", "d_560975149", "d_842312685", "d_132929440", "d_789843387",
    "d_555611076", "d_145971562", "d_959708259", "d_948887825", "d_666553960",
    "d_885486943", "d_656548982", "d_105891443", "d_238268405", "d_870456401",
    "d_926457119", "d_333524031",
]

BOXES_BAG_TYPE_FIELDS = [
    ("d_787237543", "Blood/Urine"),
    ("d_223999569", "Mouth wash"),
    ("d_522094118", "Orphan bag"),
]

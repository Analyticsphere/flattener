import fnmatch
import os
import shutil
import tempfile
from typing import Any, Optional

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from google.cloud import storage  # type: ignore

import core.constants as constants
import core.utils as utils


BOXES_KEY_STRUCT = pa.struct(
    [
        pa.field("namespace", pa.string()),
        pa.field("app", pa.string()),
        pa.field("path", pa.string()),
        pa.field("kind", pa.string()),
        pa.field("name", pa.string()),
        pa.field("id", pa.int64()),
    ]
)


def read_varint(buf: bytes, pos: int) -> tuple[Optional[int], int]:
    """Read a protobuf-style varint starting at ``pos``."""
    value = 0
    shift = 0

    while pos < len(buf):
        byte = buf[pos]
        pos += 1
        value |= (byte & 0x7F) << shift

        if not (byte & 0x80):
            return value, pos

        shift += 7
        if shift > 63:
            return None, pos

    return None, pos


def parse_message(buf: bytes) -> Optional[list[tuple[int, int, Any]]]:
    """Parse a Firestore/protobuf-like blob into ``(field_no, wire_type, value)`` tuples."""
    out: list[tuple[int, int, Any]] = []
    pos = 0
    end = len(buf)

    while pos < end:
        tag, pos = read_varint(buf, pos)
        if tag is None:
            return None

        field_no = tag >> 3
        wire_type = tag & 0x07
        if field_no == 0:
            return None

        if wire_type == 0:
            value, pos = read_varint(buf, pos)
            if value is None:
                return None
            out.append((field_no, wire_type, value))
        elif wire_type == 1:
            if pos + 8 > end:
                return None
            out.append((field_no, wire_type, buf[pos : pos + 8]))
            pos += 8
        elif wire_type == 2:
            length, pos = read_varint(buf, pos)
            if length is None or pos + length > end:
                return None
            out.append((field_no, wire_type, buf[pos : pos + length]))
            pos += length
        elif wire_type == 5:
            if pos + 4 > end:
                return None
            out.append((field_no, wire_type, buf[pos : pos + 4]))
            pos += 4
        else:
            return None

    return out


def maybe_text(buf: bytes) -> Optional[str]:
    """Treat binary data as text only when it decodes cleanly and is mostly printable."""
    try:
        text = buf.decode("utf-8")
    except UnicodeDecodeError:
        return None

    if not text:
        return ""

    printable = sum(ch.isprintable() for ch in text)
    if printable / len(text) < 0.9:
        return None

    return text


def decode_value_message(buf: bytes) -> Any:
    """Decode the Firestore value wrapper used inside map entries."""
    msg = parse_message(buf)
    if msg is None or not msg:
        return None

    for field_no, wire_type, value in msg:
        if field_no == 1 and wire_type == 0:
            return int(value)
        if field_no == 4 and wire_type == 0:
            return bool(value)

    for field_no, wire_type, value in msg:
        if field_no == 3 and wire_type == 2:
            # String values are often wrapped one level deeper in these blobs.
            nested = parse_message(value)
            if nested is not None and len(nested) == 1 and nested[0][0] == 3 and nested[0][1] == 2:
                nested_text = maybe_text(nested[0][2])
                if nested_text is not None:
                    return nested_text

            text = maybe_text(value)
            if text is not None:
                return text

    nested_map = decode_blob_map(buf)
    if nested_map:
        return nested_map

    return None


def decode_blob_map(blob: bytes) -> dict[str, Any]:
    """Decode a Firestore document map blob into a plain Python dictionary."""
    top = parse_message(blob)
    if top is None:
        return {}

    out: dict[str, Any] = {}
    for field_no, wire_type, value in top:
        if field_no != 15 or wire_type != 2:
            continue

        entry = parse_message(value)
        if entry is None:
            continue

        key: Optional[str] = None
        parsed_value: Any = None
        for entry_field_no, entry_wire_type, entry_value in entry:
            if entry_field_no == 3 and entry_wire_type == 2:
                key = maybe_text(entry_value)
            elif entry_field_no == 5 and entry_wire_type == 2:
                parsed_value = decode_value_message(entry_value)

        if not key:
            continue

        if key in out:
            if not isinstance(out[key], list):
                out[key] = [out[key]]
            out[key].append(parsed_value)
        else:
            out[key] = parsed_value

    return out


def get_boxes_bag_arrow_type(column_name: str) -> Optional[pa.DataType]:
    """Return the typed Arrow schema for bag columns that must be decoded from BLOB."""
    if column_name not in constants.BOXES_BAG_FIELDS:
        return None

    fields = [
        pa.field("d_522094118", pa.string()),
        pa.field("d_787237543", pa.string()),
    ]

    if column_name in constants.BOXES_BAG_FIELDS_WITH_TRACKING_ID:
        fields.append(pa.field("d_618036638", pa.string()))

    fields.extend(
        [
            pa.field("d_469819603", pa.string()),
            pa.field("d_234868461", pa.list_(pa.string())),
            pa.field("d_255283733", pa.int64()),
            pa.field("d_223999569", pa.string()),
            pa.field("__key__", BOXES_KEY_STRUCT),
        ]
    )

    return pa.struct(fields)


def convert_to_arrow_type(value: Any, target_type: pa.DataType) -> Any:
    """Coerce decoded Python values into the Arrow type expected in the converted Parquet."""
    if value is None:
        return None

    if pa.types.is_struct(target_type):
        if not isinstance(value, dict):
            return None

        struct_value: dict[str, Any] = {}
        struct_type = pa.struct(target_type)
        for field in struct_type:
            key = field.name[2:] if field.name.startswith("d_") else field.name
            struct_value[field.name] = convert_to_arrow_type(value.get(key), field.type)

        return struct_value

    if pa.types.is_list(target_type):
        item_type = target_type.value_type
        if not isinstance(value, list):
            value = [value]
        return [convert_to_arrow_type(item, item_type) for item in value if item is not None]

    if pa.types.is_string(target_type):
        if isinstance(value, list):
            return str(value[0]) if value else None
        return str(value)

    if pa.types.is_integer(target_type):
        if isinstance(value, list):
            value = value[0] if value else None
        if value is None:
            return None

        try:
            return int(value)
        except Exception:
            return None

    if pa.types.is_boolean(target_type):
        if isinstance(value, list):
            value = value[0] if value else None
        if value is None:
            return None
        return bool(value)

    return value


def convert_boxes_parquet_file(input_path: str, output_path: str) -> tuple[list[str], bool]:
    """
    Convert a single boxes Parquet file from raw BLOB bag columns to typed STRUCT columns.

    Files without BLOB columns are copied unchanged so the converted directory can still be
    treated as the single source for downstream flattening.
    """
    table = pq.read_table(input_path)
    has_blob_columns = any(pa.types.is_binary(field.type) for field in table.schema)

    if not has_blob_columns:
        shutil.copyfile(input_path, output_path)
        return [], False

    arrays: list[pa.Array] = []
    names: list[str] = []
    converted_columns: list[str] = []

    for field in table.schema:
        col_name = field.name
        column = table[col_name]
        target_type = get_boxes_bag_arrow_type(col_name)

        if pa.types.is_binary(field.type) and target_type is not None:
            # Only known bag columns are decoded. Other BLOB columns are preserved as-is.
            decoded_values: list[Any] = []
            for blob in column.to_pylist():
                if blob is None:
                    decoded_values.append(None)
                    continue

                decoded_values.append(convert_to_arrow_type(decode_blob_map(blob), target_type))

            arrays.append(pa.array(decoded_values, type=target_type))
            names.append(col_name)
            converted_columns.append(col_name)
            continue

        arrays.append(column)
        names.append(col_name)

    if not converted_columns:
        shutil.copyfile(input_path, output_path)
        return [], True

    output_table = pa.table(arrays, names=names)
    pq.write_table(output_table, output_path, compression=constants.EXPORT_PARQUET_COMPRESSION)

    return converted_columns, True


def get_matching_gcs_blobs(gcs_path_pattern: str) -> tuple[storage.Bucket, list[storage.Blob]]:
    """Expand a single GCS wildcard pattern into the concrete blobs that match it."""
    import core.gcp_client as gcp_client

    bucket_name, blob_pattern = gcp_client.parse_gcs_path(gcs_path_pattern)
    blob_prefix = blob_pattern.split("*", 1)[0]

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = [
        blob
        for blob in bucket.list_blobs(prefix=blob_prefix)
        if fnmatch.fnmatch(blob.name, blob_pattern)
    ]

    return bucket, sorted(blobs, key=lambda blob: blob.name)


def convert_boxes_table_files(destination_bucket: str, table_name: str) -> None:
    """
    Convert every raw boxes Parquet part file into a parallel file under ``converted/``.

    The flatten step reads from that directory so both converted and already-string files
    follow the same downstream path.
    """
    import core.gcp_client as gcp_client

    raw_file_pattern = utils.get_raw_parquet_file_location(destination_bucket, table_name)
    converted_directory = utils.get_converted_parquet_directory(destination_bucket, table_name)
    _, converted_prefix = gcp_client.parse_gcs_path(converted_directory)

    bucket, raw_blobs = get_matching_gcs_blobs(raw_file_pattern)
    if not raw_blobs:
        raise Exception(f"No raw Parquet files found for {table_name} at {raw_file_pattern}")
    
    # Ensure task is idempotent by clearing any previously converted files before writing new ones
    gcp_client.delete_from_gcs_path(converted_directory)

    converted_file_count = 0
    copied_file_count = 0

    with tempfile.TemporaryDirectory() as tmp_dir:
        for raw_blob in raw_blobs:
            local_input_path = os.path.join(tmp_dir, os.path.basename(raw_blob.name))
            local_output_path = os.path.join(tmp_dir, f"converted_{os.path.basename(raw_blob.name)}")

            raw_blob.download_to_filename(local_input_path)

            converted_columns, had_blob_columns = convert_boxes_parquet_file(
                local_input_path,
                local_output_path,
            )

            # Keep the original `part` filename so wildcard reads preserve the raw file fan-out
            # expected in get_converted_parquet_file_location()
            output_blob = bucket.blob(f"{converted_prefix}/{os.path.basename(raw_blob.name)}")
            output_blob.upload_from_filename(local_output_path)

            if converted_columns:
                converted_file_count += 1
                utils.logger.info(
                    f"Converted {raw_blob.name} columns {converted_columns} and saved to {output_blob.name}"
                )
            else:
                copied_file_count += 1
                if had_blob_columns:
                    utils.logger.info(
                        f"No convertible boxes BLOB columns found in {raw_blob.name}; copied unchanged"
                    )
                else:
                    utils.logger.info(f"No BLOB columns found in {raw_blob.name}; copied unchanged")

    utils.logger.info(
        f"Prepared converted boxes Parquet files for {table_name}: "
        f"{converted_file_count} converted, {copied_file_count} copied"
    )


def convert_table_file(destination_bucket: str, table_name: str) -> None:
    """Dispatch table-specific conversion logic before flattening."""
    if table_name.lower() != constants.SPECIAL_LOGIC_TABLES.BOXES.value:
        utils.logger.info(f"No conversion required for {table_name}")
        return

    convert_boxes_table_files(destination_bucket, table_name)

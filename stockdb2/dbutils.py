"""Utility helpers for stockdb2 data parsing and conversion."""

import csv
import datetime as dt
import io
import re
from typing import Dict, Iterable, List, Mapping, Optional, cast

try:
    import xlrd
except ImportError:  # pragma: no cover - optional runtime dependency
    xlrd = None

_DATE_PATTERN = re.compile(r"(\d{8})")
_SYMBOL_PATTERN = re.compile(r"(\d{6,8})")


def infer_date_from_filename(filename: str) -> str:
    """Infer YYYY-MM-DD date string from a filename containing yyyymmdd."""
    match = _DATE_PATTERN.search(filename or "")
    if not match:
        raise ValueError("无法从文件名解析日期，请确保包含 yyyymmdd")

    date_obj = dt.datetime.strptime(match.group(1), "%Y%m%d").date()
    return date_obj.isoformat()


def clean_symbol(raw_value: object) -> Optional[str]:
    """Normalize stock symbol values like =\"603359\" to plain code."""
    if raw_value is None:
        return None

    text = str(raw_value).strip()
    if not text or text == "--":
        return None

    text = text.replace('="', "").replace('"', "")
    match = _SYMBOL_PATTERN.search(text)
    if not match:
        return None
    return match.group(1)


def to_nullable_text(raw_value: object) -> Optional[str]:
    """Convert placeholder values to None and return stripped text."""
    if raw_value is None:
        return None
    text = str(raw_value).strip()
    if not text or text == "--":
        return None
    return text


def to_float(raw_value: object) -> Optional[float]:
    """Convert common numeric text to float, returning None when empty."""
    text = to_nullable_text(raw_value)
    if text is None:
        return None

    text = text.replace(",", "")
    if text.endswith("%"):
        text = text[:-1]

    try:
        return float(text)
    except ValueError:
        return None


def _to_amount_100m_helper(raw_value: object, default_multiplier: float) -> Optional[float]:
    """Helper function to convert Chinese amount text to amount in 100 million yuan (亿元)."""
    text = to_nullable_text(raw_value)
    if text is None:
        return None

    text = text.replace(",", "")
    multiplier = default_multiplier
    if text.endswith("亿"):
        text = text[:-1]
        multiplier = 1.0
    elif text.endswith("万"):
        text = text[:-1]
        multiplier = 0.0001

    try:
        return float(text) * multiplier
    except ValueError:
        return None


def to_amount_100m(raw_value: object) -> Optional[float]:
    """Convert Chinese amount text to amount in 100 million yuan (亿元)."""
    return _to_amount_100m_helper(raw_value, default_multiplier=1.0)  # Default to 亿 (100,000,000) when no unit specified


def to_net_inflow_100m(raw_value: object) -> Optional[float]:
    """Convert main net inflow text to amount in 100 million yuan (亿元), default unit is 万 (10,000)."""
    return _to_amount_100m_helper(raw_value, default_multiplier=0.0001)  # Default to 万 (10,000) when no unit specified


def parse_table_from_bytes(file_bytes: bytes) -> List[Dict[str, object]]:
    """Parse uploaded bytes into a list of row dicts.

    It supports real .xls content through xlrd and text-based fake .xls files
    with tab delimiter and GB2312/GBK encodings.
    """
    rows = _parse_excel_bytes(file_bytes)
    if rows is not None:
        return rows

    text_rows = _parse_delimited_text_bytes(file_bytes)
    if text_rows is None:
        raise ValueError("文件读取失败：无法识别为 Excel 或制表符文本")
    return text_rows


def _parse_excel_bytes(file_bytes: bytes) -> Optional[List[Dict[str, object]]]:
    if xlrd is None:
        return None

    try:
        workbook = xlrd.open_workbook(file_contents=file_bytes)
    except Exception:
        return None

    if workbook.nsheets < 1:
        return []

    sheet = workbook.sheet_by_index(0)
    if sheet.nrows < 1:
        return []

    headers = [str(cell).strip() for cell in sheet.row_values(0)]
    data: List[Dict[str, object]] = []
    for row_idx in range(1, sheet.nrows):
        values = sheet.row_values(row_idx)
        row = cast(
            Dict[str, object],
            {headers[i]: values[i] if i < len(values) else None for i in range(len(headers))},
        )
        if _is_source_row(row):
            continue
        data.append(row)
    return data


def _parse_delimited_text_bytes(file_bytes: bytes) -> Optional[List[Dict[str, object]]]:
    text: Optional[str] = None
    for encoding in ("gb2312", "gbk", "utf-8"):
        try:
            text = file_bytes.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        return None

    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    rows: List[Dict[str, object]] = []
    for row in reader:
        if _is_source_row(row):
            continue
        cleaned = cast(Dict[str, object], {str(k).strip(): v for k, v in row.items() if k is not None})
        rows.append(cleaned)
    return rows


def _is_source_row(row: Mapping[str, object]) -> bool:
    first_value = None
    for value in row.values():
        first_value = value
        break

    if first_value is None:
        return False

    return "数据来源" in str(first_value)


def parse_csv_values(raw_value: Optional[str]) -> List[str]:
    """Parse comma-separated query values into a clean list."""
    text = (raw_value or "").strip()
    if not text:
        return []
    return [item.strip() for item in text.split(",") if item.strip()]


def rolling_stats(values: Iterable[Optional[float]], window: int) -> List[Dict[str, Optional[float]]]:
    """Compute rolling average and rolling sum for a numeric sequence."""
    if window <= 1:
        return [{"avg": value, "sum": value} for value in values]

    queue: List[Optional[float]] = []
    valid_sum = 0.0
    valid_count = 0
    result: List[Dict[str, Optional[float]]] = []

    for value in values:
        queue.append(value)
        if value is not None:
            valid_sum += value
            valid_count += 1

        if len(queue) > window:
            removed = queue.pop(0)
            if removed is not None:
                valid_sum -= removed
                valid_count -= 1

        avg_value = None if valid_count == 0 else valid_sum / valid_count
        sum_value = None if valid_count == 0 else valid_sum
        result.append({"avg": avg_value, "sum": sum_value})

    return result

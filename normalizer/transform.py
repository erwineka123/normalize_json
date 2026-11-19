from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any


DESTINATION_FIELDS = (
    "file_name",
    "source_channel",
    "Status",
    "company_domain",
    "NIK",
    "Email+Password",
    "Nomor Rekening",
    "PIN",
    "Token / API key",
    "Data Kesehatan",
    "Email",
    "Username",
    "Nomor Telepon",
    "Password",
    "Nama",
    "Kota / Provinsi",
    "Alamat",
)

ALIASES = {
    "file_name": ("file_name", "filename", "file", "source_file", "nama_file"),
    "source_channel": ("source_channel", "channel", "source", "platform"),
    "Status": ("status",),
    "company_domain": ("company_domain", "domain", "website"),
    "NIK": ("nik", "nik_karyawan", "employee_id", "id_karyawan", "national_id", "no_ktp", "ktp"),
    "Email+Password": ("email_password", "emailpassword", "email_dan_password"),
    "Nomor Rekening": ("nomor_rekening", "no_rekening", "rekening", "account_number"),
    "PIN": ("pin",),
    "Token / API key": (
        "token_api_key",
        "tokenapikey",
        "token",
        "api_key",
        "apikey",
        "access_token",
    ),
    "Data Kesehatan": ("data_kesehatan", "health_data", "medical_data", "kesehatan"),
    "Email": ("email", "e_mail"),
    "Username": ("username", "user_name", "login"),
    "Nomor Telepon": ("nomor_telepon", "no_hp", "nomor_hp", "phone", "telephone"),
    "Password": ("password", "pass", "kata_sandi"),
    "Nama": ("nama", "name", "full_name"),
    "Kota / Provinsi": ("kota_provinsi", "kota", "provinsi", "city", "province"),
    "Alamat": ("alamat", "address", "street_address"),
    "Tanggal Lahir": ("tanggal_lahir", "tgl_lahir", "birth_date", "date_of_birth"),
}

_NORMALIZED_ALIASES = {
    re.sub(r"[^a-z0-9]", "", alias.lower()): field
    for field, aliases in ALIASES.items()
    for alias in aliases
}
_HASH_FIELDS = tuple(field for field in DESTINATION_FIELDS if field not in {
    "file_name",
    "source_channel",
    "Status",
})


def _key_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _string_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    text = str(value).strip()
    return text or None


def _flatten(record: Mapping[str, Any]) -> dict[str, Any]:
    flattened: dict[str, Any] = {}

    def visit(value: Mapping[str, Any]) -> None:
        for key, child in value.items():
            if isinstance(child, Mapping):
                visit(child)
            else:
                token = _key_token(str(key))
                if token and token not in flattened:
                    flattened[token] = child

    visit(record)
    return flattened


def _records(content: Any) -> list[Mapping[str, Any]]:
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except json.JSONDecodeError:
            # Some exports use simple key:value lines instead of JSON.
            pairs = re.findall(r"(?m)^\s*([^:\n=]+?)\s*[:=]\s*([^,\n]+)", content)
            content = dict(pairs) if pairs else None

    if isinstance(content, Mapping):
        return [content]
    if isinstance(content, list):
        return [item for item in content if isinstance(item, Mapping)]
    return []


def normalize_source_row(
    source_row: Mapping[str, Any],
    *,
    default_file_name: str,
    default_source_channel: str,
    default_status: str,
) -> list[dict[str, Any]]:
    """Convert one raw_leaks row into zero or more destination rows."""
    content = source_row.get("content")
    normalized_rows: list[dict[str, Any]] = []
    source_created_at = _string_value(source_row.get("created_at"))

    for record in _records(content):
        values: dict[str, str] = {}
        for key, value in _flatten(record).items():
            field = _NORMALIZED_ALIASES.get(key)
            text = _string_value(value)
            if field and text is not None and field not in values:
                values[field] = text

        if "Email+Password" not in values and values.get("Email") and values.get("Password"):
            values["Email+Password"] = f"{values['Email']}:{values['Password']}"

        if not any(values.get(field) for field in _HASH_FIELDS):
            continue

        values.setdefault("file_name", default_file_name)
        values.setdefault("source_channel", default_source_channel)
        values.setdefault("Status", default_status)

        destination_row: dict[str, Any] = {
            field: values.get(field) for field in DESTINATION_FIELDS
        }
        destination_row["created_at"] = source_created_at

        fingerprint_data = {
            field: values[field]
            for field in _HASH_FIELDS
            if values.get(field) is not None
        }
        canonical = json.dumps(
            fingerprint_data,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        destination_row["hash-256"] = hashlib.sha256(canonical).hexdigest()
        normalized_rows.append(destination_row)

    return normalized_rows

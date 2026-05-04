import csv
import io
import re

from fastapi import HTTPException, status

from app.core.config import settings
from app.models.schemas import HospitalRow, ValidationError

MAX_FILE_SIZE_BYTES = 1_048_576
PHONE_PATTERN = re.compile(r"^\+?[0-9][0-9\s().-]{6,24}$")


class CSVValidator:
    """Three-layer CSV validator used before any side effects occur."""

    def validate(self, content: bytes) -> list[HospitalRow]:
        text = self._validate_file_level(content)
        rows, header_map = self._validate_structure_level(text)
        return self._validate_row_level(rows, header_map)

    def _validate_file_level(self, content: bytes) -> str:
        if not content or not content.strip():
            self._raise_validation_failed(
                [ValidationError(row=0, field="file", value=None, message="CSV file must not be empty")]
            )
        if len(content) > MAX_FILE_SIZE_BYTES:
            self._raise_validation_failed(
                [ValidationError(row=0, field="file", value=None, message="CSV file must be under 1MB")],
                http_status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            return content.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "code": "CSV_VALIDATION_FAILED",
                    "message": "CSV must be valid UTF-8. No hospitals were created.",
                    "errors": [
                        {
                            "row": 0,
                            "field": "file",
                            "value": None,
                            "message": "Invalid UTF-8 encoding",
                        }
                    ],
                },
            ) from exc

    def _validate_structure_level(self, text: str) -> tuple[list[dict[str, str]], dict[str, str]]:
        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames or not any((field or "").strip() for field in reader.fieldnames):
            self._raise_validation_failed(
                [ValidationError(row=1, field="header", value=None, message="CSV must include a header row")]
            )

        header_map = {
            field.strip().lower(): field
            for field in reader.fieldnames or []
            if field is not None and field.strip()
        }
        missing = [
            ValidationError(row=1, field=column, value=None, message=f"Missing required column: {column}")
            for column in ("name", "address")
            if column not in header_map
        ]
        if missing:
            self._raise_validation_failed(missing)

        rows = [
            row
            for row in reader
            if any((value or "").strip() for key, value in row.items() if key is not None)
        ]
        if not rows:
            self._raise_validation_failed(
                [ValidationError(row=2, field="row", value=None, message="CSV must include at least 1 data row")]
            )
        if len(rows) > settings.MAX_HOSPITALS_PER_CSV:
            self._raise_validation_failed(
                [
                    ValidationError(
                        row=0,
                        field="file",
                        value=str(len(rows)),
                        message=f"CSV may include at most {settings.MAX_HOSPITALS_PER_CSV} hospitals",
                    )
                ]
            )
        return rows, header_map

    def _validate_row_level(self, rows: list[dict[str, str]], header_map: dict[str, str]) -> list[HospitalRow]:
        errors: list[ValidationError] = []
        hospitals: list[HospitalRow] = []

        for line_number, row in enumerate(rows, start=2):
            raw_name = (row.get(header_map["name"]) or "").strip()
            raw_address = (row.get(header_map["address"]) or "").strip()
            raw_phone = (row.get(header_map.get("phone", ""), "") or "").strip() if "phone" in header_map else ""

            if not raw_name:
                errors.append(
                    ValidationError(row=line_number, field="name", value=raw_name, message="Name must not be empty")
                )
            if not raw_address:
                errors.append(
                    ValidationError(
                        row=line_number,
                        field="address",
                        value=raw_address,
                        message="Address must not be empty",
                    )
                )
            if raw_phone and not self._valid_phone(raw_phone):
                errors.append(
                    ValidationError(
                        row=line_number,
                        field="phone",
                        value=raw_phone,
                        message="Invalid phone format",
                    )
                )

            if raw_name and raw_address and (not raw_phone or self._valid_phone(raw_phone)):
                hospitals.append(HospitalRow(name=raw_name, address=raw_address, phone=raw_phone or None))

        if errors:
            self._raise_validation_failed(errors)
        return hospitals

    @staticmethod
    def _valid_phone(value: str) -> bool:
        digits = re.sub(r"\D", "", value)
        return bool(PHONE_PATTERN.match(value)) and 7 <= len(digits) <= 15

    @staticmethod
    def _raise_validation_failed(
        errors: list[ValidationError],
        http_status: int = status.HTTP_422_UNPROCESSABLE_CONTENT,
    ) -> None:
        invalid_rows = {error.row for error in errors if error.row > 1}
        row_word = "row" if len(invalid_rows) == 1 else "rows"
        message = (
            f"CSV contains {len(invalid_rows)} invalid {row_word}. No hospitals were created."
            if invalid_rows
            else "CSV validation failed. No hospitals were created."
        )
        raise HTTPException(
            status_code=http_status,
            detail={
                "code": "CSV_VALIDATION_FAILED",
                "message": message,
                "errors": [error.model_dump() for error in errors],
            },
        )

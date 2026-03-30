"""
Google Sheets client for the deals spreadsheet.

Writes a new column for each "consider" decision, pre-filled with
blue-cell defaults and grey-cell formulas copied from a reference column.
"""

import logging
import os
import re
from datetime import date
from typing import Optional

import toml
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from config.src.sheet_formulas import BLUE_LABELS, FORMULA_TEMPLATES
from common.src.flat_info import FlatInfo

logger = logging.getLogger(__name__)

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _load_deals_config() -> dict:
    config_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "config", "src", "config.toml"
    )
    config = toml.load(config_path)
    return config["deals"]


def _col_letter(col_index: int) -> str:
    """Convert 0-based column index to spreadsheet letter(s). 0->A, 25->Z, 26->AA."""
    result = ""
    n = col_index
    while True:
        result = chr(ord("A") + n % 26) + result
        n = n // 26 - 1
        if n < 0:
            break
    return result


def _col_index(col_letter: str) -> int:
    """Convert column letter(s) to 0-based index. A->0, Z->25, AA->26."""
    result = 0
    for ch in col_letter.upper():
        result = result * 26 + (ord(ch) - ord("A") + 1)
    return result - 1


def _reanchor_formula(formula: str, old_col: str, new_col: str) -> str:
    """Replace cell references from old column letter to new column letter.

    Only replaces references where the column letter matches exactly.
    E.g. for old_col='D', replaces D12 -> F12 but not AD12.
    """
    pattern = re.compile(r"(?<![A-Z])" + re.escape(old_col) + r"(\d+)")
    return pattern.sub(new_col + r"\1", formula)


class DealsSheetClient:
    """Client for reading/writing columns in the deals spreadsheet."""

    def __init__(self):
        cfg = _load_deals_config()
        self._spreadsheet_id: str = cfg["spreadsheet_id"]
        self._target_gid: int = cfg["sheet_gid"]
        self._resale_multiplier: float = cfg.get("resale_multiplier", 1.15)

        sa_path = cfg["google_service_account_json"]
        if not os.path.isabs(sa_path):
            sa_path = os.path.join(os.path.dirname(__file__), "..", "..", sa_path)
        creds = Credentials.from_service_account_file(sa_path, scopes=_SCOPES)
        self._service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        self._sheets = self._service.spreadsheets()

        # Cached after first call
        self._tab_title: Optional[str] = None
        self._label_row_map: Optional[dict[str, int]] = None
        self._total_rows: int = 0

    # ── helpers ────────────────────────────────────────────────────────

    def _get_tab_title(self) -> str:
        if self._tab_title is not None:
            return self._tab_title
        meta = self._sheets.get(spreadsheetId=self._spreadsheet_id).execute()
        for sheet in meta["sheets"]:
            props = sheet["properties"]
            if props["sheetId"] == self._target_gid:
                self._tab_title = props["title"]
                return self._tab_title
        raise ValueError(
            f"No tab with GID {self._target_gid} in spreadsheet {self._spreadsheet_id}"
        )

    def _get_label_row_map(self) -> dict[str, int]:
        """Read column B to build {label: 1-based-row-number} mapping."""
        if self._label_row_map is not None:
            return self._label_row_map
        tab = self._get_tab_title()
        result = (
            self._sheets.values()
            .get(
                spreadsheetId=self._spreadsheet_id,
                range=f"'{tab}'!B1:B50",
            )
            .execute()
        )
        rows = result.get("values", [])
        mapping: dict[str, int] = {}
        last_row = 0
        for i, row in enumerate(rows):
            if row and str(row[0]).strip():
                label = str(row[0]).strip()
                # Skip summary/header values (row 1 often has a currency summary)
                if label in BLUE_LABELS or label in FORMULA_TEMPLATES:
                    mapping[label] = i + 1  # 1-based
                    last_row = i + 1
        self._label_row_map = mapping
        self._total_rows = last_row
        return self._label_row_map

    @property
    def sheet_url(self) -> str:
        return (
            f"https://docs.google.com/spreadsheets/d/"
            f"{self._spreadsheet_id}/edit#gid={self._target_gid}"
        )

    # ── public API ────────────────────────────────────────────────────

    def find_column_for_flat(self, flat_id: str) -> Optional[int]:
        """Return the 0-based column index where flat_id appears in the
        Flat ID row, or None if not found."""
        label_map = self._get_label_row_map()
        flat_id_row = label_map.get("Flat ID")
        if flat_id_row is None:
            raise ValueError("Cannot find 'Flat ID' row in sheet")

        tab = self._get_tab_title()
        result = (
            self._sheets.values()
            .get(
                spreadsheetId=self._spreadsheet_id,
                range=f"'{tab}'!A{flat_id_row}:ZZ{flat_id_row}",
            )
            .execute()
        )
        row = result.get("values", [[]])[0]
        for i, val in enumerate(row):
            if str(val).strip() == str(flat_id).strip():
                return i
        return None

    def _find_reference_column(self) -> Optional[int]:
        """Return the 0-based index of the rightmost column that has a
        non-empty Flat ID value."""
        label_map = self._get_label_row_map()
        flat_id_row = label_map.get("Flat ID")
        if flat_id_row is None:
            raise ValueError("Cannot find 'Flat ID' row in sheet")

        tab = self._get_tab_title()
        result = (
            self._sheets.values()
            .get(
                spreadsheetId=self._spreadsheet_id,
                range=f"'{tab}'!A{flat_id_row}:ZZ{flat_id_row}",
            )
            .execute()
        )
        row = result.get("values", [[]])[0]
        rightmost = None
        for i, val in enumerate(row):
            if str(val).strip():
                rightmost = i
        return rightmost

    def _read_reference_formulas(self, col_index: int) -> dict[int, str]:
        """Read formulas from the reference column for all rows.

        Uses spreadsheets.get with includeGridData to get formulas.
        Returns {1-based-row-number: formula_string} for cells that
        contain formulas.
        """
        tab = self._get_tab_title()
        col_letter = _col_letter(col_index)
        total = self._total_rows or 40
        range_str = f"'{tab}'!{col_letter}1:{col_letter}{total}"

        resp = self._sheets.get(
            spreadsheetId=self._spreadsheet_id,
            ranges=[range_str],
            includeGridData=True,
        ).execute()

        formulas: dict[int, str] = {}
        grid_data = resp["sheets"][0]["data"][0]
        row_data_list = grid_data.get("rowData", [])

        for i, row_data in enumerate(row_data_list):
            row_num = i + 1  # 1-based
            cells = row_data.get("values", [])
            if cells:
                cell = cells[0]
                uev = cell.get("userEnteredValue", {})
                formula = uev.get("formulaValue")
                if formula:
                    formulas[row_num] = formula

        return formulas

    def write_deal_column(self, flat: FlatInfo) -> str:
        """Write a new deal column for the given flat.

        Returns the direct sheet URL.
        Idempotent: if flat_id already exists, returns URL without writing.
        """
        # 1. Check if flat already has a column
        existing = self.find_column_for_flat(flat.flat_id)
        if existing is not None:
            logger.info(
                f"Flat {flat.flat_id} already in sheet at column "
                f"{_col_letter(existing)}"
            )
            return self.sheet_url

        # 2. Find reference column
        ref_col = self._find_reference_column()

        # 3. Determine new column index
        if ref_col is not None:
            new_col_idx = ref_col + 1
        else:
            # Column A is empty, column B is labels; first deal goes in C
            new_col_idx = 2

        new_col = _col_letter(new_col_idx)
        label_map = self._get_label_row_map()
        total = self._total_rows

        # 4. Get grey-cell formulas
        if ref_col is not None:
            ref_letter = _col_letter(ref_col)
            ref_formulas = self._read_reference_formulas(ref_col)
            grey_formulas: dict[int, str] = {}
            for row_num, formula in ref_formulas.items():
                grey_formulas[row_num] = _reanchor_formula(formula, ref_letter, new_col)
        else:
            # Fallback: use hardcoded templates
            grey_formulas = {}
            for label, tmpl in FORMULA_TEMPLATES.items():
                row_num = label_map.get(label)
                if row_num:
                    grey_formulas[row_num] = tmpl.replace("{col}", new_col)

        # 5. Build column values (one entry per row, rows 1..total)
        today = date.today()

        # Map label -> default value for blue cells
        half = round(flat.price / 2)
        blue_defaults: dict[str, object] = {
            "Project": flat.residential_complex or "",
            "Flat ID": flat.flat_id,
            "Investment Date": today.strftime("%d/%m/%Y"),
            "Flat Price": flat.price,
            "Other Costs": 0,
            "By Damir": 0,
            "By Arthur": half,
            "By Aigerim": flat.price - half,
            "Commission Damir (KZT)": 500000,
            "Resale Date": f"=EDATE({new_col}4,3)",
            "Rent Received": 0,
            "Resale Price": round(flat.price * self._resale_multiplier),
            "EUR/KZT Initial": f'=INDEX(GOOGLEFINANCE("CURRENCY:EURKZT", "price", {new_col}4),2,2)',
            "EUR/KZT Final": '=INDEX(GOOGLEFINANCE("CURRENCY:EURKZT", "price", TODAY()),2,2)',
            "Completed": "FALSE",
        }

        # Reverse map: row_number -> label
        row_to_label: dict[int, str] = {v: k for k, v in label_map.items()}

        # 6. Write only cells with data (sparse) to preserve row formatting
        tab = self._get_tab_title()
        data: list[dict] = []
        for row_num in range(1, total + 1):
            label = row_to_label.get(row_num)
            if label and label in blue_defaults:
                value = blue_defaults[label]
            elif row_num in grey_formulas:
                value = grey_formulas[row_num]
            else:
                continue  # skip — don't touch unrelated cells
            cell = f"'{tab}'!{new_col}{row_num}"
            data.append({"range": cell, "values": [[value]]})

        self._sheets.values().batchUpdate(
            spreadsheetId=self._spreadsheet_id,
            body={
                "valueInputOption": "USER_ENTERED",
                "data": data,
            },
        ).execute()

        logger.info(f"Wrote deal column {new_col} for flat {flat.flat_id}")
        return self.sheet_url

    def get_sheet_url_for_flat(self, flat_id: str) -> Optional[str]:
        """Return the sheet URL if the flat has a column, else None."""
        col = self.find_column_for_flat(flat_id)
        if col is not None:
            return self.sheet_url
        return None

    def read_all_deals(self) -> list[dict]:
        """Read all deal columns from the spreadsheet.

        Returns a list of dicts, one per deal column that has a Flat ID.
        Each dict maps label names to their evaluated values.
        """
        label_map = self._get_label_row_map()
        total = self._total_rows
        if not label_map or total == 0:
            return []

        # Reverse map: row_number -> label
        row_to_label: dict[int, str] = {v: k for k, v in label_map.items()}

        # Read the full data block: columns C onward, rows 1..total
        tab = self._get_tab_title()
        range_str = f"'{tab}'!C1:ZZ{total}"
        result = (
            self._sheets.values()
            .get(
                spreadsheetId=self._spreadsheet_id,
                range=range_str,
                valueRenderOption="FORMATTED_VALUE",
            )
            .execute()
        )
        all_rows = result.get("values", [])
        if not all_rows:
            return []

        # Transpose: rows -> columns
        # all_rows[row_idx] is a list of values across columns
        max_cols = max(len(row) for row in all_rows)

        deals: list[dict] = []
        for col_idx in range(max_cols):
            col_data: dict[str, str] = {}
            for row_idx in range(len(all_rows)):
                row_num = row_idx + 1  # 1-based
                label = row_to_label.get(row_num)
                if label and col_idx < len(all_rows[row_idx]):
                    value = all_rows[row_idx][col_idx]
                    if value is not None and str(value).strip():
                        col_data[label] = str(value).strip()

            # Skip empty columns (no Flat ID)
            if not col_data.get("Flat ID"):
                continue

            deal = self._parse_deal(col_data)
            deals.append(deal)

        return deals

    @staticmethod
    def _parse_deal(col_data: dict[str, str]) -> dict:
        """Parse raw string values from a sheet column into a typed deal dict."""

        def _parse_number(val: str) -> Optional[float]:
            if not val:
                return None
            # Remove currency symbols, commas, spaces, % signs
            cleaned = val.replace(",", "").replace(" ", "").replace("%", "")
            try:
                return float(cleaned)
            except (ValueError, TypeError):
                return None

        def _parse_date(val: str) -> Optional[str]:
            """Parse DD/MM/YYYY to ISO YYYY-MM-DD."""
            if not val:
                return None
            from datetime import datetime as dt

            for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y"):
                try:
                    return dt.strptime(val, fmt).strftime("%Y-%m-%d")
                except ValueError:
                    continue
            return None

        completed_raw = col_data.get("Completed", "FALSE").upper()
        completed = completed_raw in ("TRUE", "1", "YES")

        investment_date_iso = _parse_date(col_data.get("Investment Date", ""))
        resale_date_iso = _parse_date(col_data.get("Resale Date", ""))

        # Compute days_held
        days_held = None
        if investment_date_iso:
            from datetime import date as d

            inv = d.fromisoformat(investment_date_iso)
            if completed and resale_date_iso:
                end = d.fromisoformat(resale_date_iso)
            else:
                end = d.today()
            days_held = (end - inv).days

        return {
            "project": col_data.get("Project", ""),
            "flat_id": col_data.get("Flat ID", ""),
            "investment_date": investment_date_iso,
            "flat_price": _parse_number(col_data.get("Flat Price", "")),
            "total_cost": _parse_number(col_data.get("Total Cost", "")),
            "total_cost_eur": _parse_number(col_data.get("Total Cost (EUR)", "")),
            "resale_date": resale_date_iso,
            "rent_received": _parse_number(col_data.get("Rent Received", "")),
            "resale_price": _parse_number(col_data.get("Resale Price", "")),
            "net_profit_kzt": _parse_number(col_data.get("Net Profit KZT", "")),
            "net_profit_eur": _parse_number(col_data.get("Net Profit EUR", "")),
            "net_return_kzt": _parse_number(col_data.get("Net Return KZT", "")),
            "net_return_eur": _parse_number(col_data.get("Net Return EUR", "")),
            "equivalent_annual_return_eur": _parse_number(
                col_data.get("Equivalent Annual Return EUR", "")
            ),
            "multiple": _parse_number(col_data.get("Multiple", "")),
            "number_of_weeks": _parse_number(col_data.get("Number of weeks", "")),
            "completed": completed,
            "days_held": days_held,
        }

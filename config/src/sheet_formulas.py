"""
Hardcoded formula templates for the deals spreadsheet.

Used as a fallback when there is no existing reference column to copy
formulas from (i.e., the very first deal entry).

Each formula uses {col} as a placeholder for the column letter of the
new deal column.  Row numbers match the *actual* sheet layout discovered
from column B of the "RE - Kz Ops" tab:

  Row  1: (summary header)
  Row  2: Project              (blue)
  Row  3: Flat ID              (blue)
  Row  4: Investment Date      (blue)
  Row  5: EUR/KZT Initial      (blue)
  Row  6: Flat Price           (blue)
  Row  7: Other Costs          (blue)
  Row  8: Total Cost           (grey)
  Row  9: Total Cost (EUR)     (grey)
  Row 10: By Damir             (blue)
  Row 11: By Arthur            (blue)
  Row 12: By Aigerim           (blue)
  Row 13: Resale Date          (blue)
  Row 14: Rent Received        (blue)
  Row 15: Resale Price         (blue)
  Row 16: Resale Price (EUR)   (grey)
  Row 17: Tax KZT              (grey)
  Row 18: Net Profit KZT       (grey)
  Row 19: EUR/KZT Final        (blue)
  Row 20: Net Profit EUR       (grey)
  Row 21: Net Return KZT       (grey)
  Row 22: Net Return EUR       (grey)
  Row 23: Equivalent Annual Return EUR (grey)
  Row 24: Commission Damir (KZT) (grey)
  Row 25: Profit Damir (KZT)   (grey)
  Row 26: Profit Arthur (KZT)  (grey)
  Row 27: Profit Aigerim (KZT) (grey)
  Row 28: Profit Arthur (EUR)  (grey)
  Row 29: Profit Aigerim (EUR) (grey)
  Row 30: Completed            (blue)
  Row 31: Multiple             (grey)
  Row 32: Number of weeks      (grey)
"""

# Blue-cell labels (user input).  Everything else is grey (formula-driven).
BLUE_LABELS: set[str] = {
    "Project",
    "Flat ID",
    "Investment Date",
    "Flat Price",
    "Other Costs",
    "By Damir",
    "By Arthur",
    "By Aigerim",
    "Resale Date",
    "Rent Received",
    "Resale Price",
    "EUR/KZT Initial",
    "EUR/KZT Final",
    "Completed",
}

# Fallback formula templates keyed by label.
# {col} → column letter, row numbers are hard-coded to match the actual sheet.
FORMULA_TEMPLATES: dict[str, str] = {
    "Total Cost": "={col}6+{col}7",
    "Total Cost (EUR)": "={col}8/{col}5",
    "Resale Price (EUR)": "={col}15/{col}19",
    "Tax KZT": "=IF({col}13-{col}4>365,0,10%)*({col}15-{col}6)",
    "Net Profit KZT": "={col}15-{col}8-{col}17+{col}14",
    "Net Profit EUR": "={col}18/{col}19",
    "Net Return KZT": "={col}18/{col}8",
    "Net Return EUR": "={col}20/{col}9",
    "Equivalent Annual Return EUR": "={col}22*365/({col}13-{col}4)",
    "Commission Damir (KZT)": "=IF({col}10>0,{col}18*{col}10/{col}8,0)",
    "Profit Damir (KZT)": "={col}24+IF({col}10>0,({col}18-{col}24)*{col}10/{col}8,0)",
    "Profit Arthur (KZT)": "=({col}18-{col}24)*{col}11/{col}8",
    "Profit Aigerim (KZT)": "=({col}18-{col}24)*{col}12/{col}8",
    "Profit Arthur (EUR)": "={col}26/{col}19",
    "Profit Aigerim (EUR)": "={col}27/{col}19",
    "Multiple": "=({col}8+{col}18)/{col}8",
    "Number of weeks": '=IF({col}13="",({col}4-TODAY())/-7,({col}13-{col}4)/7)',
}

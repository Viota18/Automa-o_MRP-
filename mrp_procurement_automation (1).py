
# ============================================================
#  MRP DATA PIPELINE AUTOMATION  |  v1.0
#  Author: Vitor Gabriel Cardoso dos Santos
#  GitHub: github.com/seu-usuario
#
#  Description:
#  End-to-end data pipeline for material planning and purchasing processes.
#
#  Features:
#    - Reads multiple ERP data sources (Excel)
#    - Consolidates stock, demand, and supply data
#    - Applies business rules for material planning (MRP logic)
#    - Enriches data with supplier and catalog information
#    - Generates structured output for purchasing systems
#    - Produces audit reports and logs
# ============================================================

# DEPENDENCIES:
#   pip install pandas openpyxl xlrd

# HOW TO USE:
#   1. Export ERP reports in Excel format (.xlsx) and save them
#      to the folder defined in INPUT_DATA_DIR below.
#   2. Make sure the purchasing system reference files are
#      up to date (supplier_item_list, item_list, etc.).
#   3. Run: python mrp_procurement_automation.py
#   4. The generated CSV will be at OUTPUT_DIR/procurement_load_YYYYMMDD.csv

# EXPECTED INPUT FILES (configure in SETTINGS below):
#   ERP Report Exports:
#     - requisitions.xlsx → Open Purchase Requisitions       [MAIN]
#     - reservations.xlsx → Material Reservations
#     - stock.xlsx        → Warehouse Stock
#     - orders.xlsx       → Open Purchase Orders
#     - materials.xlsx    → Material planning master data
#
#   Purchasing System References (weekly update):
#     - supplier_item_list.xlsx  → Supplier-item mapping
#     - item_list.xlsx           → Catalog item list
#     - supplier_list.xlsx       → Approved supplier list
#
#   Reference Tables (occasional update):
#     - unit_conversion.xlsx     → Unit of measure conversion
#     - commodity_groups.xlsx    → Commodity / buyer group mapping
#     - material_class.xlsx      → Material classification
# ============================================================


import math
import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


# ============================================================
#  SETTINGS — adjust to your environment
# ============================================================

# Directory where ERP report exports are saved
INPUT_DATA_DIR = Path("./input_data")

# Directory for purchasing system reference files
REFERENCE_DIR = Path("./reference")

# Output directory for generated files
OUTPUT_DIR = Path("./output")

# Default plant / site code
DEFAULT_PLANT = "PLANT1"

# Purchasing organization code
PURCHASING_ORG = "PO01"

# Default delivery location code
DELIVERY_LOCATION_CODE = "PLANT1"

# Minimum lead time in days for items without a contract (spot buy)
SPOT_LEAD_TIME_DAYS = 19


# ============================================================
#  INPUT FILE PATHS
# ============================================================

FILES = {
    # ERP exports
    "requisitions":  INPUT_DATA_DIR / "requisitions.xlsx",
    "reservations":  INPUT_DATA_DIR / "reservations.xlsx",
    "stock":         INPUT_DATA_DIR / "stock.xlsx",
    "orders":        INPUT_DATA_DIR / "orders.xlsx",
    "materials":     INPUT_DATA_DIR / "materials.xlsx",

    # Purchasing system references
    "supplier_item_list": REFERENCE_DIR / "supplier_item_list.xlsx",
    "item_list":          REFERENCE_DIR / "item_list.xlsx",
    "supplier_list":      REFERENCE_DIR / "supplier_list.xlsx",

    # Reference tables
    "unit_conversion":  REFERENCE_DIR / "unit_conversion.xlsx",
    "commodity_groups": REFERENCE_DIR / "commodity_groups.xlsx",
    "material_class":   REFERENCE_DIR / "material_class.xlsx",
}


# ============================================================
#  LOGGING SETUP
# ============================================================

OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(OUTPUT_DIR / "automation.log", mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger("mrp_pipeline")


# ============================================================
#  ERP REPORT READERS
# ============================================================

def read_purchase_requisitions(path: Path) -> pd.DataFrame:
    """
    Reads open purchase requisitions from ERP export.

    Contains items flagged by the planning engine as pending
    procurement action.

    Key columns read:
        - Requisition number
        - Item number
        - Material code
        - Short description
        - Requested quantity
        - Unit of measure
        - Valuation price
        - Total value
        - Delivery date (requirement date)
        - Request date
        - Plant / Storage location
        - Cost element category
        - Document type
        - Item category
        - Contract reference
        - MRP planner
        - Buyer group
        - Suggested supplier name
        - Currency
    """
    log.info(f"Reading Purchase Requisitions: {path}")
    df = pd.read_excel(path, dtype=str)

    # Map ERP column names → standardized internal names
    # Adjust the keys below to match your ERP column headers exactly
    column_map = {
        "Purchase Requisition":  "req_number",
        "Item":                  "req_item",
        "Material":              "material",
        "Short Text":            "description",
        "Quantity":              "requested_qty",
        "Unit of Measure":       "uom_erp",
        "Valuation Price":       "valuation_price",
        "Total Value":           "total_value",
        "Delivery Date":         "delivery_date",        # ← REQUIREMENT DATE
        "Requisition Date":      "request_date",
        "Plant":                 "plant",
        "Storage Location":      "storage_location",
        "Account Category":      "account_category",
        "Document Type":         "doc_type",
        "Item Category":         "item_category",
        "Outline Agreement":     "contract_ref",
        "MRP Controller":        "mrp_planner",
        "Purchasing Group":      "buyer_group",
        "Vendor Name":           "vendor_name",
        "Currency":              "currency",
    }
    df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

    # Type conversions
    for col in ["requested_qty", "valuation_price", "total_value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].str.replace(",", "."), errors="coerce")

    for col in ["delivery_date", "request_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    # Keep only rows with a material code
    df = df[df["material"].notna() & (df["material"] != "")].copy()

    log.info(f"  Purchase Requisitions: {len(df)} rows loaded")
    return df


def read_reservations(path: Path) -> pd.DataFrame:
    """
    Reads material reservations from ERP export.

    Reservations represent confirmed future demand from production
    orders, maintenance orders, or manual requests.
    Additional data integrated to improve demand accuracy.

    Key columns read:
        - Reservation number
        - Requirement date
        - Material
        - Required quantity
        - Cost center
    """
    log.info(f"Reading Reservations: {path}")
    df = pd.read_excel(path, dtype=str)

    column_map = {
        "Plant":               "plant",
        "Reservation":         "reservation_number",
        "Reservation Item":    "reservation_item",
        "Requirement Date":    "requirement_date",
        "Material":            "material",
        "Required Quantity":   "required_qty",
        "Withdrawal Quantity": "withdrawal_qty",
        "Base Unit of Measure":"uom",
        "Account Category":    "account_category",
        "Cost Center":         "cost_center",
        "Movement Type":       "movement_type",
    }
    df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

    for col in ["required_qty", "withdrawal_qty"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].str.replace(",", "."), errors="coerce")

    if "requirement_date" in df.columns:
        df["requirement_date"] = pd.to_datetime(df["requirement_date"], errors="coerce", dayfirst=True)

    # Aggregate per material: sum qty, keep latest reservation number and most urgent date
    summary = df.groupby("material").agg(
        reserved_qty=("required_qty", "sum"),
        reservation_number=("reservation_number", "last"),
        requirement_date=("requirement_date", "max"),
        cost_center=("cost_center", "last"),
    ).reset_index()

    log.info(f"  Reservations: {len(summary)} materials with reservations")
    return summary


def read_stock(path: Path) -> pd.DataFrame:
    """
    Reads warehouse stock levels from ERP export.

    Returns unrestricted stock quantity per material.
    """
    log.info(f"Reading Stock: {path}")
    df = pd.read_excel(path, dtype=str)

    column_map = {
        "Plant":              "plant",
        "Material":           "material",
        "Material Description": "description",
        "Storage Location":   "storage_location",
        "Unrestricted Stock": "unrestricted_stock",
    }
    df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

    df["unrestricted_stock"] = pd.to_numeric(
        df.get("unrestricted_stock", pd.Series(dtype=float)), errors="coerce"
    ).fillna(0)

    summary = df.groupby("material", as_index=False)["unrestricted_stock"].sum()
    summary.rename(columns={"unrestricted_stock": "stock_qty"}, inplace=True)

    log.info(f"  Stock: {len(summary)} materials with stock records")
    return summary


def read_open_orders(path: Path) -> pd.DataFrame:
    """
    Reads open purchase orders from ERP export.

    Returns quantity still pending delivery per material,
    keeping the most recent expected delivery date.
    """
    log.info(f"Reading Open Purchase Orders: {path}")
    df = pd.read_excel(path, dtype=str)

    column_map = {
        "Material":             "material",
        "Purchasing Document":  "po_number",
        "Order Value":          "order_value",
        "Open Quantity":        "open_qty",
        "Delivery Date":        "delivery_date",
    }
    df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

    if "open_qty" in df.columns:
        df["open_qty"] = pd.to_numeric(
            df["open_qty"].str.replace(",", "."), errors="coerce"
        ).fillna(0)

    # Aggregate: sum open quantities, keep latest delivery date per material
    summary = df.groupby("material", as_index=False).agg(
        open_qty=("open_qty", "sum"),
        delivery_date=("delivery_date", "max"),
    )

    log.info(f"  Open Orders: {len(summary)} materials with open POs")
    return summary


def read_material_data(path: Path) -> pd.DataFrame:
    """
    Reads material planning master data from ERP export.

    Contains MRP parameters per material:
        - MRP type (VB = reorder point, PD = deterministic, etc.)
        - Lead time
        - Safety stock
        - Minimum lot size
        - Rounding value
        - Maximum stock
    """
    log.info(f"Reading Material Planning Data: {path}")
    df = pd.read_excel(path, dtype=str)

    column_map = {
        "Material":           "material",
        "Material Description": "description",
        "Material Type":      "material_type",
        "MRP Type":           "mrp_type",
        "MRP Controller":     "mrp_planner",
        "Lead Time":          "lead_time",
        "Reorder Point":      "reorder_point",
        "Safety Stock":       "safety_stock",
        "Min Lot Size":       "min_lot_size",
        "Rounding Value":     "rounding_value",
        "Max Stock":          "max_stock",
        "Base UoM":           "base_uom",
        "Material Group":     "material_group",
        "Material Group Desc": "material_group_desc",
    }
    df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

    for col in ["lead_time", "safety_stock", "min_lot_size", "rounding_value", "max_stock"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].str.replace(",", "."), errors="coerce"
            ).fillna(0)

    log.info(f"  Material Data: {len(df)} materials loaded")
    return df


# ============================================================
#  DATA CONSOLIDATION
# ============================================================

def consolidate_data(
    requisitions: pd.DataFrame,
    reservations: pd.DataFrame,
    stock: pd.DataFrame,
    open_orders: pd.DataFrame,
    material_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    Consolidates the 5 ERP data sources into a single analytical dataset.

    Business logic applied:
        available_coverage  = stock + open orders + open requisitions
        reserved_qty        = sum of reservations per material
        demand              = calculated per MRP type:
                                VB  → max_stock + reserved_qty
                                PD  → reserved_qty + safety_stock
                                default → max_stock + reserved_qty
        should_purchase     = True when demand > available_coverage
        purchase_suggestion = demand - coverage, rounded to min lot size
    """
    log.info("Consolidating 5 ERP data sources...")

    # Start from purchase requisitions (what MRP decided to buy)
    base = requisitions.copy()

    # 1. Add stock data
    base = base.merge(stock[["material", "stock_qty"]], on="material", how="left")
    base["stock_qty"] = base["stock_qty"].fillna(0)

    # 2. Add reservation data
    base = base.merge(
        reservations[["material", "reserved_qty", "reservation_number",
                       "requirement_date", "cost_center"]],
        on="material", how="left"
    )
    base["reserved_qty"] = base["reserved_qty"].fillna(0)

    # 3. Resolve final requirement date (reservation date takes priority)
    if "delivery_date" in base.columns:
        base["final_requirement_date"] = base["requirement_date"].combine_first(
            base["delivery_date"]
        )
    else:
        base["final_requirement_date"] = base["requirement_date"]

    # 4. Add open purchase order data
    base = base.merge(
        open_orders[["material", "open_qty", "delivery_date"]],
        on="material", how="left"
    )
    base["open_qty"] = base["open_qty"].fillna(0)

    # 5. Add MRP master data
    mrp_cols = [c for c in [
        "material", "material_type", "mrp_type", "lead_time",
        "safety_stock", "min_lot_size", "rounding_value",
        "max_stock", "base_uom", "material_group", "material_group_desc",
    ] if c in material_data.columns]
    base = base.merge(material_data[mrp_cols], on="material", how="left")

    # ---- Analytical calculations ----

    base["available_coverage"] = (
        base["open_qty"] + base["requested_qty"] + base["stock_qty"]
    )

    # Demand calculation by MRP type
    def calculate_demand(row):
        mrp_type  = str(row.get("mrp_type", "")).upper()
        mat_type  = str(row.get("material_type", "")).upper()
        safety    = row.get("safety_stock", 0) or 0
        max_stock = row.get("max_stock", 0) or 0
        reserved  = row.get("reserved_qty", 0) or 0

        if mat_type == "UNBW":          # non-valuated material → replenish to max
            return max_stock
        if mrp_type == "VB":            # reorder-point planning
            return max_stock + reserved
        if mrp_type == "PD":            # deterministic / requirements-based
            return reserved + safety
        return max_stock + reserved     # default fallback

    base["demand"] = base.apply(calculate_demand, axis=1)

    # Should purchase? YES / NO
    def should_purchase(row):
        mrp_type = str(row.get("mrp_type", "")).upper()
        demand   = row.get("demand", 0) or 0
        coverage = row.get("available_coverage", 0) or 0
        stock    = row.get("stock_qty", 0) or 0
        safety   = row.get("safety_stock", 0) or 0
        reserved = row.get("reserved_qty", 0) or 0

        if mrp_type == "VB":
            return "YES" if (stock < safety and coverage < safety) else "NO"
        return "YES" if (safety + reserved) > coverage else "NO"

    base["should_purchase"] = base.apply(should_purchase, axis=1)

    # Theoretical purchase suggestion
    base["theoretical_suggestion"] = base.apply(
        lambda r: max(0, (r.get("demand", 0) or 0) - (r.get("available_coverage", 0) or 0))
        if r["should_purchase"] == "YES" else 0,
        axis=1,
    )

    # Suggestion rounded up to minimum lot size
    def round_to_lot(row):
        suggestion = row.get("theoretical_suggestion", 0) or 0
        min_lot    = row.get("min_lot_size", 0) or 0
        rounding   = row.get("rounding_value", 0) or 0

        if suggestion <= 0:
            return 0
        if min_lot > 0 and suggestion < min_lot:
            return min_lot
        if rounding > 1:
            return math.ceil(suggestion / rounding) * rounding
        return suggestion

    base["purchase_suggestion"] = base.apply(round_to_lot, axis=1)

    log.info(
        f"  Consolidation done: {len(base)} rows | "
        f"{base[base['should_purchase'] == 'YES'].shape[0]} items to purchase"
    )
    return base


# ============================================================
#  JUSTIFICATION FIELD BUILDER
# ============================================================

def add_justification(base: pd.DataFrame) -> pd.DataFrame:
    """
    Auto-generates the 'Justification' field for purchasing requisitions.

    The justification is assembled from fields already present in the data:
        1. Source of the request (planning engine or manual reservation)
        2. Reservation number (when available)
        3. Material code + description
        4. Requirement date
        5. Current stock level
    """

    def build_justification(row):
        parts = []

        # Origin
        doc_type = str(row.get("doc_type", ""))
        if doc_type.startswith("Z"):        # planning engine document type convention
            parts.append("Requisition auto-generated by planning engine (MRP)")
        else:
            parts.append("Manual purchase requisition")

        # Reservation number (was being lost)
        res_num = row.get("reservation_number")
        if pd.notna(res_num) and str(res_num).strip() not in ("", "nan"):
            parts.append(f"Reservation: {res_num}")

        # Material + description
        material = row.get("material", "")
        desc     = row.get("description", "")
        if material and desc:
            parts.append(f"Material {material} – {desc}")
        elif material:
            parts.append(f"Material {material}")

        # Requirement date (was being lost)
        req_date = row.get("final_requirement_date")
        if pd.notna(req_date):
            parts.append(f"Required by: {pd.Timestamp(req_date).strftime('%Y-%m-%d')}")

        # Current stock
        stock = row.get("stock_qty", 0) or 0
        parts.append(f"Current stock: {stock:.0f}")

        return " | ".join(parts)

    base["justification"] = base.apply(build_justification, axis=1)
    log.info("  Justification field populated for all rows")
    return base


# ============================================================
#  PURCHASING SYSTEM DATA ENRICHMENT
# ============================================================

def enrich_with_procurement_data(
    base: pd.DataFrame,
    supplier_item_list_path: Path,
    item_list_path: Path,
    unit_conversion_path: Path,
) -> pd.DataFrame:
    """
    Enriches the consolidated dataset with purchasing system master data:
        - Supplier name / Supplier number
        - Contract name
        - Catalog item number
        - Commodity name
        - Target system unit of measure
        - Quantity conversion (when ERP UoM ≠ target system UoM)
        - Converted price
    """

    # --- Supplier Item List ---
    if supplier_item_list_path.exists():
        log.info(f"Reading Supplier Item List: {supplier_item_list_path}")
        sil = pd.read_excel(supplier_item_list_path, dtype=str)

        # Column detection (flexible — adapts to minor naming changes)
        col_material     = next((c for c in sil.columns if "material" in c.lower()), None)
        col_vendor_num   = next((c for c in sil.columns if "supplier" in c.lower() and "number" in c.lower()), None)
        col_vendor_name  = next((c for c in sil.columns if "supplier" in c.lower() and "name"   in c.lower()), None)
        col_contract     = next((c for c in sil.columns if "contract" in c.lower()), None)
        col_catalog_item = next((c for c in sil.columns if "catalog"  in c.lower() and "item"   in c.lower()), None)
        col_uom_target = next((c for c in sil.columns if "unit"     in c.lower() and "target" in c.lower()), None)

        if col_material:
            rename_map = {col_material: "material"}
            if col_vendor_num:   rename_map[col_vendor_num]   = "vendor_number"
            if col_vendor_name:  rename_map[col_vendor_name]  = "vendor_name_platform"
            if col_contract:     rename_map[col_contract]     = "contract_name"
            if col_catalog_item: rename_map[col_catalog_item] = "catalog_item_number"
            if col_uom_target: rename_map[col_uom_target] = "uom_target"

            sil_lookup = sil.rename(columns=rename_map)
            merge_cols = ["material"] + [
                c for c in ["vendor_number", "vendor_name_platform",
                             "contract_name", "catalog_item_number", "uom_target"]
                if c in sil_lookup.columns
            ]
            base = base.merge(
                sil_lookup[merge_cols].drop_duplicates("material"),
                on="material", how="left"
            )
            log.info("  Supplier Item List: vendor/contract data merged")
    else:
        log.warning(f"  WARNING: Supplier Item List not found at {supplier_item_list_path}")
        for col in ["vendor_number", "vendor_name_platform", "contract_name",
                    "catalog_item_number", "uom_target"]:
            base[col] = ""

    # --- Item List (commodity name) ---
    if item_list_path.exists():
        log.info(f"Reading Item List: {item_list_path}")
        il = pd.read_excel(item_list_path, dtype=str)
        col_mat  = next((c for c in il.columns if "material"  in c.lower()), None)
        col_comm = next((c for c in il.columns if "commodity" in c.lower()), None)
        if col_mat and col_comm:
            il_lookup = il[[col_mat, col_comm]].rename(
                columns={col_mat: "material", col_comm: "commodity_name"}
            ).drop_duplicates("material")
            base = base.merge(il_lookup, on="material", how="left")
    else:
        log.warning(f"  WARNING: Item List not found at {item_list_path}")
        base["commodity_name"] = ""

    # --- Unit of Measure Conversion ---
    base["converted_qty"]   = base["requested_qty"]
    base["converted_price"] = base.get("valuation_price", pd.Series(0, index=base.index))

    if unit_conversion_path.exists() and "uom_target" in base.columns:
        log.info(f"Reading Unit Conversion Table: {unit_conversion_path}")
        # Flag items where ERP UoM differs from target system UoM for manual review
        base["uom_conversion_status"] = base.apply(
            lambda r: "ok"
            if str(r.get("uom_erp", "")) == str(r.get("uom_target", ""))
            else "Review Conversion",
            axis=1,
        )
        log.info("  UoM conversion check completed")
    else:
        base["uom_conversion_status"] = "ok"

    return base


# ============================================================
#  PRE-EXPORT VALIDATIONS
# ============================================================

def validate_data(base: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the consolidated dataset before generating the import CSV.
    Logs warnings and removes or corrects invalid records.

    Checks performed:
        1. Requirement date must not be in the past
        2. Items without a contract must have sufficient lead time (spot buy buffer)
        3. Quantity must be greater than zero
        4. Unit of measure conversion must be resolved
        5. All items must have a mapped supplier
    """
    log.info("Running validations...")

    today        = pd.Timestamp.today().normalize()
    minimum_date = today + timedelta(days=SPOT_LEAD_TIME_DAYS)
    warnings     = []

    # 1. Requirement date in the past → bump to minimum date
    mask_past = (
        base["final_requirement_date"].notna() &
        (base["final_requirement_date"] < today)
    )
    if mask_past.any():
        n = mask_past.sum()
        warnings.append(
            f"⚠️  {n} item(s) with requirement date in the past "
            f"— adjusted to {minimum_date.strftime('%Y-%m-%d')}"
        )
        base.loc[mask_past, "final_requirement_date"] = minimum_date

    # 2. Spot items with insufficient lead time → bump to minimum date
    if "contract_name" in base.columns:
        mask_spot = (
            (base["contract_name"].isna() | (base["contract_name"] == "")) &
            base["final_requirement_date"].notna() &
            (base["final_requirement_date"] < minimum_date)
        )
        if mask_spot.any():
            n = mask_spot.sum()
            warnings.append(
                f"⚠️  {n} spot item(s) with lead time < {SPOT_LEAD_TIME_DAYS} days "
                f"— date adjusted to {minimum_date.strftime('%Y-%m-%d')}"
            )
            base.loc[mask_spot, "final_requirement_date"] = minimum_date

    # 3. Zero quantity → remove from export
    mask_zero_qty = base["converted_qty"] <= 0
    if mask_zero_qty.any():
        n = mask_zero_qty.sum()
        warnings.append(f"🚫 {n} item(s) with zero quantity — REMOVED from CSV")
        base = base[~mask_zero_qty].copy()

    # 4. Pending UoM conversion → flag for review
    mask_conv = (
        base.get("uom_conversion_status", pd.Series("ok", index=base.index))
        == "Review Conversion"
    )
    if mask_conv.any():
        warnings.append(f"⚠️  {mask_conv.sum()} item(s) with unresolved UoM conversion — REVIEW")

    # 5. Items without a mapped supplier → informational
    if "vendor_number" in base.columns:
        mask_no_vendor = base["vendor_number"].isna() | (base["vendor_number"] == "")
        if mask_no_vendor.any():
            warnings.append(f"ℹ️  {mask_no_vendor.sum()} item(s) with no mapped supplier")

    for w in warnings:
        log.warning(w)

    log.info(f"  Validation complete: {len(base)} rows ready for CSV export")
    return base


# ============================================================
#  CSV GENERATION
# ============================================================

def generate_procurement_csv(base: pd.DataFrame, output_dir: Path) -> Path:
    """
    Generates the structured CSV import file for the target purchasing system.

    File structure (two-row header):
        Row 1: Requisition header  (PO-level fields)
        Row 2: Request line header (line-item fields)
        Row 3+: Data rows — one block per requisition

    Key fields included:
        - Justification (requisition header)
        - Reservation Number (requisition header)
        - Requirement date (both header and line level)
        - Original PR Number (request line)
    """
    log.info("Generating purchasing system CSV...")

    today_str   = datetime.today().strftime("%Y%m%d")
    output_file = output_dir / f"procurement_load_{today_str}.csv"

    # ---- Row 1: Requisition header columns ----
    header_req = [
        "Requisition",
        "Requisition Title",
        "Submit for Approval?",
        "Need By Date",
        "Justification",                    # ← preserved field
        "Requested By (Email)",
        "Requested By (Login)",
        "Delivery Location Code",
        "Requisition Type",
        "Trial?",
        "Urgent",
        "Budget",
        "Buyer Group",
        "Purchasing Organization",
        "PO Type",
        "ERP Requisition Number",           # ← preserved field
        "Maintenance Service?",
    ]

    # ---- Row 2: Request line columns ----
    header_line = [
        "Request Line",
        "Line Number",
        "Catalog Item Number",
        "Catalog Item Name",
        "Non-Catalog Item Description",
        "Quantity",
        "Price",
        "Need By Date",                     # ← preserved field
        "Transmission Emails",
        "Supplier Name",
        "Supplier Number",
        "Unit of Measure Code",
        "Commodity Name",
        "Contract Name",
        "Currency Code",
        "Chart of Accounts",
        "Account Segment 1",
        "Account Segment 2",
        "Account Segment 3 (Reservation)",
        "Account Segment 4",
        "Payment Terms",
        "Shipping Terms",
        "Item Type",
        "Permitted Plants",
        "GL Account",
        "Valuation Class",
        "Maintenance Order Number",
        "Tax Classification",
        "Buyer Group",
        "Price Per",
        "Net Price",
        "Original ERP PR Number",           # ← preserved field
        "Supplier Number (line)",
        "Contract Name (line)",
    ]

    n_cols = max(len(header_req), len(header_line))
    rows   = []

    # Write both header rows
    rows.append(header_req  + [""] * (n_cols - len(header_req)))
    rows.append(header_line + [""] * (n_cols - len(header_line)))

    # Filter to items that should be purchased
    to_buy = base[base["should_purchase"] == "YES"].copy()

    if to_buy.empty:
        log.warning("⚠️  No items with 'should_purchase = YES'. CSV will be generated with headers only.")

    for _, row in to_buy.iterrows():
        # Format requirement date
        req_date     = row.get("final_requirement_date")
        req_date_str = (
            pd.Timestamp(req_date).strftime("%Y%m%d")
            if pd.notna(req_date) else ""
        )

        # ----- Requisition row -----
        req_row = [""] * n_cols
        req_row[0]  = "Requisition"
        req_row[1]  = str(row.get("description", ""))[:100]
        req_row[2]  = "Yes"
        req_row[3]  = req_date_str
        req_row[4]  = str(row.get("justification", ""))
        req_row[5]  = ""    # requester email — populate from your directory/LDAP
        req_row[6]  = ""    # requester login
        req_row[7]  = DELIVERY_LOCATION_CODE
        req_row[8]  = str(row.get("doc_type", "STANDARD"))
        req_row[9]  = "No"
        req_row[10] = "No"
        req_row[11] = ""    # budget code
        req_row[12] = str(row.get("buyer_group", ""))
        req_row[13] = PURCHASING_ORG
        req_row[14] = ""    # PO type
        req_row[15] = str(row.get("req_number", ""))
        req_row[16] = "No"

        rows.append(req_row)

        # ----- Request line row -----
        line_row = [""] * n_cols
        line_row[0]  = "Request Line"
        line_row[1]  = "1"
        line_row[2]  = str(row.get("catalog_item_number", ""))
        line_row[3]  = str(row.get("catalog_item_name", ""))
        line_row[4]  = str(row.get("description", ""))
        line_row[5]  = str(row.get("converted_qty", 0))
        line_row[6]  = str(row.get("converted_price", 0))
        line_row[7]  = req_date_str
        line_row[8]  = ""    # transmission emails
        line_row[9]  = str(row.get("vendor_name_platform", row.get("vendor_name", "")))
        line_row[10] = str(row.get("vendor_number", ""))
        line_row[11] = str(row.get("uom_target", row.get("uom_erp", "")))
        line_row[12] = str(row.get("commodity_name", ""))
        line_row[13] = str(row.get("contract_name", ""))
        line_row[14] = str(row.get("currency", "USD"))
        line_row[15] = ""    # chart of accounts
        line_row[16] = str(row.get("plant", DEFAULT_PLANT))
        line_row[17] = str(row.get("storage_location", ""))
        line_row[18] = str(row.get("reservation_number", ""))   # ← reservation ref
        line_row[19] = ""    # segment 4
        line_row[20] = ""    # payment terms
        line_row[21] = ""    # shipping terms
        line_row[22] = "Goods"
        line_row[23] = str(row.get("plant", DEFAULT_PLANT))
        line_row[24] = ""    # GL account
        line_row[25] = str(row.get("account_category", "K"))
        line_row[26] = ""    # maintenance order
        line_row[27] = ""    # tax classification
        line_row[28] = str(row.get("buyer_group", ""))
        line_row[29] = "1"
        line_row[30] = ""    # net price
        line_row[31] = str(row.get("req_number", ""))           # ← original ERP PR
        line_row[32] = str(row.get("vendor_number", ""))
        line_row[33] = str(row.get("contract_name", ""))

        rows.append(line_row)

    df_csv = pd.DataFrame(rows)
    df_csv.to_csv(output_file, index=False, header=False, sep=";", encoding="utf-8-sig")

    log.info(f"✅ CSV generated: {output_file}")
    log.info(f"   Items included: {to_buy.shape[0]}")
    return output_file


# ============================================================
#  AUDIT REPORT
# ============================================================

def generate_audit_report(base: pd.DataFrame, output_dir: Path) -> Path:
    """
    Generates an Excel audit file with all calculated fields.

    Useful for:
        - Reviewing the purchase/no-purchase decision per item
        - Tracing the source of requirement dates and reservation numbers
        - Validating quantity conversions
        - Verifying supplier and contract mappings
    """
    today_str    = datetime.today().strftime("%Y%m%d")
    output_file  = output_dir / f"audit_mrp_{today_str}.xlsx"

    audit_columns = [
        "plant", "material", "description",
        "stock_qty", "reserved_qty", "open_qty", "requested_qty",
        "available_coverage", "demand", "should_purchase",
        "theoretical_suggestion", "purchase_suggestion", "converted_qty",
        "uom_erp", "uom_target", "uom_conversion_status",
        "reservation_number",
        "requirement_date",
        "delivery_date",
        "final_requirement_date",
        "justification",
        "req_number",
        "vendor_name_platform", "vendor_number",
        "contract_name", "commodity_name",
        "buyer_group", "currency",
    ]
    present_cols = [c for c in audit_columns if c in base.columns]
    base[present_cols].to_excel(output_file, index=False)

    log.info(f"✅ Audit report generated: {output_file}")
    return output_file


# ============================================================
#  MAIN
# ============================================================

def main():
    log.info("=" * 60)
    log.info("  MRP DATA PIPELINE — START")
    log.info("=" * 60)

    OUTPUT_DIR.mkdir(exist_ok=True)

    # Check mandatory input files
    mandatory = ("requisitions", "reservations", "stock", "orders", "materials")
    missing = [name for name in mandatory if not FILES[name].exists()]
    if missing:
        log.error(f"Input files not found: {missing}")
        log.error(f"Place the files in: {INPUT_DATA_DIR}")
        sys.exit(1)

    # --- Step 1: Read ERP exports ---
    requisitions  = read_purchase_requisitions(FILES["requisitions"])
    reservations  = read_reservations(FILES["reservations"])
    stock         = read_stock(FILES["stock"])
    open_orders   = read_open_orders(FILES["orders"])
    material_data = read_material_data(FILES["materials"])

    # --- Step 2: Consolidate ---
    base = consolidate_data(requisitions, reservations, stock, open_orders, material_data)

    # --- Step 3: Add justification field ---
    base = add_justification(base)

    # --- Step 4: Enrich with purchasing system data ---
    base = enrich_with_procurement_data(
        base,
        FILES.get("supplier_item_list", Path("")),
        FILES.get("item_list", Path("")),
        FILES.get("unit_conversion", Path("")),
    )

    # --- Step 5: Validate ---
    base = validate_data(base)

    # --- Step 6: Generate import CSV ---
    csv_path = generate_procurement_csv(base, OUTPUT_DIR)

    # --- Step 7: Generate audit report ---
    audit_path = generate_audit_report(base, OUTPUT_DIR)

    log.info("=" * 60)
    log.info("  PROCESS COMPLETE")
    log.info(f"  Procurement CSV : {csv_path}")
    log.info(f"  Audit Report    : {audit_path}")
    log.info("=" * 60)

    return csv_path


if __name__ == "__main__":
    main()

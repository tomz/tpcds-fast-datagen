"""TPC-DS table schemas — PyArrow types, column names, row count formulas.

All 25 TPC-DS tables with:
- Ordered column names (matching dsdgen pipe-delimited output order)
- PyArrow data types
- Row count formula as a function of scale factor
- Companion table relationships (tables that must be generated together)

Reference: TPC-DS v2.10.0 specification, tpcds.sql from tpcds-kit
"""

from __future__ import annotations

import pyarrow as pa
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass(frozen=True)
class TableDef:
    """Definition of a TPC-DS table."""
    name: str
    columns: list[tuple[str, pa.DataType]]
    row_count: Callable[[int], int]  # f(scale_factor) -> row count
    companion: Optional[str] = None  # companion table generated in same pass

    @property
    def schema(self) -> pa.Schema:
        return pa.schema([(name, dtype) for name, dtype in self.columns])

    @property
    def column_names(self) -> list[str]:
        return [name for name, _ in self.columns]


# Type aliases for readability
_int = pa.int32()
_bigint = pa.int64()
_str = pa.string()
_date = pa.date32()
_time = pa.time32("s")
_dec72 = pa.decimal128(7, 2)
_dec52 = pa.decimal128(5, 2)
_dec152 = pa.decimal128(15, 2)


# === Dimension Tables ===

DBGEN_VERSION = TableDef(
    name="dbgen_version",
    columns=[
        ("dv_version", _str),
        ("dv_create_date", _date),
        ("dv_create_time", _time),
        ("dv_cmdline_args", _str),
    ],
    row_count=lambda sf: 1,
)

CUSTOMER_ADDRESS = TableDef(
    name="customer_address",
    columns=[
        ("ca_address_sk", _int),
        ("ca_address_id", _str),
        ("ca_street_number", _str),
        ("ca_street_name", _str),
        ("ca_street_type", _str),
        ("ca_suite_number", _str),
        ("ca_city", _str),
        ("ca_county", _str),
        ("ca_state", _str),
        ("ca_zip", _str),
        ("ca_country", _str),
        ("ca_gmt_offset", _dec52),
        ("ca_location_type", _str),
    ],
    row_count=lambda sf: sf * 50000,
)

CUSTOMER_DEMOGRAPHICS = TableDef(
    name="customer_demographics",
    columns=[
        ("cd_demo_sk", _int),
        ("cd_gender", _str),
        ("cd_marital_status", _str),
        ("cd_education_status", _str),
        ("cd_purchase_estimate", _int),
        ("cd_credit_rating", _str),
        ("cd_dep_count", _int),
        ("cd_dep_employed_count", _int),
        ("cd_dep_college_count", _int),
    ],
    row_count=lambda sf: 1920800,  # fixed
)

DATE_DIM = TableDef(
    name="date_dim",
    columns=[
        ("d_date_sk", _int),
        ("d_date_id", _str),
        ("d_date", _date),
        ("d_month_seq", _int),
        ("d_week_seq", _int),
        ("d_quarter_seq", _int),
        ("d_year", _int),
        ("d_dow", _int),
        ("d_moy", _int),
        ("d_dom", _int),
        ("d_qoy", _int),
        ("d_fy_year", _int),
        ("d_fy_quarter_seq", _int),
        ("d_fy_week_seq", _int),
        ("d_day_name", _str),
        ("d_quarter_name", _str),
        ("d_holiday", _str),
        ("d_weekend", _str),
        ("d_following_holiday", _str),
        ("d_first_dom", _int),
        ("d_last_dom", _int),
        ("d_same_day_ly", _int),
        ("d_same_day_lq", _int),
        ("d_current_day", _str),
        ("d_current_week", _str),
        ("d_current_month", _str),
        ("d_current_quarter", _str),
        ("d_current_year", _str),
    ],
    row_count=lambda sf: 73049,  # fixed
)

WAREHOUSE = TableDef(
    name="warehouse",
    columns=[
        ("w_warehouse_sk", _int),
        ("w_warehouse_id", _str),
        ("w_warehouse_name", _str),
        ("w_warehouse_sq_ft", _int),
        ("w_street_number", _str),
        ("w_street_name", _str),
        ("w_street_type", _str),
        ("w_suite_number", _str),
        ("w_city", _str),
        ("w_county", _str),
        ("w_state", _str),
        ("w_zip", _str),
        ("w_country", _str),
        ("w_gmt_offset", _dec52),
    ],
    row_count=lambda sf: max(5, sf),  # at least 5
)

SHIP_MODE = TableDef(
    name="ship_mode",
    columns=[
        ("sm_ship_mode_sk", _int),
        ("sm_ship_mode_id", _str),
        ("sm_type", _str),
        ("sm_code", _str),
        ("sm_carrier", _str),
        ("sm_contract", _str),
    ],
    row_count=lambda sf: 20,  # fixed
)

TIME_DIM = TableDef(
    name="time_dim",
    columns=[
        ("t_time_sk", _int),
        ("t_time_id", _str),
        ("t_time", _int),
        ("t_hour", _int),
        ("t_minute", _int),
        ("t_second", _int),
        ("t_am_pm", _str),
        ("t_shift", _str),
        ("t_sub_shift", _str),
        ("t_meal_time", _str),
    ],
    row_count=lambda sf: 86400,  # fixed
)

REASON = TableDef(
    name="reason",
    columns=[
        ("r_reason_sk", _int),
        ("r_reason_id", _str),
        ("r_reason_desc", _str),
    ],
    row_count=lambda sf: max(35, sf),  # at least 35
)

INCOME_BAND = TableDef(
    name="income_band",
    columns=[
        ("ib_income_band_sk", _int),
        ("ib_lower_bound", _int),
        ("ib_upper_bound", _int),
    ],
    row_count=lambda sf: 20,  # fixed
)

ITEM = TableDef(
    name="item",
    columns=[
        ("i_item_sk", _int),
        ("i_item_id", _str),
        ("i_rec_start_date", _date),
        ("i_rec_end_date", _date),
        ("i_item_desc", _str),
        ("i_current_price", _dec72),
        ("i_wholesale_cost", _dec72),
        ("i_brand_id", _int),
        ("i_brand", _str),
        ("i_class_id", _int),
        ("i_class", _str),
        ("i_category_id", _int),
        ("i_category", _str),
        ("i_manufact_id", _int),
        ("i_manufact", _str),
        ("i_size", _str),
        ("i_formulation", _str),
        ("i_color", _str),
        ("i_units", _str),
        ("i_container", _str),
        ("i_manager_id", _int),
        ("i_product_name", _str),
    ],
    row_count=lambda sf: sf * 18000,  # ~18K per SF for SF>=1
)

STORE = TableDef(
    name="store",
    columns=[
        ("s_store_sk", _int),
        ("s_store_id", _str),
        ("s_rec_start_date", _date),
        ("s_rec_end_date", _date),
        ("s_closed_date_sk", _int),
        ("s_store_name", _str),
        ("s_number_employees", _int),
        ("s_floor_space", _int),
        ("s_hours", _str),
        ("s_manager", _str),
        ("s_market_id", _int),
        ("s_geography_class", _str),
        ("s_market_desc", _str),
        ("s_market_manager", _str),
        ("s_division_id", _int),
        ("s_division_name", _str),
        ("s_company_id", _int),
        ("s_company_name", _str),
        ("s_street_number", _str),
        ("s_street_name", _str),
        ("s_street_type", _str),
        ("s_suite_number", _str),
        ("s_city", _str),
        ("s_county", _str),
        ("s_state", _str),
        ("s_zip", _str),
        ("s_country", _str),
        ("s_gmt_offset", _dec52),
        ("s_tax_precentage", _dec52),
    ],
    row_count=lambda sf: max(12, sf * 12 // 1),  # ~12 per SF
)

CALL_CENTER = TableDef(
    name="call_center",
    columns=[
        ("cc_call_center_sk", _int),
        ("cc_call_center_id", _str),
        ("cc_rec_start_date", _date),
        ("cc_rec_end_date", _date),
        ("cc_closed_date_sk", _int),
        ("cc_open_date_sk", _int),
        ("cc_name", _str),
        ("cc_class", _str),
        ("cc_employees", _int),
        ("cc_sq_ft", _int),
        ("cc_hours", _str),
        ("cc_manager", _str),
        ("cc_mkt_id", _int),
        ("cc_mkt_class", _str),
        ("cc_mkt_desc", _str),
        ("cc_market_manager", _str),
        ("cc_division", _int),
        ("cc_division_name", _str),
        ("cc_company", _int),
        ("cc_company_name", _str),
        ("cc_street_number", _str),
        ("cc_street_name", _str),
        ("cc_street_type", _str),
        ("cc_suite_number", _str),
        ("cc_city", _str),
        ("cc_county", _str),
        ("cc_state", _str),
        ("cc_zip", _str),
        ("cc_country", _str),
        ("cc_gmt_offset", _dec52),
        ("cc_tax_percentage", _dec52),
    ],
    row_count=lambda sf: max(6, sf * 6 // 1),  # ~6 per SF
)

CUSTOMER = TableDef(
    name="customer",
    columns=[
        ("c_customer_sk", _int),
        ("c_customer_id", _str),
        ("c_current_cdemo_sk", _int),
        ("c_current_hdemo_sk", _int),
        ("c_current_addr_sk", _int),
        ("c_first_shipto_date_sk", _int),
        ("c_first_sales_date_sk", _int),
        ("c_salutation", _str),
        ("c_first_name", _str),
        ("c_last_name", _str),
        ("c_preferred_cust_flag", _str),
        ("c_birth_day", _int),
        ("c_birth_month", _int),
        ("c_birth_year", _int),
        ("c_birth_country", _str),
        ("c_login", _str),
        ("c_email_address", _str),
        ("c_last_review_date_sk", _int),
    ],
    row_count=lambda sf: sf * 100000,
)

WEB_SITE = TableDef(
    name="web_site",
    columns=[
        ("web_site_sk", _int),
        ("web_site_id", _str),
        ("web_rec_start_date", _date),
        ("web_rec_end_date", _date),
        ("web_name", _str),
        ("web_open_date_sk", _int),
        ("web_close_date_sk", _int),
        ("web_class", _str),
        ("web_manager", _str),
        ("web_mkt_id", _int),
        ("web_mkt_class", _str),
        ("web_mkt_desc", _str),
        ("web_market_manager", _str),
        ("web_company_id", _int),
        ("web_company_name", _str),
        ("web_street_number", _str),
        ("web_street_name", _str),
        ("web_street_type", _str),
        ("web_suite_number", _str),
        ("web_city", _str),
        ("web_county", _str),
        ("web_state", _str),
        ("web_zip", _str),
        ("web_country", _str),
        ("web_gmt_offset", _dec52),
        ("web_tax_percentage", _dec52),
    ],
    row_count=lambda sf: max(30, sf * 30 // 1),  # ~30 per SF
)

HOUSEHOLD_DEMOGRAPHICS = TableDef(
    name="household_demographics",
    columns=[
        ("hd_demo_sk", _int),
        ("hd_income_band_sk", _int),
        ("hd_buy_potential", _str),
        ("hd_dep_count", _int),
        ("hd_vehicle_count", _int),
    ],
    row_count=lambda sf: 7200,  # fixed
)

WEB_PAGE = TableDef(
    name="web_page",
    columns=[
        ("wp_web_page_sk", _int),
        ("wp_web_page_id", _str),
        ("wp_rec_start_date", _date),
        ("wp_rec_end_date", _date),
        ("wp_creation_date_sk", _int),
        ("wp_access_date_sk", _int),
        ("wp_autogen_flag", _str),
        ("wp_customer_sk", _int),
        ("wp_url", _str),
        ("wp_type", _str),
        ("wp_char_count", _int),
        ("wp_link_count", _int),
        ("wp_image_count", _int),
        ("wp_max_ad_count", _int),
    ],
    row_count=lambda sf: sf * 60,  # ~60 per SF
)

PROMOTION = TableDef(
    name="promotion",
    columns=[
        ("p_promo_sk", _int),
        ("p_promo_id", _str),
        ("p_start_date_sk", _int),
        ("p_end_date_sk", _int),
        ("p_item_sk", _int),
        ("p_cost", _dec152),
        ("p_response_target", _int),
        ("p_promo_name", _str),
        ("p_channel_dmail", _str),
        ("p_channel_email", _str),
        ("p_channel_catalog", _str),
        ("p_channel_tv", _str),
        ("p_channel_radio", _str),
        ("p_channel_press", _str),
        ("p_channel_event", _str),
        ("p_channel_demo", _str),
        ("p_channel_details", _str),
        ("p_purpose", _str),
        ("p_discount_active", _str),
    ],
    row_count=lambda sf: max(300, sf * 300),
)

CATALOG_PAGE = TableDef(
    name="catalog_page",
    columns=[
        ("cp_catalog_page_sk", _int),
        ("cp_catalog_page_id", _str),
        ("cp_start_date_sk", _int),
        ("cp_end_date_sk", _int),
        ("cp_department", _str),
        ("cp_catalog_number", _int),
        ("cp_catalog_page_number", _int),
        ("cp_description", _str),
        ("cp_type", _str),
    ],
    row_count=lambda sf: sf * 11718 + 12,  # approximate
)

# === Fact Tables ===

INVENTORY = TableDef(
    name="inventory",
    columns=[
        ("inv_date_sk", _int),
        ("inv_item_sk", _int),
        ("inv_warehouse_sk", _int),
        ("inv_quantity_on_hand", _int),
    ],
    row_count=lambda sf: sf * 11745000,
)

STORE_RETURNS = TableDef(
    name="store_returns",
    columns=[
        ("sr_returned_date_sk", _int),
        ("sr_return_time_sk", _int),
        ("sr_item_sk", _int),
        ("sr_customer_sk", _int),
        ("sr_cdemo_sk", _int),
        ("sr_hdemo_sk", _int),
        ("sr_addr_sk", _int),
        ("sr_store_sk", _int),
        ("sr_reason_sk", _int),
        ("sr_ticket_number", _int),
        ("sr_return_quantity", _int),
        ("sr_return_amt", _dec72),
        ("sr_return_tax", _dec72),
        ("sr_return_amt_inc_tax", _dec72),
        ("sr_fee", _dec72),
        ("sr_return_ship_cost", _dec72),
        ("sr_refunded_cash", _dec72),
        ("sr_reversed_charge", _dec72),
        ("sr_store_credit", _dec72),
        ("sr_net_loss", _dec72),
    ],
    row_count=lambda sf: sf * 287514,  # verified at SF=1
    companion="store_sales",
)

CATALOG_RETURNS = TableDef(
    name="catalog_returns",
    columns=[
        ("cr_returned_date_sk", _int),
        ("cr_returned_time_sk", _int),
        ("cr_item_sk", _int),
        ("cr_refunded_customer_sk", _int),
        ("cr_refunded_cdemo_sk", _int),
        ("cr_refunded_hdemo_sk", _int),
        ("cr_refunded_addr_sk", _int),
        ("cr_returning_customer_sk", _int),
        ("cr_returning_cdemo_sk", _int),
        ("cr_returning_hdemo_sk", _int),
        ("cr_returning_addr_sk", _int),
        ("cr_call_center_sk", _int),
        ("cr_catalog_page_sk", _int),
        ("cr_ship_mode_sk", _int),
        ("cr_warehouse_sk", _int),
        ("cr_reason_sk", _int),
        ("cr_order_number", _int),
        ("cr_return_quantity", _int),
        ("cr_return_amount", _dec72),
        ("cr_return_tax", _dec72),
        ("cr_return_amt_inc_tax", _dec72),
        ("cr_fee", _dec72),
        ("cr_return_ship_cost", _dec72),
        ("cr_refunded_cash", _dec72),
        ("cr_reversed_charge", _dec72),
        ("cr_store_credit", _dec72),
        ("cr_net_loss", _dec72),
    ],
    row_count=lambda sf: sf * 144067,
    companion="catalog_sales",
)

WEB_RETURNS = TableDef(
    name="web_returns",
    columns=[
        ("wr_returned_date_sk", _int),
        ("wr_returned_time_sk", _int),
        ("wr_item_sk", _int),
        ("wr_refunded_customer_sk", _int),
        ("wr_refunded_cdemo_sk", _int),
        ("wr_refunded_hdemo_sk", _int),
        ("wr_refunded_addr_sk", _int),
        ("wr_returning_customer_sk", _int),
        ("wr_returning_cdemo_sk", _int),
        ("wr_returning_hdemo_sk", _int),
        ("wr_returning_addr_sk", _int),
        ("wr_web_page_sk", _int),
        ("wr_reason_sk", _int),
        ("wr_order_number", _int),
        ("wr_return_quantity", _int),
        ("wr_return_amt", _dec72),
        ("wr_return_tax", _dec72),
        ("wr_return_amt_inc_tax", _dec72),
        ("wr_fee", _dec72),
        ("wr_return_ship_cost", _dec72),
        ("wr_refunded_cash", _dec72),
        ("wr_reversed_charge", _dec72),
        ("wr_account_credit", _dec72),
        ("wr_net_loss", _dec72),
    ],
    row_count=lambda sf: sf * 71763,
    companion="web_sales",
)

WEB_SALES = TableDef(
    name="web_sales",
    columns=[
        ("ws_sold_date_sk", _int),
        ("ws_sold_time_sk", _int),
        ("ws_ship_date_sk", _int),
        ("ws_item_sk", _int),
        ("ws_bill_customer_sk", _int),
        ("ws_bill_cdemo_sk", _int),
        ("ws_bill_hdemo_sk", _int),
        ("ws_bill_addr_sk", _int),
        ("ws_ship_customer_sk", _int),
        ("ws_ship_cdemo_sk", _int),
        ("ws_ship_hdemo_sk", _int),
        ("ws_ship_addr_sk", _int),
        ("ws_web_page_sk", _int),
        ("ws_web_site_sk", _int),
        ("ws_ship_mode_sk", _int),
        ("ws_warehouse_sk", _int),
        ("ws_promo_sk", _int),
        ("ws_order_number", _int),
        ("ws_quantity", _int),
        ("ws_wholesale_cost", _dec72),
        ("ws_list_price", _dec72),
        ("ws_sales_price", _dec72),
        ("ws_ext_discount_amt", _dec72),
        ("ws_ext_sales_price", _dec72),
        ("ws_ext_wholesale_cost", _dec72),
        ("ws_ext_list_price", _dec72),
        ("ws_ext_tax", _dec72),
        ("ws_coupon_amt", _dec72),
        ("ws_ext_ship_cost", _dec72),
        ("ws_net_paid", _dec72),
        ("ws_net_paid_inc_tax", _dec72),
        ("ws_net_paid_inc_ship", _dec72),
        ("ws_net_paid_inc_ship_tax", _dec72),
        ("ws_net_profit", _dec72),
    ],
    row_count=lambda sf: sf * 719384,
    companion="web_returns",
)

CATALOG_SALES = TableDef(
    name="catalog_sales",
    columns=[
        ("cs_sold_date_sk", _int),
        ("cs_sold_time_sk", _int),
        ("cs_ship_date_sk", _int),
        ("cs_bill_customer_sk", _int),
        ("cs_bill_cdemo_sk", _int),
        ("cs_bill_hdemo_sk", _int),
        ("cs_bill_addr_sk", _int),
        ("cs_ship_customer_sk", _int),
        ("cs_ship_cdemo_sk", _int),
        ("cs_ship_hdemo_sk", _int),
        ("cs_ship_addr_sk", _int),
        ("cs_call_center_sk", _int),
        ("cs_catalog_page_sk", _int),
        ("cs_ship_mode_sk", _int),
        ("cs_warehouse_sk", _int),
        ("cs_item_sk", _int),
        ("cs_promo_sk", _int),
        ("cs_order_number", _int),
        ("cs_quantity", _int),
        ("cs_wholesale_cost", _dec72),
        ("cs_list_price", _dec72),
        ("cs_sales_price", _dec72),
        ("cs_ext_discount_amt", _dec72),
        ("cs_ext_sales_price", _dec72),
        ("cs_ext_wholesale_cost", _dec72),
        ("cs_ext_list_price", _dec72),
        ("cs_ext_tax", _dec72),
        ("cs_coupon_amt", _dec72),
        ("cs_ext_ship_cost", _dec72),
        ("cs_net_paid", _dec72),
        ("cs_net_paid_inc_tax", _dec72),
        ("cs_net_paid_inc_ship", _dec72),
        ("cs_net_paid_inc_ship_tax", _dec72),
        ("cs_net_profit", _dec72),
    ],
    row_count=lambda sf: sf * 1441548,
    companion="catalog_returns",
)

STORE_SALES = TableDef(
    name="store_sales",
    columns=[
        ("ss_sold_date_sk", _int),
        ("ss_sold_time_sk", _int),
        ("ss_item_sk", _int),
        ("ss_customer_sk", _int),
        ("ss_cdemo_sk", _int),
        ("ss_hdemo_sk", _int),
        ("ss_addr_sk", _int),
        ("ss_store_sk", _int),
        ("ss_promo_sk", _int),
        ("ss_ticket_number", _int),
        ("ss_quantity", _int),
        ("ss_wholesale_cost", _dec72),
        ("ss_list_price", _dec72),
        ("ss_sales_price", _dec72),
        ("ss_ext_discount_amt", _dec72),
        ("ss_ext_sales_price", _dec72),
        ("ss_ext_wholesale_cost", _dec72),
        ("ss_ext_list_price", _dec72),
        ("ss_ext_tax", _dec72),
        ("ss_coupon_amt", _dec72),
        ("ss_net_paid", _dec72),
        ("ss_net_paid_inc_tax", _dec72),
        ("ss_net_profit", _dec72),
    ],
    row_count=lambda sf: sf * 2880404,
    companion="store_returns",
)


# All tables indexed by name
ALL_TABLES: dict[str, TableDef] = {
    t.name: t for t in [
        DBGEN_VERSION,
        CUSTOMER_ADDRESS, CUSTOMER_DEMOGRAPHICS, DATE_DIM,
        WAREHOUSE, SHIP_MODE, TIME_DIM, REASON, INCOME_BAND,
        ITEM, STORE, CALL_CENTER, CUSTOMER, WEB_SITE,
        HOUSEHOLD_DEMOGRAPHICS, WEB_PAGE, PROMOTION, CATALOG_PAGE,
        INVENTORY,
        STORE_SALES, STORE_RETURNS,
        CATALOG_SALES, CATALOG_RETURNS,
        WEB_SALES, WEB_RETURNS,
    ]
}

# Fact tables (large, benefit from parallel generation)
FACT_TABLES = [
    "store_sales", "store_returns",
    "catalog_sales", "catalog_returns",
    "web_sales", "web_returns",
    "inventory",
]

# Companion pairs — these are generated together by dsdgen
# When generating store_sales, store_returns is produced as a side-effect (and vice versa)
COMPANION_PAIRS = {
    "store_sales": "store_returns",
    "store_returns": "store_sales",
    "catalog_sales": "catalog_returns",
    "catalog_returns": "catalog_sales",
    "web_sales": "web_returns",
    "web_returns": "web_sales",
}

# The "primary" table in each companion pair (the one you pass to dsdgen -TABLE)
COMPANION_PRIMARY = {
    "store_sales": "store_sales",
    "store_returns": "store_sales",
    "catalog_sales": "catalog_sales",
    "catalog_returns": "catalog_sales",
    "web_sales": "web_sales",
    "web_returns": "web_sales",
}

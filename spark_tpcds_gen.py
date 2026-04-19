#!/usr/bin/env python3
"""Distributed TPC-DS data generation using Spark + dsdgen.

Uses mapPartitions to run dsdgen on each executor node, streaming
CSV-to-Parquet conversion with constant memory. Output to HDFS/ABFS/local.

Architecture
------------
* Driver (Python 3.6, NO pyarrow): calls plan_tasks() + Spark APIs only.
* Executors (have pyarrow): run_dsdgen_task() -> _build_all_tables() ->
  _streaming_dat_to_parquet().

Usage::

    spark-submit --master yarn \\
        --archives /path/to/conda_env.tar.gz#conda_env \\
        --num-executors 10 --executor-cores 8 --executor-memory 16g \\
        --files /path/to/tpcds-kit/dsdgen,/path/to/tpcds-kit/tpcds.idx \\
        spark_tpcds_gen.py --scale 1000 --output abfs:///tpcds/sf1000

Self-contained: no tpcds_fast_datagen package required.
Compatible with Python 3.6+.
"""

import argparse
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Row-count estimates — driver-safe, no pyarrow
# ---------------------------------------------------------------------------

# Table name -> callable(scale_factor) -> approximate row count
TABLE_ROW_COUNTS = {
    "dbgen_version":          lambda sf: 1,
    "customer_address":       lambda sf: sf * 50000,
    "customer_demographics":  lambda sf: 1920800,
    "date_dim":               lambda sf: 73049,
    "warehouse":              lambda sf: max(5, sf * 5),
    "ship_mode":              lambda sf: 20,
    "time_dim":               lambda sf: 86400,
    "reason":                 lambda sf: max(35, sf + 35),
    "income_band":            lambda sf: 20,
    "item":                   lambda sf: sf * 18000,
    "store":                  lambda sf: max(12, sf * 12),
    "call_center":            lambda sf: max(6, sf * 6),
    "customer":               lambda sf: sf * 100000,
    "web_site":               lambda sf: max(30, sf * 30),
    "household_demographics": lambda sf: 7200,
    "web_page":               lambda sf: sf * 60,
    "promotion":              lambda sf: max(300, sf * 300),
    "catalog_page":           lambda sf: sf * 11718 + 12,
    "inventory":              lambda sf: sf * 11745000,
    "store_sales":            lambda sf: sf * 2880404,
    "store_returns":          lambda sf: sf * 287514,
    "catalog_sales":          lambda sf: sf * 1441548,
    "catalog_returns":        lambda sf: sf * 144067,
    "web_sales":              lambda sf: sf * 719384,
    "web_returns":            lambda sf: sf * 71763,
}  # type: Dict[str, Callable[[int], int]]

# Companion pairs — dsdgen generates these together in one run.
COMPANION_PAIRS = {
    "store_sales":    "store_returns",
    "store_returns":  "store_sales",
    "catalog_sales":  "catalog_returns",
    "catalog_returns": "catalog_sales",
    "web_sales":      "web_returns",
    "web_returns":    "web_sales",
}  # type: Dict[str, str]

# The table name passed to dsdgen -TABLE for each companion pair.
COMPANION_PRIMARY = {
    "store_sales":    "store_sales",
    "store_returns":  "store_sales",
    "catalog_sales":  "catalog_sales",
    "catalog_returns": "catalog_sales",
    "web_sales":      "web_sales",
    "web_returns":    "web_sales",
}  # type: Dict[str, str]


# ---------------------------------------------------------------------------
# TableDef — lightweight container; pyarrow types stored at runtime (executor)
# ---------------------------------------------------------------------------

class TableDef(object):
    """Definition of a TPC-DS table schema."""

    def __init__(
        self,
        name,           # type: str
        columns,        # type: List[Tuple[str, object]]  # (col_name, pa_type)
        row_count,      # type: Callable[[int], int]
        companion=None, # type: Optional[str]
    ):
        self.name = name
        self.columns = columns
        self.row_count = row_count
        self.companion = companion

    @property
    def schema(self):
        """Return a pyarrow.Schema.  Must be called on executors only."""
        import pyarrow as pa
        return pa.schema([(n, t) for n, t in self.columns])

    @property
    def column_names(self):
        # type: () -> List[str]
        return [n for n, _ in self.columns]


# ---------------------------------------------------------------------------
# _build_all_tables — executor-only; imports pyarrow here
# ---------------------------------------------------------------------------

def _build_all_tables():
    # type: () -> Dict[str, TableDef]
    """Build and return all 25 TPC-DS TableDef objects.

    Imports pyarrow internally.  MUST be called only on executor nodes.
    """
    import pyarrow as pa

    # Convenient type aliases
    _int    = pa.int32()
    _bigint = pa.int64()
    _str    = pa.string()
    _date   = pa.date32()
    _time   = pa.time32("s")
    _dec72  = pa.decimal128(7, 2)
    _dec52  = pa.decimal128(5, 2)
    _dec152 = pa.decimal128(15, 2)

    # ------------------------------------------------------------------
    # Dimension tables
    # ------------------------------------------------------------------

    dbgen_version = TableDef(
        name="dbgen_version",
        columns=[
            ("dv_version",       _str),
            ("dv_create_date",   _date),
            ("dv_create_time",   _time),
            ("dv_cmdline_args",  _str),
        ],
        row_count=lambda sf: 1,
    )

    customer_address = TableDef(
        name="customer_address",
        columns=[
            ("ca_address_sk",    _int),
            ("ca_address_id",    _str),
            ("ca_street_number", _str),
            ("ca_street_name",   _str),
            ("ca_street_type",   _str),
            ("ca_suite_number",  _str),
            ("ca_city",          _str),
            ("ca_county",        _str),
            ("ca_state",         _str),
            ("ca_zip",           _str),
            ("ca_country",       _str),
            ("ca_gmt_offset",    _dec52),
            ("ca_location_type", _str),
        ],
        row_count=lambda sf: sf * 50000,
    )

    customer_demographics = TableDef(
        name="customer_demographics",
        columns=[
            ("cd_demo_sk",           _int),
            ("cd_gender",            _str),
            ("cd_marital_status",    _str),
            ("cd_education_status",  _str),
            ("cd_purchase_estimate", _int),
            ("cd_credit_rating",     _str),
            ("cd_dep_count",         _int),
            ("cd_dep_employed_count",_int),
            ("cd_dep_college_count", _int),
        ],
        row_count=lambda sf: 1920800,
    )

    date_dim = TableDef(
        name="date_dim",
        columns=[
            ("d_date_sk",          _int),
            ("d_date_id",          _str),
            ("d_date",             _date),
            ("d_month_seq",        _int),
            ("d_week_seq",         _int),
            ("d_quarter_seq",      _int),
            ("d_year",             _int),
            ("d_dow",              _int),
            ("d_moy",              _int),
            ("d_dom",              _int),
            ("d_qoy",              _int),
            ("d_fy_year",          _int),
            ("d_fy_quarter_seq",   _int),
            ("d_fy_week_seq",      _int),
            ("d_day_name",         _str),
            ("d_quarter_name",     _str),
            ("d_holiday",          _str),
            ("d_weekend",          _str),
            ("d_following_holiday",_str),
            ("d_first_dom",        _int),
            ("d_last_dom",         _int),
            ("d_same_day_ly",      _int),
            ("d_same_day_lq",      _int),
            ("d_current_day",      _str),
            ("d_current_week",     _str),
            ("d_current_month",    _str),
            ("d_current_quarter",  _str),
            ("d_current_year",     _str),
        ],
        row_count=lambda sf: 73049,
    )

    warehouse = TableDef(
        name="warehouse",
        columns=[
            ("w_warehouse_sk",    _int),
            ("w_warehouse_id",    _str),
            ("w_warehouse_name",  _str),
            ("w_warehouse_sq_ft", _int),
            ("w_street_number",   _str),
            ("w_street_name",     _str),
            ("w_street_type",     _str),
            ("w_suite_number",    _str),
            ("w_city",            _str),
            ("w_county",          _str),
            ("w_state",           _str),
            ("w_zip",             _str),
            ("w_country",         _str),
            ("w_gmt_offset",      _dec52),
        ],
        row_count=lambda sf: max(5, sf * 5),
    )

    ship_mode = TableDef(
        name="ship_mode",
        columns=[
            ("sm_ship_mode_sk", _int),
            ("sm_ship_mode_id", _str),
            ("sm_type",         _str),
            ("sm_code",         _str),
            ("sm_carrier",      _str),
            ("sm_contract",     _str),
        ],
        row_count=lambda sf: 20,
    )

    time_dim = TableDef(
        name="time_dim",
        columns=[
            ("t_time_sk",  _int),
            ("t_time_id",  _str),
            ("t_time",     _int),
            ("t_hour",     _int),
            ("t_minute",   _int),
            ("t_second",   _int),
            ("t_am_pm",    _str),
            ("t_shift",    _str),
            ("t_sub_shift",_str),
            ("t_meal_time",_str),
        ],
        row_count=lambda sf: 86400,
    )

    reason = TableDef(
        name="reason",
        columns=[
            ("r_reason_sk",   _int),
            ("r_reason_id",   _str),
            ("r_reason_desc", _str),
        ],
        row_count=lambda sf: max(35, sf + 35),
    )

    income_band = TableDef(
        name="income_band",
        columns=[
            ("ib_income_band_sk", _int),
            ("ib_lower_bound",    _int),
            ("ib_upper_bound",    _int),
        ],
        row_count=lambda sf: 20,
    )

    item = TableDef(
        name="item",
        columns=[
            ("i_item_sk",        _int),
            ("i_item_id",        _str),
            ("i_rec_start_date", _date),
            ("i_rec_end_date",   _date),
            ("i_item_desc",      _str),
            ("i_current_price",  _dec72),
            ("i_wholesale_cost", _dec72),
            ("i_brand_id",       _int),
            ("i_brand",          _str),
            ("i_class_id",       _int),
            ("i_class",          _str),
            ("i_category_id",    _int),
            ("i_category",       _str),
            ("i_manufact_id",    _int),
            ("i_manufact",       _str),
            ("i_size",           _str),
            ("i_formulation",    _str),
            ("i_color",          _str),
            ("i_units",          _str),
            ("i_container",      _str),
            ("i_manager_id",     _int),
            ("i_product_name",   _str),
        ],
        row_count=lambda sf: sf * 18000,
    )

    store = TableDef(
        name="store",
        columns=[
            ("s_store_sk",        _int),
            ("s_store_id",        _str),
            ("s_rec_start_date",  _date),
            ("s_rec_end_date",    _date),
            ("s_closed_date_sk",  _int),
            ("s_store_name",      _str),
            ("s_number_employees",_int),
            ("s_floor_space",     _int),
            ("s_hours",           _str),
            ("s_manager",         _str),
            ("s_market_id",       _int),
            ("s_geography_class", _str),
            ("s_market_desc",     _str),
            ("s_market_manager",  _str),
            ("s_division_id",     _int),
            ("s_division_name",   _str),
            ("s_company_id",      _int),
            ("s_company_name",    _str),
            ("s_street_number",   _str),
            ("s_street_name",     _str),
            ("s_street_type",     _str),
            ("s_suite_number",    _str),
            ("s_city",            _str),
            ("s_county",          _str),
            ("s_state",           _str),
            ("s_zip",             _str),
            ("s_country",         _str),
            ("s_gmt_offset",      _dec52),
            ("s_tax_precentage",  _dec52),
        ],
        row_count=lambda sf: max(12, sf * 12),
    )

    call_center = TableDef(
        name="call_center",
        columns=[
            ("cc_call_center_sk", _int),
            ("cc_call_center_id", _str),
            ("cc_rec_start_date", _date),
            ("cc_rec_end_date",   _date),
            ("cc_closed_date_sk", _int),
            ("cc_open_date_sk",   _int),
            ("cc_name",           _str),
            ("cc_class",          _str),
            ("cc_employees",      _int),
            ("cc_sq_ft",          _int),
            ("cc_hours",          _str),
            ("cc_manager",        _str),
            ("cc_mkt_id",         _int),
            ("cc_mkt_class",      _str),
            ("cc_mkt_desc",       _str),
            ("cc_market_manager", _str),
            ("cc_division",       _int),
            ("cc_division_name",  _str),
            ("cc_company",        _int),
            ("cc_company_name",   _str),
            ("cc_street_number",  _str),
            ("cc_street_name",    _str),
            ("cc_street_type",    _str),
            ("cc_suite_number",   _str),
            ("cc_city",           _str),
            ("cc_county",         _str),
            ("cc_state",          _str),
            ("cc_zip",            _str),
            ("cc_country",        _str),
            ("cc_gmt_offset",     _dec52),
            ("cc_tax_percentage", _dec52),
        ],
        row_count=lambda sf: max(6, sf * 6),
    )

    customer = TableDef(
        name="customer",
        columns=[
            ("c_customer_sk",          _int),
            ("c_customer_id",          _str),
            ("c_current_cdemo_sk",     _int),
            ("c_current_hdemo_sk",     _int),
            ("c_current_addr_sk",      _int),
            ("c_first_shipto_date_sk", _int),
            ("c_first_sales_date_sk",  _int),
            ("c_salutation",           _str),
            ("c_first_name",           _str),
            ("c_last_name",            _str),
            ("c_preferred_cust_flag",  _str),
            ("c_birth_day",            _int),
            ("c_birth_month",          _int),
            ("c_birth_year",           _int),
            ("c_birth_country",        _str),
            ("c_login",                _str),
            ("c_email_address",        _str),
            ("c_last_review_date_sk",  _int),
        ],
        row_count=lambda sf: sf * 100000,
    )

    web_site = TableDef(
        name="web_site",
        columns=[
            ("web_site_sk",        _int),
            ("web_site_id",        _str),
            ("web_rec_start_date", _date),
            ("web_rec_end_date",   _date),
            ("web_name",           _str),
            ("web_open_date_sk",   _int),
            ("web_close_date_sk",  _int),
            ("web_class",          _str),
            ("web_manager",        _str),
            ("web_mkt_id",         _int),
            ("web_mkt_class",      _str),
            ("web_mkt_desc",       _str),
            ("web_market_manager", _str),
            ("web_company_id",     _int),
            ("web_company_name",   _str),
            ("web_street_number",  _str),
            ("web_street_name",    _str),
            ("web_street_type",    _str),
            ("web_suite_number",   _str),
            ("web_city",           _str),
            ("web_county",         _str),
            ("web_state",          _str),
            ("web_zip",            _str),
            ("web_country",        _str),
            ("web_gmt_offset",     _dec52),
            ("web_tax_percentage", _dec52),
        ],
        row_count=lambda sf: max(30, sf * 30),
    )

    household_demographics = TableDef(
        name="household_demographics",
        columns=[
            ("hd_demo_sk",        _int),
            ("hd_income_band_sk", _int),
            ("hd_buy_potential",  _str),
            ("hd_dep_count",      _int),
            ("hd_vehicle_count",  _int),
        ],
        row_count=lambda sf: 7200,
    )

    web_page = TableDef(
        name="web_page",
        columns=[
            ("wp_web_page_sk",      _int),
            ("wp_web_page_id",      _str),
            ("wp_rec_start_date",   _date),
            ("wp_rec_end_date",     _date),
            ("wp_creation_date_sk", _int),
            ("wp_access_date_sk",   _int),
            ("wp_autogen_flag",     _str),
            ("wp_customer_sk",      _int),
            ("wp_url",              _str),
            ("wp_type",             _str),
            ("wp_char_count",       _int),
            ("wp_link_count",       _int),
            ("wp_image_count",      _int),
            ("wp_max_ad_count",     _int),
        ],
        row_count=lambda sf: sf * 60,
    )

    promotion = TableDef(
        name="promotion",
        columns=[
            ("p_promo_sk",        _int),
            ("p_promo_id",        _str),
            ("p_start_date_sk",   _int),
            ("p_end_date_sk",     _int),
            ("p_item_sk",         _int),
            ("p_cost",            _dec152),
            ("p_response_target", _int),
            ("p_promo_name",      _str),
            ("p_channel_dmail",   _str),
            ("p_channel_email",   _str),
            ("p_channel_catalog", _str),
            ("p_channel_tv",      _str),
            ("p_channel_radio",   _str),
            ("p_channel_press",   _str),
            ("p_channel_event",   _str),
            ("p_channel_demo",    _str),
            ("p_channel_details", _str),
            ("p_purpose",         _str),
            ("p_discount_active", _str),
        ],
        row_count=lambda sf: max(300, sf * 300),
    )

    catalog_page = TableDef(
        name="catalog_page",
        columns=[
            ("cp_catalog_page_sk",     _int),
            ("cp_catalog_page_id",     _str),
            ("cp_start_date_sk",       _int),
            ("cp_end_date_sk",         _int),
            ("cp_department",          _str),
            ("cp_catalog_number",      _int),
            ("cp_catalog_page_number", _int),
            ("cp_description",         _str),
            ("cp_type",                _str),
        ],
        row_count=lambda sf: sf * 11718 + 12,
    )

    # ------------------------------------------------------------------
    # Fact tables
    # ------------------------------------------------------------------

    inventory = TableDef(
        name="inventory",
        columns=[
            ("inv_date_sk",          _int),
            ("inv_item_sk",          _int),
            ("inv_warehouse_sk",     _int),
            ("inv_quantity_on_hand", _int),
        ],
        row_count=lambda sf: sf * 11745000,
    )

    store_returns = TableDef(
        name="store_returns",
        columns=[
            ("sr_returned_date_sk",  _int),
            ("sr_return_time_sk",    _int),
            ("sr_item_sk",           _int),
            ("sr_customer_sk",       _int),
            ("sr_cdemo_sk",          _int),
            ("sr_hdemo_sk",          _int),
            ("sr_addr_sk",           _int),
            ("sr_store_sk",          _int),
            ("sr_reason_sk",         _int),
            ("sr_ticket_number",     _bigint),
            ("sr_return_quantity",   _int),
            ("sr_return_amt",        _dec72),
            ("sr_return_tax",        _dec72),
            ("sr_return_amt_inc_tax",_dec72),
            ("sr_fee",               _dec72),
            ("sr_return_ship_cost",  _dec72),
            ("sr_refunded_cash",     _dec72),
            ("sr_reversed_charge",   _dec72),
            ("sr_store_credit",      _dec72),
            ("sr_net_loss",          _dec72),
        ],
        row_count=lambda sf: sf * 287514,
        companion="store_sales",
    )

    catalog_returns = TableDef(
        name="catalog_returns",
        columns=[
            ("cr_returned_date_sk",       _int),
            ("cr_returned_time_sk",       _int),
            ("cr_item_sk",                _int),
            ("cr_refunded_customer_sk",   _int),
            ("cr_refunded_cdemo_sk",      _int),
            ("cr_refunded_hdemo_sk",      _int),
            ("cr_refunded_addr_sk",       _int),
            ("cr_returning_customer_sk",  _int),
            ("cr_returning_cdemo_sk",     _int),
            ("cr_returning_hdemo_sk",     _int),
            ("cr_returning_addr_sk",      _int),
            ("cr_call_center_sk",         _int),
            ("cr_catalog_page_sk",        _int),
            ("cr_ship_mode_sk",           _int),
            ("cr_warehouse_sk",           _int),
            ("cr_reason_sk",              _int),
            ("cr_order_number",           _bigint),
            ("cr_return_quantity",        _int),
            ("cr_return_amount",          _dec72),
            ("cr_return_tax",             _dec72),
            ("cr_return_amt_inc_tax",     _dec72),
            ("cr_fee",                    _dec72),
            ("cr_return_ship_cost",       _dec72),
            ("cr_refunded_cash",          _dec72),
            ("cr_reversed_charge",        _dec72),
            ("cr_store_credit",           _dec72),
            ("cr_net_loss",               _dec72),
        ],
        row_count=lambda sf: sf * 144067,
        companion="catalog_sales",
    )

    web_returns = TableDef(
        name="web_returns",
        columns=[
            ("wr_returned_date_sk",      _int),
            ("wr_returned_time_sk",      _int),
            ("wr_item_sk",               _int),
            ("wr_refunded_customer_sk",  _int),
            ("wr_refunded_cdemo_sk",     _int),
            ("wr_refunded_hdemo_sk",     _int),
            ("wr_refunded_addr_sk",      _int),
            ("wr_returning_customer_sk", _int),
            ("wr_returning_cdemo_sk",    _int),
            ("wr_returning_hdemo_sk",    _int),
            ("wr_returning_addr_sk",     _int),
            ("wr_web_page_sk",           _int),
            ("wr_reason_sk",             _int),
            ("wr_order_number",          _bigint),
            ("wr_return_quantity",       _int),
            ("wr_return_amt",            _dec72),
            ("wr_return_tax",            _dec72),
            ("wr_return_amt_inc_tax",    _dec72),
            ("wr_fee",                   _dec72),
            ("wr_return_ship_cost",      _dec72),
            ("wr_refunded_cash",         _dec72),
            ("wr_reversed_charge",       _dec72),
            ("wr_account_credit",        _dec72),
            ("wr_net_loss",              _dec72),
        ],
        row_count=lambda sf: sf * 71763,
        companion="web_sales",
    )

    web_sales = TableDef(
        name="web_sales",
        columns=[
            ("ws_sold_date_sk",          _int),
            ("ws_sold_time_sk",          _int),
            ("ws_ship_date_sk",          _int),
            ("ws_item_sk",               _int),
            ("ws_bill_customer_sk",      _int),
            ("ws_bill_cdemo_sk",         _int),
            ("ws_bill_hdemo_sk",         _int),
            ("ws_bill_addr_sk",          _int),
            ("ws_ship_customer_sk",      _int),
            ("ws_ship_cdemo_sk",         _int),
            ("ws_ship_hdemo_sk",         _int),
            ("ws_ship_addr_sk",          _int),
            ("ws_web_page_sk",           _int),
            ("ws_web_site_sk",           _int),
            ("ws_ship_mode_sk",          _int),
            ("ws_warehouse_sk",          _int),
            ("ws_promo_sk",              _int),
            ("ws_order_number",          _bigint),
            ("ws_quantity",              _int),
            ("ws_wholesale_cost",        _dec72),
            ("ws_list_price",            _dec72),
            ("ws_sales_price",           _dec72),
            ("ws_ext_discount_amt",      _dec72),
            ("ws_ext_sales_price",       _dec72),
            ("ws_ext_wholesale_cost",    _dec72),
            ("ws_ext_list_price",        _dec72),
            ("ws_ext_tax",               _dec72),
            ("ws_coupon_amt",            _dec72),
            ("ws_ext_ship_cost",         _dec72),
            ("ws_net_paid",              _dec72),
            ("ws_net_paid_inc_tax",      _dec72),
            ("ws_net_paid_inc_ship",     _dec72),
            ("ws_net_paid_inc_ship_tax", _dec72),
            ("ws_net_profit",            _dec72),
        ],
        row_count=lambda sf: sf * 719384,
        companion="web_returns",
    )

    catalog_sales = TableDef(
        name="catalog_sales",
        columns=[
            ("cs_sold_date_sk",          _int),
            ("cs_sold_time_sk",          _int),
            ("cs_ship_date_sk",          _int),
            ("cs_bill_customer_sk",      _int),
            ("cs_bill_cdemo_sk",         _int),
            ("cs_bill_hdemo_sk",         _int),
            ("cs_bill_addr_sk",          _int),
            ("cs_ship_customer_sk",      _int),
            ("cs_ship_cdemo_sk",         _int),
            ("cs_ship_hdemo_sk",         _int),
            ("cs_ship_addr_sk",          _int),
            ("cs_call_center_sk",        _int),
            ("cs_catalog_page_sk",       _int),
            ("cs_ship_mode_sk",          _int),
            ("cs_warehouse_sk",          _int),
            ("cs_item_sk",               _int),
            ("cs_promo_sk",              _int),
            ("cs_order_number",          _bigint),
            ("cs_quantity",              _int),
            ("cs_wholesale_cost",        _dec72),
            ("cs_list_price",            _dec72),
            ("cs_sales_price",           _dec72),
            ("cs_ext_discount_amt",      _dec72),
            ("cs_ext_sales_price",       _dec72),
            ("cs_ext_wholesale_cost",    _dec72),
            ("cs_ext_list_price",        _dec72),
            ("cs_ext_tax",               _dec72),
            ("cs_coupon_amt",            _dec72),
            ("cs_ext_ship_cost",         _dec72),
            ("cs_net_paid",              _dec72),
            ("cs_net_paid_inc_tax",      _dec72),
            ("cs_net_paid_inc_ship",     _dec72),
            ("cs_net_paid_inc_ship_tax", _dec72),
            ("cs_net_profit",            _dec72),
        ],
        row_count=lambda sf: sf * 1441548,
        companion="catalog_returns",
    )

    store_sales = TableDef(
        name="store_sales",
        columns=[
            ("ss_sold_date_sk",       _int),
            ("ss_sold_time_sk",       _int),
            ("ss_item_sk",            _int),
            ("ss_customer_sk",        _int),
            ("ss_cdemo_sk",           _int),
            ("ss_hdemo_sk",           _int),
            ("ss_addr_sk",            _int),
            ("ss_store_sk",           _int),
            ("ss_promo_sk",           _int),
            ("ss_ticket_number",      _bigint),
            ("ss_quantity",           _int),
            ("ss_wholesale_cost",     _dec72),
            ("ss_list_price",         _dec72),
            ("ss_sales_price",        _dec72),
            ("ss_ext_discount_amt",   _dec72),
            ("ss_ext_sales_price",    _dec72),
            ("ss_ext_wholesale_cost", _dec72),
            ("ss_ext_list_price",     _dec72),
            ("ss_ext_tax",            _dec72),
            ("ss_coupon_amt",         _dec72),
            ("ss_net_paid",           _dec72),
            ("ss_net_paid_inc_tax",   _dec72),
            ("ss_net_profit",         _dec72),
        ],
        row_count=lambda sf: sf * 2880404,
        companion="store_returns",
    )

    # Build and return the index
    all_tables = {
        t.name: t for t in [
            dbgen_version,
            customer_address,
            customer_demographics,
            date_dim,
            warehouse,
            ship_mode,
            time_dim,
            reason,
            income_band,
            item,
            store,
            call_center,
            customer,
            web_site,
            household_demographics,
            web_page,
            promotion,
            catalog_page,
            inventory,
            store_sales,
            store_returns,
            catalog_sales,
            catalog_returns,
            web_sales,
            web_returns,
        ]
    }  # type: Dict[str, TableDef]

    return all_tables


# ---------------------------------------------------------------------------
# Task planning — driver-safe (no pyarrow)
# ---------------------------------------------------------------------------

def plan_tasks(scale_factor, num_chunks):
    # type: (int, int) -> List[Dict]
    """Plan dsdgen tasks to be distributed across the cluster.

    Large tables are split into *num_chunks* chunks using dsdgen's
    ``-PARALLEL / -CHILD`` mechanism.  Small tables get a single task.

    Returns a list of task dicts, each with keys:
        table    – dsdgen table name (primary table for companion pairs)
        child    – 1-based chunk index
        parallel – total number of chunks (1 for small tables)
    """
    tasks = []   # type: List[Dict]
    planned = set()  # type: set

    for table_name in TABLE_ROW_COUNTS:
        if table_name in planned:
            continue

        primary = COMPANION_PRIMARY.get(table_name, table_name)
        companion = COMPANION_PAIRS.get(primary)
        planned.add(primary)
        if companion:
            planned.add(companion)

        row_count = TABLE_ROW_COUNTS[primary](scale_factor)

        if row_count < 1_000_000:
            tasks.append({
                "table":    primary,
                "child":    1,
                "parallel": 1,
            })
        else:
            max_parallel = max(1, row_count // 1_000_000)
            effective_parallel = min(num_chunks, max_parallel)
            for child in range(1, effective_parallel + 1):
                tasks.append({
                    "table":    primary,
                    "child":    child,
                    "parallel": effective_parallel,
                })

    return tasks


# ---------------------------------------------------------------------------
# Executor functions — pyarrow imported lazily inside
# ---------------------------------------------------------------------------

def _parse_time_str(s):
    # type: (Optional[str]) -> Optional[int]
    """Parse HH:MM:SS string to total seconds (int) for time32('s')."""
    if s is None:
        return None
    parts = s.split(":")
    return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])


def _streaming_dat_to_parquet(dat_path, table_def, out_file,
                               compression="snappy"):
    # type: (str, TableDef, str, str) -> int
    """Stream a dsdgen pipe-delimited .dat file to a local Parquet file.

    Uses ``pyarrow.csv.open_csv`` for streaming (constant memory).
    Always writes to a local file path.

    Returns the total number of rows written.
    """
    import pyarrow as pa
    import pyarrow.csv as pcsv
    import pyarrow.parquet as pq

    # dsdgen appends a trailing "|" on every line which creates an extra
    # empty column.  We absorb it with a dummy column name and then drop it
    # via ConvertOptions.include_columns.
    extra_col = "__trailing__"
    col_names = table_def.column_names + [extra_col]

    read_opts = pcsv.ReadOptions(
        column_names=col_names,
        autogenerate_column_names=False,
        block_size=1 << 24,  # 16 MB blocks
    )

    # invalid_row_handler was added in pyarrow >= 1.0; fall back gracefully.
    try:
        parse_opts = pcsv.ParseOptions(
            delimiter="|",
            invalid_row_handler=lambda row: "skip",
        )
    except TypeError:
        parse_opts = pcsv.ParseOptions(delimiter="|")

    # Time columns: read as string first, convert manually to seconds.
    has_time_cols = any(
        pa.types.is_time(dtype) for _, dtype in table_def.columns
    )
    csv_types = {}  # type: Dict[str, object]
    for col_name, dtype in table_def.columns:
        if pa.types.is_time(dtype):
            csv_types[col_name] = pa.string()
        else:
            csv_types[col_name] = dtype
    csv_types[extra_col] = pa.string()

    convert_opts = pcsv.ConvertOptions(
        column_types=csv_types,
        strings_can_be_null=True,
        null_values=[""],
        include_columns=table_def.column_names,  # drop extra_col
    )

    reader = pcsv.open_csv(
        dat_path,
        read_options=read_opts,
        parse_options=parse_opts,
        convert_options=convert_opts,
    )

    # Always write locally.
    os.makedirs(os.path.dirname(out_file), exist_ok=True)

    writer = None   # type: Optional[pq.ParquetWriter]
    total_rows = 0

    try:
        for batch in reader:
            if batch.num_rows == 0:
                continue

            if has_time_cols:
                # Convert string HH:MM:SS columns to time32('s') arrays.
                arrays = list(batch.columns)
                names = batch.schema.names
                for col_name, dtype in table_def.columns:
                    if pa.types.is_time(dtype) and col_name in names:
                        idx = names.index(col_name)
                        str_col = arrays[idx]
                        time_arr = pa.array(
                            [
                                _parse_time_str(v.as_py())
                                if v.is_valid else None
                                for v in str_col
                            ],
                            type=dtype,
                        )
                        arrays[idx] = time_arr
                batch = pa.RecordBatch.from_arrays(
                    arrays, schema=table_def.schema
                )
            else:
                batch = batch.cast(table_def.schema)

            if writer is None:
                writer = pq.ParquetWriter(
                    out_file,
                    schema=table_def.schema,
                    compression=compression,
                )

            writer.write_batch(batch)
            total_rows += batch.num_rows

    except pa.lib.ArrowInvalid:
        pass  # tolerate malformed trailing data at end of file

    finally:
        if writer is not None:
            writer.close()

    return total_rows


def _is_remote_path(path):
    # type: (str) -> bool
    """Check if path is a remote filesystem (HDFS, ABFS, S3, etc.)."""
    return "://" in path


def _hdfs_put(local_path, remote_path):
    # type: (str, str) -> None
    """Upload a local file to HDFS/ABFS using hdfs dfs -put."""
    # Ensure remote parent directory exists
    remote_dir = remote_path.rsplit("/", 1)[0]
    subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", remote_dir],
        capture_output=True, text=True,
    )
    proc = subprocess.run(
        ["hdfs", "dfs", "-put", "-f", local_path, remote_path],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            "hdfs dfs -put failed: " + proc.stderr
        )


def run_dsdgen_task(task, scale_factor, output_base, compression="snappy"):
    # type: (Dict, int, str, str) -> Dict
    """Run one dsdgen task on an executor node.

    Calls ``_build_all_tables()`` (which imports pyarrow) to resolve schemas,
    runs the dsdgen subprocess, then streams each generated ``.dat`` file
    to Parquet via ``_streaming_dat_to_parquet()``.

    For remote output paths (hdfs://, abfs://), writes Parquet locally first
    then uploads via ``hdfs dfs -put``.

    Returns a result dict::

        {
          "task":    <original task dict>,
          "results": {table_name: row_count, ...},
        }

    or on error::

        {"error": "<message>", "task": <original task dict>}
    """
    from pyspark import SparkFiles

    table_name = task["table"]
    child      = task["child"]
    parallel   = task["parallel"]

    # dsdgen binary was distributed via --files; make it executable.
    dsdgen_path = SparkFiles.get("dsdgen")
    os.chmod(dsdgen_path, 0o755)

    # Also make tpcds.idx available in the same directory
    try:
        idx_path = SparkFiles.get("tpcds.idx")
        dsdgen_dir = os.path.dirname(dsdgen_path)
        target_idx = os.path.join(dsdgen_dir, "tpcds.idx")
        if not os.path.exists(target_idx):
            import shutil
            shutil.copy2(idx_path, target_idx)
    except Exception:
        pass  # tpcds.idx might already be in the right place

    # Build all table schemas on this executor (lazy pyarrow import).
    all_tables = _build_all_tables()
    remote = _is_remote_path(output_base)

    # Use /mnt/tmp for temp files — /mnt is the large data disk (590GB)
    # vs root disk (124GB) which fills up with concurrent dsdgen tasks.
    mnt_tmp = "/mnt/tmp"
    try:
        os.makedirs(mnt_tmp, exist_ok=True)
    except OSError:
        mnt_tmp = None  # fall back to default /tmp

    with tempfile.TemporaryDirectory(
        prefix="tpcds_" + table_name + "_c" + str(child) + "_",
        dir=mnt_tmp,
    ) as tmp_dir:
        cmd = [
            dsdgen_path,
            "-SCALE",  str(scale_factor),
            "-TABLE",  table_name,
            "-DIR",    tmp_dir,
            "-FORCE",  "Y",
            "-QUIET",  "Y",
        ]
        if parallel > 1:
            cmd.extend(["-PARALLEL", str(parallel), "-CHILD", str(child)])

        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=os.path.dirname(dsdgen_path),
        )

        if proc.returncode != 0:
            return {
                "error": "dsdgen failed: " + proc.stderr,
                "task":  task,
            }

        results = {}  # type: Dict[str, int]
        dat_files = list(Path(tmp_dir).glob("*.dat"))

        for dat_file in dat_files:
            fname = dat_file.stem   # filename without extension
            tname = fname

            # Strip "_<child>_<parallel>" suffix from parallel output files.
            if parallel > 1:
                suffix = "_" + str(child) + "_" + str(parallel)
                if fname.endswith(suffix):
                    tname = fname[: -len(suffix)]
                else:
                    parts = fname.rsplit("_", 2)
                    if (
                        len(parts) >= 3
                        and parts[-1].isdigit()
                        and parts[-2].isdigit()
                    ):
                        tname = "_".join(parts[:-2])

            if tname not in all_tables:
                continue

            table_def = all_tables[tname]

            # Build parquet filename
            if parallel > 1:
                pq_name = "part-" + str(child).zfill(5) + ".parquet"
            else:
                pq_name = "part-00000.parquet"

            if remote:
                # Write locally, then upload
                local_pq = os.path.join(tmp_dir, tname + "_" + pq_name)
                rows = _streaming_dat_to_parquet(
                    str(dat_file), table_def, local_pq, compression,
                )
                if rows > 0:
                    remote_path = output_base + "/" + tname + "/" + pq_name
                    _hdfs_put(local_pq, remote_path)
            else:
                out_file = output_base + "/" + tname + "/" + pq_name
                rows = _streaming_dat_to_parquet(
                    str(dat_file), table_def, out_file, compression,
                )

            results[tname] = rows

    return {"task": task, "results": results}


# ---------------------------------------------------------------------------
# Driver entry point — no pyarrow imported here
# ---------------------------------------------------------------------------

def main():
    # type: () -> None
    parser = argparse.ArgumentParser(
        description="Distributed TPC-DS data generation via Spark + dsdgen"
    )
    parser.add_argument(
        "--scale", "-s", type=int, required=True,
        help="TPC-DS scale factor (GB)",
    )
    parser.add_argument(
        "--output", "-o", type=str, required=True,
        help="Base output path: local path, hdfs://, or abfs://",
    )
    parser.add_argument(
        "--chunks", type=int, default=0,
        help=(
            "Parallel chunks per large table "
            "(0 = auto: 10 * spark.executor.instances)"
        ),
    )
    parser.add_argument(
        "--compression", type=str, default="snappy",
        choices=["snappy", "gzip", "zstd", "none"],
        help="Parquet compression codec",
    )
    args = parser.parse_args()

    # pyspark import is safe on the driver; it does not require pyarrow.
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("TPC-DS Datagen SF=" + str(args.scale))
        .getOrCreate()
    )
    sc = spark.sparkContext

    num_executors = int(
        sc.getConf().get("spark.executor.instances", "10")
    )
    executor_cores = int(
        sc.getConf().get("spark.executor.cores", "8")
    )

    if args.chunks > 0:
        num_chunks = args.chunks
    else:
        # Auto-size chunks so each dsdgen temp .dat file stays under ~30GB.
        # At ~200 bytes/row, 30GB ≈ 150M rows per chunk.
        # With N concurrent tasks (executor_cores) per node, total temp ≈
        # N * 30GB which must fit in the node's temp disk (~295GB).
        max_rows_per_chunk = 150_000_000  # ~30GB .dat at 200 bytes/row
        biggest_table_rows = max(fn(args.scale) for fn in TABLE_ROW_COUNTS.values())
        chunks_for_disk = max(1, biggest_table_rows // max_rows_per_chunk)
        chunks_for_parallelism = num_executors * executor_cores
        num_chunks = max(chunks_for_disk, chunks_for_parallelism)

    print("=== TPC-DS SF={sf} Distributed Generation ===".format(
        sf=args.scale))
    print("Executors: {n}, Chunks per large table: {c}".format(
        n=num_executors, c=num_chunks))
    print("Output: " + args.output)

    start = time.time()

    tasks = plan_tasks(args.scale, num_chunks)
    print("Planned {n} tasks".format(n=len(tasks)))

    sf          = args.scale
    output      = args.output
    compression = args.compression

    tasks_rdd = sc.parallelize(tasks, numSlices=len(tasks))

    def process_partition(task_iter):
        for task in task_iter:
            result = run_dsdgen_task(task, sf, output, compression)
            yield result

    results = tasks_rdd.mapPartitions(process_partition).collect()

    elapsed = time.time() - start

    total_rows = 0
    table_rows = {}  # type: Dict[str, int]
    errors = []      # type: List[Dict]

    for r in results:
        if "error" in r:
            errors.append(r)
        else:
            for tname, rows in r.get("results", {}).items():
                table_rows[tname] = table_rows.get(tname, 0) + rows
                total_rows += rows

    print("\n=== Generation Complete ===")
    print("Time: {t:.1f}s ({m:.1f}m)".format(
        t=elapsed, m=elapsed / 60.0))
    print("Tables: " + str(len(table_rows)))
    print("Total rows: {:,}".format(total_rows))
    for tname, rows in sorted(table_rows.items()):
        print("  {t}: {r:,} rows".format(t=tname, r=rows))

    if errors:
        print("\nErrors ({n}):".format(n=len(errors)))
        for e in errors:
            print("  " + str(e))

    spark.stop()


if __name__ == "__main__":
    main()

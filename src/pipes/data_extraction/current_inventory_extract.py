from dataclasses import dataclass
from datetime import UTC, datetime

from pandas import DataFrame, read_sql

from common.config import CONNECTIONS
from common.database import Database
from common.enums.output_destination import OutputDestination
from common.flat_file import Column, FlatFile
from common.pipe import Pipe


@dataclass
class CurrentInventoryExtractParameters:
    email_recipients: str = ""
    s3_bucket: str = ""
    s3_key: str = (
        "data-exports/inventory/"
        f"year={datetime.now(UTC).strftime('%Y')}/"
        f"month={datetime.now(UTC).strftime('%m')}/"
        f"inventory_{datetime.now(UTC).strftime('%Y-%m-%d')}.csv"
    )
    category: str = "Inventory"
    max_date: str = datetime.now(UTC).date().strftime("%Y-%m-%d")


class CurrentInventoryExtract(Pipe):
    parameter_class = CurrentInventoryExtractParameters
    output_destination = OutputDestination.S3_EMAIL
    schema = FlatFile(
        Column("Entity"),
        Column("Stock ID", primary_key=True),
        Column("Product ID"),
        Column("PO ID"),
        Column("PO Item ID"),
        Column("Warehouse"),
        Column("Country"),
        Column("Location"),
        Column("Status"),
        Column("Type"),
        Column("Is Accessory"),
        Column("Purchase Channel"),
        Column("Stock Creation"),
        Column("Product Creation"),
        Column("Product Update"),
        Column("Last Verification"),
        Column("SKU"),
        Column("Brand"),
        Column("Model ID"),
        Column("Model"),
        Column("Dexterity"),
        Column("Condition"),
        Column("Condition Category"),
        Column("Shaft Material"),
        Column("Flex"),
        Column("Loft"),
        Column("Pieces"),
        Column("Loft Degree"),
        Column("Length"),
        Column("Length Difference"),
        Column("Is Available for Sale"),
        Column("Is VAT Margin Schema Eligible"),
        Column("Original Currency"),
        Column("Total Quantity"),
        Column("Available Quantity"),
        Column("Sold Quantity"),
        Column("Paid Quantity"),
        Column("Cost"),
        Column("Expected Resale Price"),
        Column("Regular Resale Price"),
        Column("Final Resale Price"),
        Column("Expected Resale Margin"),
    )

    @staticmethod
    def extract(_parameters: CurrentInventoryExtractParameters) -> dict[str, DataFrame]:

        dwh_current_inventory_query = """-- sql
            SELECT
                d_e.code AS "Entity",
                f_ic.stock_id AS "Stock ID",
                f_ic.product_id AS "Product ID",
                f_ic.purchase_order_id AS "PO ID",
                f_ic.purchase_order_item_id AS "PO Item ID",
                d_w.code AS "Warehouse",
                d_w.country_code AS "Country",
                f_ic.location_name AS "Location",
                d_ps.name AS "Status",
                d_pt.name AS "Type",
                IF(d_pt.is_golf_club, "No", "Yes") AS "Is Accessory",
                d_pch.name AS "Purchase Channel",
                f_ic.stock_creation_date AS "Stock Creation",
                f_ic.product_creation_date AS "Product Creation",
                f_ic.product_change_date AS "Product Update",
                f_ic.last_verification_date AS "Last Verification",
                f_ic.sku AS "SKU",
                COALESCE(d_b.name, 'N/A') AS "Brand",
                f_ic.model_id AS "Model ID",
                COALESCE(d_m.name, 'N/A') AS "Model",
                COALESCE(d_d.name, 'N/A') AS "Dexterity",
                COALESCE(d_pc.name, 'N/A') AS "Condition",
                CASE
                    WHEN COALESCE(d_p.condition_id, 0) = 0 THEN 'N/A'
                    WHEN d_p.condition_id = 1 THEN 'New'
                    ELSE 'Used'
                END AS "Condition Category",
                COALESCE(d_sm.name, 'N/A') AS "Shaft Material",
                COALESCE(d_sf.name, 'N/A') AS "Flex",
                COALESCE(d_sl.name, 'N/A') AS "Loft",
                COALESCE(d_p.club_count, 1) AS "Pieces",
                d_p.loft_degree AS "Loft Degree",
                d_p.length AS "Length",
                d_p.length_difference AS "Length Difference",
                IF(f_ic.is_available_for_sale, "Yes", "No") AS "Is Available for Sale",
                IF(f_ic.is_vat_margin_scheme_eligible, "Yes", "No") AS "Is VAT Margin Schema Eligible",
                f_ic.currency AS "Original Currency",
                f_ic.total_qty AS "Total Quantity",
                f_ic.available_qty AS "Available Quantity",
                f_ic.sold_qty AS "Sold Quantity",
                f_ic.paid_qty AS "Paid Quantity",
                ROUND(f_ic.fx_rate * f_ic.cost, 2) AS "Cost",
                CASE
                    WHEN f_ic.expected_resale_price <> 0 THEN ROUND(f_ic.fx_rate * f_ic.expected_resale_price, 2)
                    ELSE NULL
                END AS "Expected Resale Price",
                f_ic.regular_resale_price AS "Regular Resale Price",
                f_ic.final_resale_price AS "Final Resale Price",
                CASE
                    WHEN f_ic.cost > 0 AND f_ic.final_resale_price > 0 AND f_ic.fx_rate > 0
                    THEN ROUND((f_ic.final_resale_price - ROUND(f_ic.cost * f_ic.fx_rate, 2)) / f_ic.final_resale_price, 4)
                    ELSE NULL
                END AS "Expected Resale Margin"
            FROM
                fact_inventory_current f_ic
                INNER JOIN dim_warehouse d_w ON f_ic.warehouse_id = d_w.id
                INNER JOIN dim_product_status d_ps ON f_ic.product_status_id = d_ps.id
                INNER JOIN dim_product_type d_pt ON f_ic.product_type_id = d_pt.id
                LEFT JOIN dim_entity d_e ON f_ic.entity_id = d_e.id
                LEFT JOIN dim_product d_p ON f_ic.product_id = d_p.id
                LEFT JOIN dim_model d_m ON f_ic.model_id = d_m.id
                LEFT JOIN dim_brand d_b ON d_m.brand_id = d_b.id
                LEFT JOIN dim_dexterity d_d ON d_p.dexterity_id = d_d.id
                LEFT JOIN dim_standard_flex d_sf ON d_p.standard_flex_id = d_sf.id
                LEFT JOIN dim_standard_loft d_sl ON d_p.standard_loft_id = d_sl.id
                LEFT JOIN dim_shaft_material d_sm ON d_p.shaft_material_id = d_sm.id
                LEFT JOIN dim_product_condition d_pc ON d_p.condition_id = d_pc.id
                LEFT JOIN dim_purchase_channel d_pch ON f_ic.channel_id = d_pch.id
        """

        with Database(CONNECTIONS["data_warehouse"]) as db:
            return {
                "data": read_sql(
                    dwh_current_inventory_query,
                    db,
                ),
            }


handle = CurrentInventoryExtract()

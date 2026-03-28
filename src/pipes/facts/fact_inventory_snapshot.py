from dataclasses import dataclass
from datetime import UTC, datetime

from numpy import nan
from pandas import DataFrame, read_sql
from sqlalchemy import Column, Table
from sqlalchemy.dialects.mysql import DECIMAL, INTEGER, SMALLINT, TINYINT
from sqlalchemy.types import DateTime

from common.config import CONNECTIONS
from common.database import Database
from common.enums.loading_method import LoadingMethod
from common.enums.output_destination import OutputDestination
from common.pipe import Pipe


@dataclass
class FactInventorySnapshotParameters:
    pass


class FactInventorySnapshot(Pipe):
    parameter_class = FactInventorySnapshotParameters
    output_destination = OutputDestination.DATABASE
    connection = CONNECTIONS["data_warehouse"]
    loading_method = LoadingMethod.INSERT
    schema = Table(
        "fact_inventory",
        connection.metadata,
        Column("date_id", INTEGER(unsigned=True), primary_key=True),
        Column("product_id", INTEGER(unsigned=True), primary_key=True, index=True),
        Column("warehouse_id", TINYINT(unsigned=True), primary_key=True),
        Column("entity_id", TINYINT(unsigned=True), nullable=False),
        Column(
            "status_id", TINYINT(unsigned=True), index=True, nullable=False, default=0
        ),
        Column("quantity", SMALLINT(unsigned=True), nullable=False),
        Column(
            "avg_unit_cost",
            DECIMAL(precision=7, scale=2, unsigned=True),
            nullable=False,
            default=0,
            comment="Average unit cost in reporting currency",
        ),
        Column(
            "total_cost",
            DECIMAL(precision=10, scale=2, unsigned=True),
            nullable=False,
            default=0,
            comment="Total cost in reporting currency",
        ),
        Column(
            "unit_price",
            DECIMAL(precision=7, scale=2, unsigned=True),
            nullable=False,
            default=0,
            comment="Average unit price in reporting currency",
        ),
        Column(
            "total_value",
            DECIMAL(precision=10, scale=2, unsigned=True),
            nullable=False,
            default=0,
            comment="Total value in reporting currency",
        ),
        Column("stock_creation_date", DateTime),
    )

    @staticmethod
    def extract(_parameters: FactInventorySnapshotParameters) -> dict[str, DataFrame]:

        stock_query = """-- sql
            SELECT
                p.system_id AS "entity_id",
                p.warehouse_id AS "warehouse_id",
                p.id AS "product_id",
                p.status_id AS "status_id",
                MIN(s.created_at) AS "stock_creation_date",
                SUM(s.total_qty * s.cost) AS "total_cost",
                SUM(s.total_qty) AS "total_qty",
                SUM(s.available_qty) AS "available_qty"
            FROM
                stock_quantities s
                INNER JOIN products p ON p.id = s.product_id
            WHERE
                s.total_qty > 0
                AND s.deleted_at IS NULL
                AND p.status_id != 6 -- Out of Stock
            GROUP BY
                p.id,
                p.warehouse_id
            HAVING
                SUM(s.available_qty) > 0
        """

        product_prices_query = """-- sql
            SELECT
                pp.product_id AS "product_id",
                pp.entity_id AS "entity_id",
                pp.regular_price AS "unit_price"
            FROM
                product_prices pp
        """

        with Database(CONNECTIONS["erp_read"]) as erp_db:
            df_stock = read_sql(
                stock_query,
                erp_db,
            )
            df_product_prices = read_sql(
                product_prices_query,
                erp_db,
            )

        return {
            "stock": df_stock,
            "product_prices": df_product_prices,
        }

    @staticmethod
    def transform(
        data: dict[str, DataFrame], _parameters: FactInventorySnapshotParameters
    ) -> DataFrame:

        df_inventory = data["stock"].merge(
            data["product_prices"],
            on=["entity_id", "product_id"],
            how="left",
        )

        return df_inventory.assign(
            date_id=int(datetime.now(UTC).date().strftime("%Y%m%d")),
            avg_unit_cost=lambda df: (
                df["total_cost"].div(df["total_qty"].replace(0, nan)).round(2).fillna(0)
            ),
            total_cost=lambda df: (df["avg_unit_cost"] * df["available_qty"]).round(2),
            total_value=lambda df: (df["unit_price"] * df["available_qty"]).round(2),
        ).rename(columns={"available_qty": "quantity"})


handle = FactInventorySnapshot()

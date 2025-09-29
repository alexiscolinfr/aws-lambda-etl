from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict

from pandas import DataFrame, merge, read_sql

from common.config import CONNEXIONS
from common.database import Database
from common.enums.output_destination import OutputDestination
from common.flat_file import Column, FlatFile
from common.pipe import Pipe


@dataclass
class DWHToS3ExtractParameters:
    min_date: str = (
        (datetime.now(timezone.utc) - timedelta(days=7)).date().strftime("%Y-%m-%d")
    )
    max_date: str = datetime.now(timezone.utc).date().strftime("%Y-%m-%d")
    s3_bucket: str = "dwh-extracts"
    s3_key: str = (
        f"files/dwh_export_{datetime.now(timezone.utc).date().strftime('%Y-%m-%d')}.csv"
    )


class DWHToS3Extract(Pipe):
    parameter_class = DWHToS3ExtractParameters
    output_destination = OutputDestination.S3
    schema = FlatFile(
        Column("time", primary_key=True),
        Column("region_type", primary_key=True),
        Column("region", primary_key=True),
        Column("country", primary_key=True),
        Column("kpi_name", primary_key=True),
        Column("value", default=0),
    )

    @staticmethod
    def extract(
        parameters: DWHToS3ExtractParameters,
    ) -> Dict[str, DataFrame]:

        sales_query = """-- sql
            SELECT
                cs.entity_id AS "entity_id",
                cs.order_id AS "order_id",
                DATE(cs.order_date) AS "time",
                cs.country AS "country",
                cs.region_type AS "region_type",
                cs.region AS "region",
                cs.customer_type AS "customer_type",
                cs.net_subtotal AS "net_subtotal"
            FROM
                customer_sales cs
            WHERE
                DATE(cs.order_date) BETWEEN %(min_date)s AND %(max_date)s
        """

        purchases_query = """-- sql
           SELECT
                cp.entity_id AS "entity_id",
                cp.po_id AS "po_id",
                DATE(cp.order_date) AS "time",
                cp.country AS "country",
                cp.region_type AS "region_type",
                cp.region AS "region",
                cp.customer_type AS "customer_type",
                cp.total AS "total"
            FROM
                customer_purchases cp
            WHERE
                DATE(cp.order_date) BETWEEN %(min_date)s AND %(max_date)s
        """

        with Database(CONNEXIONS["dwh"]) as db:

            return {
                "sales": read_sql(
                    sales_query,
                    db,
                    params={
                        "min_date": parameters.min_date,
                        "max_date": parameters.max_date,
                    },
                ),
                "purchases": read_sql(
                    purchases_query,
                    db,
                    params={
                        "min_date": parameters.min_date,
                        "max_date": parameters.max_date,
                    },
                ),
            }

    @staticmethod
    def transform(
        data: Dict[str, DataFrame], parameters: DWHToS3ExtractParameters
    ) -> DataFrame:

        df_sales = data["sales"]
        df_purchases = data["purchases"]

        group_cols = ["time", "region_type", "region", "country"]

        merged_df = merge(df_sales, df_purchases, on=group_cols, how="outer")

        df = merged_df.melt(
            id_vars=group_cols,
            var_name="kpi_name",
            value_name="value",
        )

        df["value"] = df["value"].round(2)

        return df[df["value"].notna() & (df["value"] != 0)]


handle = DWHToS3Extract()

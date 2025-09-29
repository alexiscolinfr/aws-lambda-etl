from dataclasses import dataclass
from datetime import date
from typing import Dict

from pandas import DataFrame, Timedelta, date_range
from sqlalchemy import Column, Date, Table
from sqlalchemy.types import BigInteger, Integer, String

from common.config import CONNEXIONS
from common.enums.loading_method import LoadingMethod
from common.enums.output_destination import OutputDestination
from common.pipe import Pipe


@dataclass
class RPDDateParameters:
    start_date: str = date(2000, 1, 1).strftime("%Y-%m-%d")
    end_date: str = date(2099, 12, 31).strftime("%Y-%m-%d")


class RPDDate(Pipe):
    parameter_class = RPDDateParameters
    output_destination = OutputDestination.DATABASE
    connexion = CONNEXIONS["dwh"]
    loading_method = LoadingMethod.DROP_INSERT
    schema = Table(
        "dim_date",
        connexion.metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("date", Date, index=True, nullable=False),
        Column("day_num", Integer, nullable=False, index=True),
        Column("week_day_name", String(255), nullable=False),
        Column("week_day_num", Integer, nullable=False, index=True),
        Column("month_num", Integer, nullable=False, index=True),
        Column("month_name", String(255), nullable=False),
        Column("is_month_start", Integer, nullable=False),
        Column("is_month_end", Integer, nullable=False),
        Column("quarter_num", Integer, nullable=False, index=True),
        Column("year", Integer, nullable=False, index=True),
        Column("week_num", Integer, nullable=False, index=True),
        Column("start_unix", BigInteger, nullable=False, index=True),
        Column("end_unix", BigInteger, nullable=False, index=True),
    )

    @staticmethod
    def extract(parameters: RPDDateParameters) -> Dict[str, DataFrame]:

        dates_df = DataFrame(
            {"date": date_range(parameters.start_date, parameters.end_date)}
        )

        dates_df = dates_df.assign(
            id=dates_df.date.dt.strftime("%Y%m%d").astype(int),
            day_num=dates_df.date.dt.day,
            week_day_name=dates_df.date.dt.day_name(),
            week_day_num=dates_df.date.dt.dayofweek + 1,
            month_num=dates_df.date.dt.month,
            month_name=dates_df.date.dt.month_name(),
            is_month_start=dates_df.date.dt.is_month_start,
            is_month_end=dates_df.date.dt.is_month_end,
            quarter_num=dates_df.date.dt.quarter,
            year=dates_df.date.dt.year,
            week_num=dates_df.date.dt.isocalendar().week,
            start_unix=dates_df.date.dt.normalize().astype(int) // 10**9,
            end_unix=(
                dates_df.date.dt.normalize() + Timedelta(days=1) - Timedelta(seconds=1)
            ).astype(int)
            // 10**9,
        )

        return {
            "data": dates_df,
        }


handle = RPDDate()

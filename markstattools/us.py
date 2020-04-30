import os
import sys
from datetime import timedelta, date
from typing import Callable, Iterator
import pyarrow
import numpy as np
import pandas as pd
import pandas_read_xml as pdx
from pandas_read_xml import auto_separate_tables


link_base = 'https://bulkdata.uspto.gov/data/trademark/dailyxml/applications/'
root_key_list = ['trademark-applications-daily', 'application-information', 'file-segments', 'action-keys']


def daterange(start_date: date, end_date: date) -> Iterator:
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)


def get_historical_zip_file_path_list() -> list:
    zip_name_base = 'apc18840407-20191231'
    return [f"{link_base}{zip_name_base}-{str(i).rjust(2, '0')}.zip"
            for i in range(1,66)]


def get_daily_zip_file_path(for_date: str) -> str:
    zip_name_base = 'apc'
    return f"{link_base}{zip_name_base}{for_date.strftime('%y%m%d')}.zip"


def get_daily_zip_file_path_list() -> list:
    start_date = date(2020, 1, 1)
    end_date = date.today() - timedelta(days=1)
    return [get_daily_zip_file_path(_date)
            for _date in daterange(start_date, end_date)]


def convert_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        if 'date' in column:
            df[column] = pd.to_datetime(df[column], format='%Y%m%d', errors='coerce')
    return df


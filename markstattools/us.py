import os
import sys
from datetime import timedelta, date
from typing import Callable, Iterator
import pyarrow
import numpy as np
import pandas as pd
import pandas_read_xml as pdx
from pandas_read_xml import auto_separate_tables


save_path = './data/us'
if not os.path.exists(save_path):
    os.makedirs(save_path)


link_base = 'https://bulkdata.uspto.gov/data/trademark/dailyxml/applications/'
root_key_list = ['trademark-applications-daily', 'application-information', 'file-segments', 'action-keys']
key_columns = ['action-key', 'case-file|serial-number']


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


def convert_date_all_tables(data: dict) -> dict:
    for key in data:
        data[key] = data[key].pipe(convert_date_columns)
    return data


def save_all_tables(data: dict, folder_name: str) -> None:
    for key in data:
        data[key].to_parquet(f'{save_path}/{folder_name}/{key}.parquet', index=False)


def download_all_historical() -> None:
    historical_zip_files = get_historical_zip_file_path_list()
    for zip_file in historical_zip_files:
        zip_name = os.path.basename(zip_file).replace('.zip', '')
        if os.path.exists(f'{save_path}/{folder_name}'):
            print(f'skipping {zip_name}')
        else:
            os.makedirs(save_path)
            print(zip_name)
            data = (pdx.read_xml(zip_file, root_key_list)
                    .pipe(auto_separate_tables, key_columns)
                    .pipe(convert_date_all_tables))
            save_all_tables(data, zip_name)
            del data

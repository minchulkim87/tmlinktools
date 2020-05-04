import os
import sys
import glob
from datetime import timedelta, date
from typing import Callable, Iterator
import pyarrow
import numpy as np
import pandas as pd
import pandas_read_xml as pdx
from pandas_read_xml import auto_separate_tables


# This is where the downloaded files will save.
save_path = './downloads/us'
if not os.path.exists(save_path):
    os.makedirs(save_path)

# This is where the combined data will save.
data_path = './data/us'
if not os.path.exists(data_path):
    os.makedirs(data_path)

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


def download_all() -> None:
    historical_zip_files = get_historical_zip_file_path_list()
    daily_zip_files = get_daily_zip_file_path_list()
    all_zip_files = historical_zip_files + daily_zip_files
    for zip_file in all_zip_files:
        zip_name = os.path.basename(zip_file).replace('.zip', '')
        if not os.path.exists(f'{save_path}/{zip_name}'):
            try:
                print(f'Downloading: {zip_name}')
                data = (pdx.read_xml(zip_file, root_key_list)
                        .pipe(auto_separate_tables, key_columns))
                data = convert_date_all_tables(data)
                os.makedirs(f'{save_path}/{zip_name}')
                save_all_tables(data, zip_name)
                del data
            except:
                print(f'Failed to download: {zip_name}')
    print('Done')


def get_historical_download_folder_list() -> list:
    name_base = 'apc18840407-20191231'
    return [f"{save_path}{name_base}-{str(i).rjust(2, '0')}"
            for i in range(1,66)]


def get_daily_download_folder_list() -> list:
    name_base = 'apc'
    start_date = date(2020, 1, 1)
    end_date = date.today() - timedelta(days=1)
    return [f"{save_path}/{name_base}{_date.strftime('%y%m%d')}"
            for _date in daterange(start_date, end_date)]


def get_files_in_folder(folder:str, file_extension: str) -> list:
    return glob.glob(f"{folder}/*.{file_extension}")


def read_all_parquet_in_folder_as_dict(folder: str) -> dict:
    data = {}
    for parquet_file in get_files_in_folder(folder, 'parquet'):
        table_name = os.path.basename(parquet_file).replace('.parquet', '')
        data[table_name] = pd.read_parquet(parquet_file)
    return data

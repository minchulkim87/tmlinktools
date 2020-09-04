import os
import sys
import shutil
import glob
from dotenv import load_dotenv
from paramiko import Transport, SFTPClient
from typing import Callable, Iterator, Union
import json
import pyarrow
import numpy as np
import pandas as pd
import dask
import dask.dataframe as dd
import pandas_read_xml as pdx
from pandas_read_xml import flatten, fully_flatten


save_path = './downloads/gb'
backup_path = './backup/gb'
temp_path = './temp/gb'
data_path = './data/gb'
upload_folder_path = './upload/closed/gb'


"""
## Special note on UKIPO (gb) pipeline

The UKIPO data ingestion will be carried out quite differently from the others.
This is in part because you will need access to the SFTP where the data is available.
It is up to the user of this package to request access to UKIPO
and ensure the data will be ingested and stored within the security and privacy requirements set by UKIPO.

In the top level folder where you run the tmlinktools package create a ".env" file which contain two variables and their values.

```
UKIPO_USERNAME=
UKIPO_PASSWORD=
```

Search python-dotenv to read more about how this works.

Also, there are some manual downloading involved because the file sizes are large and SFTP connection is limited in speed and unstable.
The full download of the historical files can take weeks and there is no simple programmtical solution to ensure complete download.
(or at least any more efficiently than manual downloads)

In the folder you are running the tmlinktools, you should have "downloads" folder.
Create a subfolder "gb", and in it, create two folders "full" and "rollup".

```
tmlink/
    downloads/
        gb/
            full/
            rollup/
```

Download (manually) all files, in the sftp folder (licensee/full/), that start with "rep_export", into the "downloads/gb/full/" folder.
Download (manually) two files (rollup_old.zip and rollup.zip), in the sftp folder (licensee/full/), into the "downloads/gb/rollup" folder.

The "rep_export" files are the historical files, and the "rollup" files are the "deltas" that would update the data to the date of your download.
""" 


load_dotenv()

UKIPO_USERNAME = os.getenv('UKIPO_USERNAME')
UKIPO_PASSWORD = os.getenv('UKIPO_PASSWORD')

ftp_link = 'licensee3.ipo.gov.uk'

root_key_list = ['MarkLicenceeExportList', 'TradeMark']
key_columns_list = ['ApplicationNumber']


# -------------------------------------------------------------------------------------
# These functions will help download all the files and save them locally.
# -------------------------------------------------------------------------------------


def clean(df: pd.DataFrame) -> pd.DataFrame:
    temp = df.copy()
    for column in temp.columns:
        if 'Date' in column:
            temp[column] = pd.to_datetime(temp[column], errors='coerce')
        elif ('true' in temp[column].values) or ('false' in temp[column].values):
            temp[column] = temp[column].fillna(False).replace('false', False).replace('true', True)
    return temp


def clean_all_tables(data: dict) -> dict:
    return {table_name: clean(table_data)
            for table_name, table_data in data.items()}


def save_all_tables(data: dict, path: str, folder_name: str) -> None:
    if not os.path.exists(f'{path}/{folder_name}'):
        os.makedirs(f'{path}/{folder_name}')
    for key in data:
        data[key].to_parquet(f'{path}/{folder_name}/{key}.parquet', index=False)


# This function is the high-level wrapper for the download process.


# -------------------------------------------------------------------------------------
# These functions will help read the downloaded files and combine them.
# -------------------------------------------------------------------------------------

def get_files_in_folder(folder: str, file_extension: str) -> list:
    return glob.glob(f"{folder}/*.{file_extension}")


def get_subfolders(folder: str) -> list:
    return [f.name for f in os.scandir(folder) if f.is_dir()]


def removed_keys_from_one_dataframe_in_another(remove_from_df: pd.DataFrame,
                                               keys_in_df: dd.DataFrame) -> pd.DataFrame:
    return (remove_from_df
            .loc[~remove_from_df['ApplicationNumber'].isin(keys_in_df['ApplicationNumber'].unique()), :])


def removed_keys_from_one_dataframe_in_another_dask(remove_from_df: dd.DataFrame,
                                                    keys_in_df: dd.DataFrame) -> dd.DataFrame:
    return (remove_from_df
            .map_partitions(
                removed_keys_from_one_dataframe_in_another,
                keys_in_df=keys_in_df
            ))


def delete_then_append_dataframe(old_df: dd.DataFrame,
                                 new_df: dd.DataFrame) -> dd.DataFrame:
    return removed_keys_from_one_dataframe_in_another_dask(old_df, new_df).append(new_df)


def save(df: dd.DataFrame, path: str) -> None:
    if os.path.exists(path):
        shutil.rmtree(path)
    if df.npartitions >= 64:
        print('        Too many partitions. Repartitioning.')
        (df
        .map_partitions(clean)
        .repartition(partition_size='512MB')
        .to_parquet(path,
                    engine='pyarrow',
                    compression='snappy',
                    allow_truncated_timestamps=True))
    else:
        (df
        .map_partitions(clean)
        .to_parquet(path,
                    engine='pyarrow',
                    compression='snappy',
                    allow_truncated_timestamps=True))


# These functions make the individual updates happen in a "safer" way by saving to temp folder and replacing the old data only after success.


def backup() -> None:
    if os.path.exists(backup_path):
        shutil.rmtree(backup_path)
    shutil.copytree(data_path, backup_path)
    if os.path.exists(temp_path):
        shutil.rmtree(temp_path)
    shutil.copytree(data_path, temp_path)


def commit(update_version: str) -> None:
    shutil.rmtree(data_path)
    shutil.copytree(temp_path, data_path)
    write_latest_folder_name(update_version)
    shutil.rmtree(temp_path)


def rollback() -> None:
    shutil.rmtree(data_path)
    shutil.copytree(backup_path, data_path)
    shutil.rmtree(backup_path)


# This function is the high-level wrap for the merging process.


def update_file(file_path: str) -> None:
    table_name = os.path.basename(file_path).replace('.parquet', '')

    print(f'    {table_name}')
    temp_file_path = f'{temp_path}/{table_name}'
    target_file_path = f'{data_path}/{table_name}'
    if os.path.exists(target_file_path):
        (delete_then_append_dataframe(dd.read_parquet(target_file_path),
                                        dd.read_parquet(file_path).pipe(remove_unnecessary))
        .pipe(save, temp_file_path))
    else:
        (dd.read_parquet(file_path).pipe(remove_unnecessary)
        .pipe(save, temp_file_path))


# This is an extra function to combine the parquet files into one file each.


def make_each_table_as_single_file() -> None:
    tables = get_subfolders(data_path)
    for table in tables:
        print(f'    {table}')
        try:
            (dd.read_parquet(f'{data_path}/{table}')
             .compute()
             .to_parquet(f'{upload_folder_path}/{table}.parquet',
                         engine='pyarrow',
                         compression='snappy',
                         allow_truncated_timestamps=True,
                         index=False))
        except Exception as error:
            print('    Failed.')
            raise error


# -------------------------------------------------------------------------------------
# This function will automate everything.
# -------------------------------------------------------------------------------------


def update_all() -> None:
    download_all()
    update_version = get_next_folder_name()
    updated = False
    while update_version:
        print("Backing up.")
        backup()
        try:
            print(f"Merging in: {update_version}")
            parquet_files = get_files_in_folder(update_version, 'parquet')
            for parquet_file in parquet_files:
                update_file(parquet_file)
            print("Committing changes.")
            commit(update_version)
            update_version = get_next_folder_name()
            updated = True
        except:
            print("Failed. Rolling back.")
            rollback()
            updated = False
            update_version = None
    if updated:
        print('Preparing upload files')
        make_each_table_as_single_file()
    print("Done")


def initialise():
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    if not os.path.exists(temp_path):
        os.makedirs(temp_path)
    if not os.path.exists(upload_folder_path):
        os.makedirs(upload_folder_path)
    if not os.path.exists(data_path):
        os.makedirs(data_path)

if __name__ == '__main__':
    initialise()
    update_all()
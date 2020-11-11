import os
import sys
import shutil
import glob
from dotenv import load_dotenv
from paramiko import Transport, SFTPClient
from typing import Callable, Iterator, Union, List
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
            temp[column] = pd.to_datetime(temp[column], errors='coerce', utc=True)
        new_column_name = column.replace('MarkLicenceeExportList|', '').replace('@', '').replace('#', '')
        temp = temp.rename(columns={column: new_column_name})
    temp = temp.dropna(how='all')
    return temp


def clean_all_tables(data: dict) -> dict:
    return {table_name: clean(table_data)
            for table_name, table_data in data.items()}


def separate_and_save_tables(df: pd.DataFrame, key_columns_list: List[str], path: str, folder_name: str) -> None:
    if not os.path.exists(f'{path}/{folder_name}'):
        os.makedirs(f'{path}/{folder_name}')
    df = df.pipe(clean)
    for column in df.columns:
        if column.endswith('Details') & (column != 'MarkImageDetails'):
            temp = df[key_columns_list + [column]]
            df = df.drop(columns=column)
            (temp
             .pipe(fully_flatten)
             .pipe(clean)
             .to_parquet(f'{path}/{folder_name}/{column}.parquet', index=False))
            del temp
        elif column == 'MarkImageDetails':
            df = df.drop(columns=column)
    (df.pipe(fully_flatten)
     .pipe(clean)
     .to_parquet(f'{path}/{folder_name}/Application.parquet', index=False))
    del df

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


def commit(update_date: str) -> None:
    shutil.rmtree(data_path)
    shutil.copytree(temp_path, data_path)
    write_update_date(update_date)
    shutil.rmtree(temp_path)


def rollback() -> None:
    shutil.rmtree(data_path)
    shutil.copytree(backup_path, data_path)
    shutil.rmtree(backup_path)


def write_update_date(update_date: str) -> None:
    with open(f'{data_path}/updates.json', 'w') as jf:
        json.dump({'latest': update_date}, jf)


def get_update_date() -> str:
    if os.path.exists(f'{data_path}/updates.json'):
        with open(f'{data_path}/updates.json', 'r') as jf:
            latest = json.loads(jf.read())['latest']
    else:
        latest = 'start'
    return latest

# This function is the high-level wrap for the merging process.


def update_file(file_path: str) -> None:
    table_name = os.path.basename(file_path).replace('.parquet', '')

    print(f'    {table_name}')
    temp_file_path = f'{temp_path}/{table_name}'
    target_file_path = f'{data_path}/{table_name}'
    if os.path.exists(target_file_path):
        (delete_then_append_dataframe(dd.read_parquet(target_file_path),
                                      dd.read_parquet(file_path))
        .pipe(save, temp_file_path))
    else:
        (dd.read_parquet(file_path)
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


def initialise():
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    if not os.path.exists(save_path+'/new'):
        os.makedirs(save_path+'/new')
    if not os.path.exists(temp_path):
        os.makedirs(temp_path)
    if not os.path.exists(upload_folder_path):
        os.makedirs(upload_folder_path)
    if not os.path.exists(data_path):
        os.makedirs(data_path)


def extract_full_rollup():
    # Note that the XML structures for full, rolllup, and diff files are different.
    full_files = get_files_in_folder(save_path+'/full', 'zip')
    full_files.sort()
    
    for full_file in full_files:
        print(f'Converting {full_file}')
        separate_and_save_tables(
            (pdx.read_xml(full_file)
             [['MarkLicenceeExportList']]
             .pipe(flatten)),
            key_columns_list=key_columns_list,
            path=save_path+'/full',
            folder_name=os.path.basename(full_file).replace('.zip', '')
        )
    
    rollup_files = get_files_in_folder(save_path+'/rollup', 'zip')
    rollup_files.sort()

    for rollup_file in rollup_files:
        print(f'Converting {rollup_file}')
        separate_and_save_tables(
            (pdx.read_xml(rollup_file)
             [['MarkLicenceeExportList']]
             .pipe(flatten)),
            key_columns_list=key_columns_list,
            path=save_path+'/rollup',
            folder_name=os.path.basename(rollup_file).replace('.zip', '')
        )

    write_update_date('full_rollup')


def merge_full_rollup():
    print('Merging in full historical files')
    full_folders = get_subfolders(f'{save_path}/full')
    full_folders.sort()
    for full_folder in full_folders:
        print(f'    merging in {full_folder}')
        print("        Backing up.")
        backup()
        try:
            parquet_files = get_files_in_folder(f'{save_path}/full/{full_folder}', 'parquet')
            for parquet_file in parquet_files:
                update_file(parquet_file)
            print("        Committing changes.")
            commit('full_rollup')
        except:
            print("        Failed. Rolling back.")
            rollback()

    print(f'    merging in rollup_old')
    print("        Backing up.")
    backup()
    try:
        parquet_files = get_files_in_folder(f'{save_path}/rollup/rollup_old', 'parquet')
        for parquet_file in parquet_files:
            update_file(parquet_file)
        print("        Committing changes.")
        commit('full_rollup')
    except:
        print("        Failed. Rolling back.")
        rollback()
    
    print(f'    merging in rollup')
    print("        Backing up.")
    backup()
    try:
        parquet_files = get_files_in_folder(f'{save_path}/rollup/rollup', 'parquet')
        for parquet_file in parquet_files:
            update_file(parquet_file)
        print("        Committing changes.")
        commit('full_rollup')
    except:
        print("        Failed. Rolling back.")
        rollback()

    write_update_date('full_rollup_merged')


def download_new_rollup():
    print('Downloading the latest rollup file')
    save_file = save_path+'/new/rollup.zip'
    with Transport((ftp_link, 22)) as transport:
        transport.connect(username=UKIPO_USERNAME, password=UKIPO_PASSWORD)
        
        # performance boost... maybe...
        transport.window_size = 2097152
        transport.packetizer.REKEY_BYTES = pow(2, 40)
        transport.packetizer.REKEY_PACKETS = pow(2, 40)
        
        with SFTPClient.from_transport(transport) as sftp:
            sftp.chdir('./licensee/Full')
            sftp.get('./rollup.zip', save_file)
    print('Download complete')


def extract_new_rollup():
    rollup_file = save_path+'/new/rollup.zip'
    print(f'Converting {rollup_file}')
    separate_and_save_tables(
        (pdx.read_xml(rollup_file)
            [['MarkLicenceeExportList']]
            .pipe(flatten)),
        key_columns_list=key_columns_list,
        path=save_path+'/new',
        folder_name=os.path.basename(rollup_file).replace('.zip', '')
    )


def merge_new_rollup():
    print(f'    merging in rollup')
    print("        Backing up.")
    backup()
    try:
        parquet_files = get_files_in_folder(f'{save_path}/new/rollup', 'parquet')
        for parquet_file in parquet_files:
            update_file(parquet_file)
        print("        Committing changes.")
        commit('full_rollup')
    except:
        print("        Failed. Rolling back.")
        rollback()


if __name__ == '__main__':
    initialise()
    if get_update_date() == 'start':
        extract_full_rollup()
    if get_update_date() == 'full_rollup':
        merge_full_rollup()
    if get_update_date() == 'full_rollup_merged':
        download_new_rollup()
        extract_new_rollup()
        merge_new_rollup()
    print('Preparing upload files')
    make_each_table_as_single_file()
    print("Done")

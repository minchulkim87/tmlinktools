import os
import sys
import shutil
from ftplib import FTP
import requests
import glob
from typing import Callable, Iterator, Union
import json
import pyarrow
import numpy as np
import pandas as pd
import dask
import dask.dataframe as dd
import pandas_read_xml as pdx
from pandas_read_xml import flatten, fully_flatten


save_path = './downloads/em'
backup_path = './backup/em'
temp_path = './temp/em'
data_path = './data/em'
upload_folder_path = './upload/open/em'

ftp_link = 'ftp.euipo.europa.eu'
root_key_list = ['Transaction', 'TradeMarkTransactionBody', 'TransactionContentDetails', 'TransactionData']
key_column_dict = {
    'Trademark': 'ApplicationNumber',
    'InternationalRegistration': 'ApplicationNumber',
    'Applicant': 'ApplicantIdentifier'
}

# -------------------------------------------------------------------------------------
# These functions will help download all the files and save them locally.
# -------------------------------------------------------------------------------------


def clean_column_names(df: pd.DataFrame, table_name: str=None) -> pd.DataFrame:
    new_columns = []
    for column in df.columns:
        new = column
        new = new.replace('@', '')
        new = new.replace('#', '')
        new = new.replace('ClassDescriptionDetails|ClassDescription|', '')
        new = new.replace('BasicRecord|', '')
        if table_name:
            new = new.replace(table_name+'|', '', 1)
            if table_name.endswith('Details'):
                new = new.replace(table_name.replace('Details', '|'), '', 1)
        if new.endswith('|'):
            new = new[::-1].replace('|', '', 1)[::-1]
        new_columns.append(new)
    df.columns = new_columns
    return df


def clean_data_types(df: pd.DataFrame) -> pd.DataFrame:
    temp = df.copy()
    for column in temp.columns:
        if column.endswith('Date'):
            temp[column] = pd.to_datetime(temp[column], errors='coerce')
        elif column.endswith('Indicator'):
            temp[column] = temp[column].fillna(False).replace('false', False).replace('true', True)
    return temp


def remove_entirely_null_rows(df: pd.DataFrame, except_columns: list) -> pd.DataFrame:
    return df.dropna(how='all', subset=df.columns[~df.columns.isin(except_columns)])


def extract_sub_tree(df: pd.DataFrame, extract_column: str, key_columns: list) -> pd.DataFrame:
    return (df
            .loc[:, key_columns+[extract_column]].copy()
            .pipe(fully_flatten)
            .pipe(clean_column_names, extract_column)
            .pipe(remove_entirely_null_rows, except_columns=key_columns)
            .pipe(clean_data_types)
            .drop_duplicates())


def separate_tables(df: pd.DataFrame, main_table_name: str, key_columns: list) -> dict:
    data = {}
    data['delete'] = df.loc[df['operationCode']=='Delete', key_columns].copy()
    df = df.query('operationCode!="Delete"')
    for table in df.columns:
        if table.endswith('Details'):
            if table.endswith('GoodsServicesDetails'):
                GoodsServicesDetails = (df.loc[:, key_columns+[table]].copy()
                                        .pipe(fully_flatten)
                                        .pipe(clean_column_names, table))
                if 'ClassificationVersion' in GoodsServicesDetails.columns:
                    data[f"{main_table_name}.GoodsServices"] = (GoodsServicesDetails
                                                                .loc[:, key_columns + ['ClassNumber', 'ClassificationVersion']]
                                                                .copy()
                                                                .drop_duplicates())
                else:
                    data[f"{main_table_name}.GoodsServices"] = (GoodsServicesDetails
                                                                .loc[:, key_columns + ['ClassNumber']]
                                                                .copy()
                                                                .drop_duplicates())
                # The EUIPO captures the description in multiple languages for most applications.
                # Unfortunately, this makes the file sizes too large.
                # The descriptions will be filtered for English ones only.
                data[f"{main_table_name}.GoodsServices.Description"] = (GoodsServicesDetails
                                                                        .loc[GoodsServicesDetails['GoodsServicesDescription|languageCode']=='en',
                                                                            key_columns + ['GoodsServicesDescription|text', 'GoodsServicesDescription|languageCode']]
                                                                        .copy()
                                                                        .drop_duplicates())
                del GoodsServicesDetails
            else:
                data[main_table_name + '.' + table.replace('Details', '')] = extract_sub_tree(df, extract_column=table, key_columns=key_columns)
            df = df.drop(columns=table)
    data[main_table_name.replace('Details', '')] = (df
                                                    .pipe(fully_flatten)
                                                    .pipe(clean_column_names)
                                                    .pipe(remove_entirely_null_rows, except_columns=key_columns)
                                                    .pipe(clean_data_types)
                                                    .drop_duplicates())
    return data


def save_all_tables(data: dict, path: str, folder_name: str) -> None:
    if not os.path.exists(f'{path}/{folder_name}'):
        os.makedirs(f'{path}/{folder_name}')
    for key in data:
        data[key].to_parquet(f'{path}/{folder_name}/{key}.parquet', index=False)


def download_from_ftp(from_folder: str,
                      zip_starts_with: str,
                      root_key_list: list,
                      main_key: str,
                      main_table_name: str,
                      key_columns: list,
                      max_tries: int = 3) -> None:
    more_to_go = True
    tries = 1
    while more_to_go and tries <= max_tries:
        try:
            print(f'Trying to download from {from_folder}')
            with FTP(ftp_link) as ftp:
                ftp.login(user='opendata', passwd='kagar1n')
                ftp.cwd(from_folder)
                folder_list = ftp.nlst()
                folder_list.sort()
                for folder in folder_list:
                    ftp.cwd(folder)
                    zip_file_list = [zip_file for zip_file in ftp.nlst() if zip_file.startswith(zip_starts_with)]
                    if len(zip_file_list) > 0:
                        zip_file_list.sort()
                        for zip_file in zip_file_list:
                            zip_name = os.path.basename(zip_file).replace('.zip', '')
                            if not os.path.exists(f'{save_path}/{from_folder}/{folder}/{zip_name}'):
                                print(f'Downloading: {from_folder}/{folder}/{zip_name}')
                                with open(f'{save_path}/temp.zip', 'wb') as temp:
                                    ftp.retrbinary(f'RETR {zip_file}', temp.write)
                                save_all_tables(
                                    (pdx.read_xml(f'{save_path}/temp.zip', root_key_list)
                                        .loc[:, [main_key]]
                                        .pipe(flatten)
                                        .pipe(clean_column_names, main_key)
                                        .pipe(separate_tables, main_table_name=main_table_name, key_columns=key_columns)),
                                    path=f'{save_path}/{from_folder}/{folder}',
                                    folder_name=zip_name
                                )
                                print('    Downloaded.')
                            if (folder_list[-1] == folder) and (zip_file_list[-1] == zip_file):
                                more_to_go = False
                    ftp.cwd('..')
        except:
            tries = tries + 1
            print('Connection dropped.')
    if more_to_go:
        print('Max tries exceeded. Closed.')
    else:
        print('No more files to download.')


# The FTP server seems to have deleted the historical and some differential files for Applicants.
# I have downloaded these before and have made them available on my account. Until EUIPO repairs this, I will use my account to download.

def download_from_my_s3() -> None:
    base_url = 'https://s3.wasabisys.com/markstat-euipo/Applicant/Full'
    zip_file_list = [
        'APPLICANTS_20191022_0001.zip',
        'APPLICANTS_20191022_0002.zip',
        'APPLICANTS_20191022_0003.zip'
    ]
    folder = 'Applicant/Full/2019'
    main_key = 'ApplicantDetails'
    main_table_name = 'Applicant'
    for zip_file in zip_file_list:
        zip_name = os.path.basename(zip_file).replace('.zip', '')
        if not os.path.exists(f'{save_path}/{folder}/{zip_name}'):
            print(f'Downloading: {folder}/{zip_name}')
            #request.urlretrieve(f'{base_url}/{zip_file}', f'{save_path}/temp.zip')
            res = requests.get(f'{base_url}/{zip_file}', stream=True)
            if res.status_code == 200:
                with open(f'{save_path}/temp.zip', 'wb') as f:
                    shutil.copyfileobj(res.raw, f)
            save_all_tables(
                (pdx.read_xml(f'{save_path}/temp.zip', root_key_list)
                    .loc[:, [main_key]]
                    .pipe(flatten)
                    .pipe(clean_column_names, main_key)
                    .pipe(separate_tables, main_table_name=main_table_name, key_columns=['operationCode', 'ApplicantIdentifier'])),
                path=f'{save_path}/{folder}',
                folder_name=zip_name
            )
            print('    Downloaded.')


def download_all() -> None:
    if not os.path.exists(f'{save_path}/Trademark/Full/2019/EUTMS_20191023_0004'):
        download_from_ftp(
            from_folder='Trademark/Full',
            zip_starts_with='EUTMS',
            root_key_list=root_key_list,
            main_key='TradeMarkDetails',
            main_table_name='TradeMark',
            key_columns=['operationCode', 'ApplicationNumber']
        )

    if not os.path.exists(f'{save_path}/InternationalRegistration/Full/2019/IRS_20191026_0001'):
        download_from_ftp(
            from_folder='InternationalRegistration/Full',
            zip_starts_with='IRS',
            root_key_list=root_key_list,
            main_key='TradeMarkDetails',
            main_table_name='InternationalRegistration',
            key_columns=['operationCode', 'ApplicationNumber']
        )

    if not os.path.exists(f'{save_path}/Applicant/Full/2019/APPLICANTS_20191022_0001'):
        download_from_my_s3()

    download_from_ftp(
        from_folder='Trademark/Differential',
        zip_starts_with='DIFF_EUTMS',
        root_key_list=root_key_list,
        main_key='TradeMarkDetails',
        main_table_name='TradeMark',
        key_columns=['operationCode', 'ApplicationNumber']
    )

    download_from_ftp(
        from_folder='InternationalRegistration/Differential',
        zip_starts_with='DIFF_IRS',
        root_key_list=root_key_list,
        main_key='TradeMarkDetails',
        main_table_name='InternationalRegistration',
        key_columns=['operationCode', 'ApplicationNumber']
    )

    download_from_ftp(
        from_folder='Applicant/Differential',
        zip_starts_with='DIFF_APPLICANTS',
        root_key_list=root_key_list,
        main_key='ApplicantDetails',
        main_table_name='Applicant',
        key_columns=['operationCode', 'ApplicantIdentifier']
    )

    # Some of the zip files for the representatives seem corrupt. Attempting to process them are resulting in infinite loop of failure.
    # The representative dataset is not of critical value for now. So we will skip.
    """
    download_from_ftp(
        from_folder='Representative/Full',
        zip_starts_with='REPS',
        root_key_list=root_key_list,
        main_key='RepresentativeDetails',
        main_table_name='Representative',
        key_columns=['operationCode', 'RepresentativeIdentifier']
    )
    download_from_ftp(
        from_folder='Representative/Differential',
        zip_starts_with='DIFF_REPS',
        root_key_list=root_key_list,
        main_key='RepresentativeDetails',
        main_table_name='Representative',
        key_columns=['operationCode', 'RepresentativeIdentifier']
    )
    """


# -------------------------------------------------------------------------------------
# These functions will help read the downloaded files and combine them.
# -------------------------------------------------------------------------------------


def get_files_in_folder(folder: str, file_extension: str) -> list:
    return glob.glob(f"{folder}/*.{file_extension}")


def get_subfolders(folder: str) -> list:
    return [f.name for f in os.scandir(folder) if f.is_dir()]


def remove_unnecessary(df: dd.DataFrame) -> dd.DataFrame:
    return (df
            .loc[:, df.columns != 'operationCode']
            .drop_duplicates())


def removed_keys_from_one_dataframe_in_another(remove_from_df: pd.DataFrame,
                                               keys_in_df: Union[pd.DataFrame, dd.DataFrame],
                                               key_column: str) -> pd.DataFrame:
    return (remove_from_df
            .loc[~remove_from_df[key_column].isin(keys_in_df[key_column].unique()), :])


def removed_keys_from_one_dataframe_in_another_dask(remove_from_df: dd.DataFrame,
                                                    keys_in_df: dd.DataFrame,
                                                    key_column: str) -> dd.DataFrame:
    return (remove_from_df
            .map_partitions(
                removed_keys_from_one_dataframe_in_another,
                keys_in_df=keys_in_df,
                key_column=key_column
            ))


def delete_then_append_dataframe(old_df: dd.DataFrame,
                                 new_df: dd.DataFrame,
                                 deletes: pd.DataFrame,
                                 key_column: str) -> dd.DataFrame:
    return removed_keys_from_one_dataframe_in_another_dask(
        removed_keys_from_one_dataframe_in_another_dask(old_df, deletes, key_column),
        new_df,
        key_column=key_column
    ).append(new_df)


def save(df: dd.DataFrame, path: str) -> None:
    if os.path.exists(path):
        shutil.rmtree(path)
    if df.npartitions >= 32:
        print('        Too many partitions. Repartitioning.')
        (df
        .map_partitions(clean_data_types)
        .repartition(partition_size='512MB')
        .to_parquet(path,
                    engine='pyarrow',
                    compression='snappy',
                    use_deprecated_int96_timestamps=True,
                    allow_truncated_timestamps=True,
                    schema='infer'))
    else:
        (df
        .map_partitions(clean_data_types)
        .to_parquet(path,
                    engine='pyarrow',
                    compression='snappy',
                    use_deprecated_int96_timestamps=True,
                    allow_truncated_timestamps=True,
                    schema='infer'))


# These functions make the individual updates happen in a "safer" way by saving to temp folder and replacing the old data only after success.

def backup() -> None:
    if os.path.exists(backup_path):
        shutil.rmtree(backup_path)
    shutil.copytree(data_path, backup_path)
    if os.path.exists(temp_path):
        shutil.rmtree(temp_path)
    shutil.copytree(data_path, temp_path)


def commit(schema: str, update_version: str) -> None:
    shutil.rmtree(data_path)
    shutil.copytree(temp_path, data_path)
    write_latest_folder_name(schema, update_version)
    shutil.rmtree(temp_path)


def rollback() -> None:
    shutil.rmtree(data_path)
    shutil.copytree(backup_path, data_path)
    shutil.rmtree(backup_path)


def get_download_folder_list(schema: str) -> list:
    folder_list = []
    top_folders = ['Full', 'Differential']
    for top_folder in top_folders:
        mid_folders = get_subfolders(f'{save_path}/{schema}/{top_folder}')
        mid_folders.sort()
        for mid_folder in mid_folders:
            bottom_folders = get_subfolders(f'{save_path}/{schema}/{top_folder}/{mid_folder}')
            bottom_folders = [f'{save_path}/{schema}/{top_folder}/{mid_folder}/{bottom_folder}' for bottom_folder in bottom_folders]
            bottom_folders.sort()
            for bottom_folder in bottom_folders:
                folder_list.append(bottom_folder)
    return folder_list


def get_next_folder_name(schema: str) -> str:
    if os.path.exists(f'{data_path}/{schema}-updates.json'):
        with open(f'{data_path}/{schema}-updates.json', 'r') as jf:
            latest = json.loads(jf.read())['latest']
        download_folder_list = get_download_folder_list(schema=schema)
        index = download_folder_list.index(latest)
        if 0 <= index < len(download_folder_list) - 1:
            return download_folder_list[index+1]
        else:
            return None
    else:
        if schema == 'Trademark':
            return f'{save_path}/Trademark/Full/1996/EUTMS_20191023_0001'
        elif schema == 'InternationalRegistration':
            return f'{save_path}/InternationalRegistration/Full/2004/IRS_20191026_0001'
        elif schema == 'Applicant':
            return f'{save_path}/Applicant/Full/2019/APPLICANTS_20191022_0003'


def write_latest_folder_name(schema: str, update_version: str) -> None:
    with open(f'{data_path}/{schema}-updates.json', 'w') as jf:
        json.dump({'latest': update_version}, jf)


# This function is the high-level wrap for the merging process.


def update_file(file_path: str, deletes: pd.DataFrame, key_column: str) -> None:
    table_name = os.path.basename(file_path).replace('.parquet', '')
    print(f'    {table_name}')
    temp_file_path = f'{temp_path}/{table_name}'
    target_file_path = f'{data_path}/{table_name}'
    if os.path.exists(target_file_path):
        (delete_then_append_dataframe(dd.read_parquet(target_file_path),
                                      dd.read_parquet(file_path).pipe(remove_unnecessary),
                                      deletes,
                                      key_column=key_column)
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
             .pipe(clean_data_types)
             .to_parquet(f'{upload_folder_path}/{table}.parquet',
                         engine='pyarrow',
                         compression='snappy',
                         allow_truncated_timestamps=True,
                         use_deprecated_int96_timestamps=True,
                         index=False))
        except Exception as error:
            print('    Failed.')
            raise error


# -------------------------------------------------------------------------------------
# This function will automate everything.
# -------------------------------------------------------------------------------------


def update_all() -> None:
    download_all()
    for schema in ['Trademark', 'InternationalRegistration', 'Applicant']:
        if get_next_folder_name(schema):
            update_version = get_next_folder_name(schema)
        else:
            update_version = None
        updated = False
        while update_version:
            print("Backing up.")
            backup()
            try:
                print(f"Merging in: {update_version}")
                deletes = pd.read_parquet(f'{update_version}/delete.parquet')
                parquet_files = [parquet_file for parquet_file in get_files_in_folder(update_version, 'parquet')
                                 if 'delete' not in parquet_file]
                for parquet_file in parquet_files:
                    update_file(parquet_file, deletes, key_column=key_column_dict[schema])
                print("Committing changes.")
                commit(schema, update_version)
                update_version = get_next_folder_name(schema)
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
    if not os.path.exists(backup_path):
        os.makedirs(backup_path)
    if not os.path.exists(temp_path):
        os.makedirs(temp_path)
    if not os.path.exists(upload_folder_path):
        os.makedirs(upload_folder_path)
    if not os.path.exists(data_path):
        os.makedirs(data_path)


if __name__ == '__main__':
    initialise()
    update_all()

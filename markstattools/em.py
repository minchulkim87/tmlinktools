import os
import sys
import shutil
from ftplib import FTP
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
upload_folder_path = './upload/em'

ftp_link = 'ftp.euipo.europa.eu'
root_key_list = ['Transaction', 'TradeMarkTransactionBody', 'TransactionContentDetails', 'TransactionData']


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
            temp[column].fillna(False).replace('false', False).replace('true', True)
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


def download_from_ftp(from_folder: str, zip_starts_with: str, root_key_list: list, main_key: str, main_table_name: str, key_columns: list) -> None:
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
                    if not os.path.exists(f'{save_path}/{zip_name}'):
                        print(f'Downloading: {zip_name}')
                        with open(f'{save_path}/temp.zip', 'wb') as temp:
                            ftp.retrbinary(f'RETR {zip_file}', temp.write)
                        save_all_tables(
                            (pdx.read_xml(f'{save_path}/temp.zip', root_key_list)
                                .loc[:, [main_key]]
                                .pipe(flatten)
                                .pipe(clean_column_names, main_key)
                                .pipe(separate_tables, main_table_name=main_table_name, key_columns=key_columns)),
                            path=save_path,
                            folder_name=zip_name
                        )
            ftp.cwd('..')


def download_all() -> None:
    download_from_ftp(
        from_folder='Trademark/Full',
        zip_starts_with='EUTMS',
        root_key_list=root_key_list,
        main_key='TradeMarkDetails',
        main_table_name='TradeMark',
        key_columns=['operationCode', 'ApplicationNumber']
    )
    download_from_ftp(
        from_folder='Trademark/Differential',
        zip_starts_with='DIFF_EUTMS',
        root_key_list=root_key_list,
        main_key='TradeMarkDetails',
        main_table_name='TradeMark',
        key_columns=['operationCode', 'ApplicationNumber']
    )
    download_from_ftp(
        from_folder='InternationalRegistration/Full',
        zip_starts_with='IRS',
        root_key_list=root_key_list,
        main_key='TradeMarkDetails',
        main_table_name='InternationalRegistration',
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



# -------------------------------------------------------------------------------------
# These functions will help read the downloaded files and combine them.
# -------------------------------------------------------------------------------------



# These functions make the individual updates happen in a "safer" way by saving to temp folder and replacing the old data only after success.

def backup() -> None:
    if os.path.exists(backup_path):
        shutil.rmtree(backup_path)
    shutil.copytree(data_path, backup_path)


def commit(update_version: str) -> None:
    shutil.rmtree(data_path)
    shutil.copytree(temp_path, data_path)
    # write_latest_folder_name(update_version)
    shutil.rmtree(temp_path)


def rollback() -> None:
    shutil.rmtree(data_path)
    shutil.copytree(backup_path, data_path)
    shutil.rmtree(backup_path)


# -------------------------------------------------------------------------------------
# This function will automate everything.
# -------------------------------------------------------------------------------------


def update_all() -> None:
    download_all()
    print("Merging not implemented yet")
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
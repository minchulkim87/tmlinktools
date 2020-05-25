import os
import sys
import shutil
import glob
from typing import Callable, Iterator, Union
import json
import pyarrow
from datetime import timedelta, date
import numpy as np
import pandas as pd
import dask
import dask.dataframe as dd
import pandas_read_xml as pdx
from pandas_read_xml import normalise, flatten, fully_flatten


save_path = './downloads/ca'
backup_path = './backup/ca'
temp_path = './temp/ca'
data_path = './data/ca'
upload_folder_path = './upload/ca'

root_key_list = ['tmk:TrademarkApplication', 'tmk:TrademarkBag']
key_columns = ['operationCategory', 'ApplicationNumber']

historical_zip_url_base = 'https://opic-cipo.ca/cipo/client_downloads/Trademarks_Historical_2019_10/'
weekly_zip_url_base = 'https://opic-cipo.ca/cipo/client_downloads/Trademarks_Weekly/'

# -------------------------------------------------------------------------------------
# These functions will help download all the files and save them locally.
# -------------------------------------------------------------------------------------

def get_file_list_as_table() -> pd.DataFrame:
    html_page = 'https://www.ic.gc.ca/eic/site/cipointernet-internetopic.nsf/eng/h_wr04302.html'

    files_list = pd.read_html(html_page, displayed_only=False, encoding='utf-8')

    weekly_files = files_list[0].iloc[:, 0:2]
    weekly_files.columns = ['file_name', 'volume']
    weekly_files = (weekly_files.loc[:, ['volume', 'file_name']]
                    .sort_values('volume').reset_index(drop=True))

    historical_files = files_list[1].iloc[:, 0:2].copy()
    historical_files.columns = ['file_name', 'date_volume']
    historical_files['volume'] = historical_files['file_name'].str.replace('.zip', '').str[-3:]
    historical_files = (historical_files.loc[(~historical_files['file_name'].str.contains('index') & ~historical_files['file_name'].str.contains('Schema')),
                                            ['volume', 'file_name']]
                        .sort_values('volume').reset_index(drop=True))

    files = pd.concat([historical_files, weekly_files], ignore_index=False)
    return files


def clean_column_names(df: pd.DataFrame, table_name: str=None) -> pd.DataFrame:
    new_columns = []
    for column in df.columns:
        new = column
        new = new.replace('cacom:', '')
        new = new.replace('@com:', '')
        new = new.replace('com:', '')
        new = new.replace('catmk:', '')
        new = new.replace('tmk:', '')
        new = new.replace('@xsi:', '')
        new = new.replace('#', '')
        if table_name:
            new = new.replace(table_name+'|', '', 1)
            if table_name.endswith('Bag'):
                new = new.replace(table_name.replace('Bag', '|'), '', 1)
        if new.endswith('|'):
            new = new[::-1].replace('|', '', 1)[::-1]
        new_columns.append(new)
    df.columns = new_columns
    return df


def clean_data_types(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        if column.endswith('Date'):
            df[column] = pd.to_datetime(df[column], errors='coerce')
        if column.endswith('Indicator'):
            df[column] = df[column].fillna(False).replace('false', False).replace('true', True)
    return df


def remove_bad_columns(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        if column.endswith('ClassificationKindCode'):
            df = df.drop(columns=column)
        elif column.endswith('CommentText'):
            df = df.drop(columns=column)
        elif column == 'type':
            df = df.drop(columns=column)
        elif column == 'NationalMarkEvent|type':
            df = df.drop(columns=column)
        elif column == 'NationalMarkEvent|MarkEventAdditionalText':
            df = df.drop(columns=column)
        elif 'Format' in column:
            df = df.drop(columns=column)
        elif 'FileName' in column:
            df = df.drop(columns=column)
        elif column.endswith('|type'):
            df = df.drop(columns=column)
        elif column == 'NationalOppositionCaseType':
            df = df.drop(columns=column)
    return df


def rename_bad_columns(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        if column.startswith('MarkDescriptionBag|'):
            df = df.rename(columns={column: column.replace('MarkDescriptionBag|', '')})
        elif column.startswith('MarkReproduction|MarkImageBag|MarkImage|MarkImageClassification|FigurativeElementClassificationBag|ViennaClassificationBag|ViennaClassification|ViennaDescriptionBag|'):
            df = df.rename(columns={column: column.replace('MarkReproduction|MarkImageBag|MarkImage|MarkImageClassification|FigurativeElementClassificationBag|ViennaClassificationBag|ViennaClassification|ViennaDescriptionBag|', '')})
        elif column.startswith('MarkReproduction|MarkImageBag|MarkImage|MarkImageClassification|FigurativeElementClassificationBag|ViennaClassificationBag|'):
            df = df.rename(columns={column: column.replace('MarkReproduction|MarkImageBag|MarkImage|MarkImageClassification|FigurativeElementClassificationBag|ViennaClassificationBag|', '')})
        elif column.startswith('MarkReproduction|MarkImageBag|'):
            df = df.rename(columns={column: column.replace('MarkReproduction|MarkImageBag|', '')})
        elif column.startswith('MarkReproduction|MarkSoundBag|'):
            df = df.rename(columns={column: column.replace('MarkReproduction|MarkSoundBag|', '')})
        elif column.startswith('MarkReproduction|WordMarkSpecification|'):
            df = df.rename(columns={column: column.replace('MarkReproduction|WordMarkSpecification|', 'WordMarkSpecification|')})
    return df


def remove_entirely_null_rows(df: pd.DataFrame, except_columns: list=key_columns) -> pd.DataFrame:
    return df.dropna(how='all', subset=df.columns[~df.columns.isin(except_columns)])


def extract_sub_tree_partial(df: pd.DataFrame, extract_column: str, key_columns: list=key_columns, n_flattens: int=1) -> pd.DataFrame:
    df = df[key_columns+[extract_column]].copy()
    for i in range(n_flattens):
        df = df.pipe(flatten)
    df = (df
          .pipe(clean_column_names, extract_column)
          .pipe(remove_bad_columns)
          .pipe(rename_bad_columns))
    return df


def extract_sub_tree(df: pd.DataFrame, extract_column: str, key_columns: list=key_columns) -> pd.DataFrame:
    return (df
            .loc[:, key_columns+[extract_column]].copy()
            .pipe(fully_flatten)
            .pipe(clean_column_names, extract_column)
            .pipe(remove_bad_columns)
            .pipe(rename_bad_columns)
            .pipe(remove_entirely_null_rows)
            .pipe(clean_data_types)
            .drop_duplicates())


def finish_partial_tree(df: pd.DataFrame, clean_column_names_with_name: str) -> pd.DataFrame:
    return (df.copy()
            .pipe(fully_flatten)
            .pipe(clean_column_names, clean_column_names_with_name)
            .pipe(remove_bad_columns)
            .pipe(rename_bad_columns)
            .pipe(remove_entirely_null_rows)
            .pipe(clean_data_types)
            .drop_duplicates())


def separate_tables(df: pd.DataFrame) -> dict:
    data = {}
    tables = [
        'GoodsServicesBag',
        'LegalProceedingsBag',
        'MarkEventBag',
        'ApplicantBag',
        'PriorityBag',
        'PublicationBag',
        'MarkDisclaimerBag',
        'MarkRepresentation',
        'NationalRepresentative',
        'NationalCorrespondent',
        'Authorization',
        'UseRight',
        'UseLimitationText',
        'NationalTrademarkInformation'
    ]
    data['delete'] = df.loc[df['operationCategory']=='delete', key_columns].copy()
    df = df.query('operationCategory!="delete"')
    for table in tables:
        if table in df.columns:
            if table == 'GoodsServicesBag':
                goods_services = extract_sub_tree_partial(df, extract_column='GoodsServicesBag', key_columns=key_columns, n_flattens=2)
                for sub_tree in ['ClassDescriptionBag', 'GoodsServicesClassificationBag']:
                    if sub_tree in goods_services.columns:
                        data[f'GoodsServices.{sub_tree.replace("Bag", "")}'] = extract_sub_tree(goods_services, sub_tree)
                        goods_services = goods_services.drop(columns=sub_tree)
                data['GoodsServices'] = finish_partial_tree(goods_services, 'GoodsServicesBag')

            elif table == 'LegalProceedingsBag':
                LegalProceedingsBag = extract_sub_tree_partial(df, extract_column='LegalProceedingsBag', key_columns=key_columns, n_flattens=3)
                if 'OppositionProceedingBag' in LegalProceedingsBag.columns:
                    OppositionProceedingsBag = extract_sub_tree_partial(LegalProceedingsBag, extract_column='OppositionProceedingBag', key_columns=key_columns, n_flattens=1)
                    for sub_tree in ['ProceedingStageBag', 'DefendantBag', 'PlaintiffBag']:
                        if sub_tree in OppositionProceedingsBag.columns:
                            data[f'OppositionProceedings.{sub_tree.replace("Bag", "")}'] = extract_sub_tree(OppositionProceedingsBag, extract_column=sub_tree, key_columns=key_columns+['OppositionIdentifier'])
                            OppositionProceedingsBag = OppositionProceedingsBag.drop(columns=sub_tree)
                    data['OppositionProceedings'] = finish_partial_tree(OppositionProceedingsBag, 'OppositionProceedingsBag')
                if 'CancellationProceedings' in LegalProceedingsBag.columns:
                    CancellationProceedingsBag = extract_sub_tree_partial(LegalProceedingsBag, extract_column='CancellationProceedings', key_columns=key_columns, n_flattens=1)
                    for sub_tree in ['ProceedingStageBag', 'DefendantBag', 'PlaintiffBag']:
                        if sub_tree in CancellationProceedingsBag.columns:
                            data[f'CancellationProceedings.{sub_tree.replace("Bag", "")}'] = extract_sub_tree(CancellationProceedingsBag, extract_column=sub_tree, key_columns=key_columns+['LegalProceedingIdentifier'])
                            CancellationProceedingsBag = CancellationProceedingsBag.drop(columns=sub_tree)
                    data['CancellationProceedings'] = finish_partial_tree(OppositionProceedingsBag, 'CancellationProceedings')
                del LegalProceedingsBag

            elif table == 'MarkEventBag':
                MarkEventBag = extract_sub_tree_partial(df, extract_column='MarkEventBag', key_columns=key_columns, n_flattens=4)
                if 'NationalMarkEvent|MarkEventOtherLanguageDescriptionTextBag' in MarkEventBag.columns:
                    MarkEventBag = MarkEventBag.drop(columns='NationalMarkEvent|MarkEventOtherLanguageDescriptionTextBag')
                data['MarkEvent'] = finish_partial_tree(MarkEventBag, 'MarkEventBag')

            elif table == 'NationalTrademarkInformation':
                NationalTrademarkInformation = extract_sub_tree_partial(df, 'NationalTrademarkInformation', key_columns=key_columns, n_flattens=1)
                for sub_tree in ['CategorizedTextBag', 'ClaimBag', 'DoubtfulCaseBag', 'FootnoteBag', 'IndexHeadingBag', 'InterestedPartyBag', 'TrademarkClass', 'Legislation', 'Section9']:
                    if sub_tree in NationalTrademarkInformation.columns:
                        data[f'NationalTrademark.{sub_tree.replace("Bag", "")}'] = extract_sub_tree(NationalTrademarkInformation, sub_tree)
                        NationalTrademarkInformation = NationalTrademarkInformation.drop(columns=sub_tree)
                data['NationalTrademark'] = finish_partial_tree(NationalTrademarkInformation, clean_column_names_with_name='NationalTrademarkInformation')
            else:
                data[table.replace('Bag', '')] = extract_sub_tree(df, table)

            df = df.drop(columns=table)
    data['Trademark'] = finish_partial_tree(df, 'TrademarkBag')
    return data


def save_all_tables(data: dict, path: str, folder_name: str) -> None:
    if not os.path.exists(f'{path}/{folder_name}'):
        os.makedirs(f'{path}/{folder_name}')
    for key in data:
        data[key].to_parquet(f'{path}/{folder_name}/{key}.parquet', index=False)


def download_all() -> None:
    zip_files_table = get_file_list_as_table()
    for zip_file in zip_files_table['file_name']:
        zip_name = os.path.basename(zip_file).replace('.zip', '')
        if not os.path.exists(f'{save_path}/{zip_name}'):
            try:
                print(f'Downloading: {zip_name}')
                if 'WEEKLY' in zip_name:
                    url = weekly_zip_url_base + zip_file
                else:
                    url = historical_zip_url_base + zip_file
                save_all_tables(
                    (pdx.read_xml(url, root_key_list, transpose=True)
                        .pipe(normalise, 'com:ApplicationNumber')
                        .pipe(clean_column_names)
                        .rename(columns={'ApplicationNumber|ST13ApplicationNumber': 'ApplicationNumber'})
                        .pipe(separate_tables)),
                    save_path,
                    zip_name
                )
            except Exception as error:
                print(f'Failed to download: {zip_name}')
                raise error


# -------------------------------------------------------------------------------------
# These functions will help read the downloaded files and combine them.
# -------------------------------------------------------------------------------------


def get_files_in_folder(folder: str, file_extension: str) -> list:
    return glob.glob(f"{folder}/*.{file_extension}")


def get_subfolders(folder: str) -> list:
    return [f.name for f in os.scandir(folder) if f.is_dir()]


def remove_unnecessary(df: dd.DataFrame) -> dd.DataFrame:
    return (df
            .loc[:, df.columns != 'operationCategory']
            .drop_duplicates())


def removed_keys_from_one_dataframe_in_another(remove_from_df: pd.DataFrame,
                                               keys_in_df: Union[pd.DataFrame, dd.DataFrame]) -> pd.DataFrame:
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
                                 new_df: dd.DataFrame,
                                 deletes: pd.DataFrame) -> dd.DataFrame:
    return removed_keys_from_one_dataframe_in_another_dask(
        removed_keys_from_one_dataframe_in_another_dask(old_df, deletes),
        new_df).append(new_df)


def save(df: dd.DataFrame, path: str) -> None:
    (df
     .map_partitions(clean_data_types)
     .to_parquet(path,
                 engine='pyarrow',
                 compression='snappy',
                 allow_truncated_timestamps=True))


# These functions make the individual updates happen in a "safer" way by saving to temp folder and replacing the old data only after success.

def backup() -> None:
    if os.path.exists(backup_path):
        shutil.rmtree(backup_path)
    shutil.copytree(data_path, backup_path)


def commit(update_version: str) -> None:
    shutil.rmtree(data_path)
    shutil.copytree(temp_path, data_path)
    write_latest_folder_name(update_version)
    shutil.rmtree(temp_path)


def rollback() -> None:
    shutil.rmtree(data_path)
    shutil.copytree(backup_path, data_path)
    shutil.rmtree(backup_path)


def get_next_folder_name() -> str:
    if os.path.exists(f'{data_path}/updates.json'):
        with open(f'{data_path}/updates.json', 'r') as jf:
            latest = json.loads(jf.read())['latest']
        download_folder_list = [os.path.basename(download).replace('.zip', '') for download in get_file_list_as_table()['file_name']]
        index = download_folder_list.index(latest)
        if index < len(download_folder_list) - 1:
            return download_folder_list[index+1]
        else:
            return None
    else:
        return f'{save_path}/CA-TMK-GLOBAL_2019-10-05_111_168014_001'


def write_latest_folder_name(update_version: str) -> None:
    with open(f'{data_path}/updates.json', 'w') as jf:
        json.dump({'latest': update_version}, jf)


# This function is the high-level wrap for the merging process.

def update_file(file_path: str, deletes: pd.DataFrame) -> None:
    table_name = os.path.basename(file_path).replace('.parquet', '')
    print(f'    {table_name}')
    temp_file_path = f'{temp_path}/{table_name}'
    target_file_path = f'{data_path}/{table_name}'
    if os.path.exists(target_file_path):
        (delete_then_append_dataframe(dd.read_parquet(target_file_path),
                                      dd.read_parquet(file_path).pipe(remove_unnecessary),
                                      deletes)
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
            deletes = pd.read_parquet(f'{update_version}/delete.parquet')
            for parquet_file in get_files_in_folder(update_version, 'parquet'):
                if 'delete' not in parquet_file:
                    update_file(parquet_file, deletes)
            print("Committing changes.")
            commit(update_version)
            update_version = get_next_folder_name()
            updated = True
        except Exception as error:
            print("Failed. Rolling back.")
            rollback()
            update_version = None
            raise error
        print('Preparing upload files')
    if updated:
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

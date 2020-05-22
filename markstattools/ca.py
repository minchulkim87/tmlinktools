import os
import sys
import shutil
import glob
from typing import Callable, Iterator
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
key_columns = ['operationCategory', 'ApplicationNumber|ST13ApplicationNumber']

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
    return (df
            .pipe(fully_flatten)
            .pipe(clean_column_names, clean_column_names_with_name)
            .pipe(remove_bad_columns)
            .pipe(rename_bad_columns)
            .pipe(remove_entirely_null_rows)
            .pipe(clean_data_types)
            .drop_duplicates())


def separate_tables(df: pd.DataFrame) -> dict:
    data = {}
    data['delete'] = df.loc[df['operationCategory']=='delete', key_columns].copy()
    df = df.query('operationCategory!="delete"')

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

    for table in tables:
        if table in df.columns:
            print(f'    Extracting {table}')
            if table == 'GoodsServicesBag':
                goods_services = extract_sub_tree_partial(df, extract_column='GoodsServicesBag', key_columns=key_columns, n_flattens=2)
                for sub_tree in ['ClassDescriptionBag', 'GoodsServicesClassificationBag']:
                    data[f'GoodsServices.{sub_tree.replace("Bag", "")}'] = extract_sub_tree(goods_services, sub_tree)
                    goods_services = goods_services.drop(columns=sub_tree)
                data['GoodsServices'] = finish_partial_tree(goods_services, 'GoodsServicesBag')

            elif table == 'LegalProceedingsBag':
                LegalProceedingsBag = extract_sub_tree_partial(df, extract_column='LegalProceedingsBag', key_columns=key_columns, n_flattens=3)
                OppositionProceedingsBag = extract_sub_tree_partial(LegalProceedingsBag, extract_column='OppositionProceedingBag', key_columns=key_columns, n_flattens=1)
                CancellationProceedingsBag = extract_sub_tree_partial(LegalProceedingsBag, extract_column='CancellationProceedings', key_columns=key_columns, n_flattens=1)
                del LegalProceedingsBag
                for sub_tree in ['ProceedingStageBag', 'DefendantBag', 'PlaintiffBag']:
                    data[f'OppositionProceedings.{sub_tree.replace("Bag", "")}'] = extract_sub_tree(OppositionProceedingsBag, extract_column=sub_tree, key_columns=key_columns+['OppositionIdentifier'])
                    data[f'CancellationProceedings.{sub_tree.replace("Bag", "")}'] = extract_sub_tree(CancellationProceedingsBag, extract_column=sub_tree, key_columns=key_columns+['LegalProceedingIdentifier'])
                    OppositionProceedingsBag = OppositionProceedingsBag.drop(columns=sub_tree)
                    CancellationProceedingsBag = CancellationProceedingsBag.drop(columns=sub_tree)
                data['OppositionProceedings'] = finish_partial_tree(OppositionProceedingsBag, 'OppositionProceedingsBag')
                data['CancellationProceedings'] = finish_partial_tree(OppositionProceedingsBag, 'CancellationProceedings')

            elif table == 'MarkEventBag':
                MarkEventBag = extract_sub_tree_partial(df, extract_column='MarkEventBag', key_columns=key_columns, n_flattens=4)
                MarkEventBag = MarkEventBag.drop(columns='NationalMarkEvent|MarkEventOtherLanguageDescriptionTextBag')
                data['MarkEvent'] = finish_partial_tree(MarkEventBag, 'MarkEventBag')

            elif table == 'NationalTrademarkInformation':
                NationalTrademarkInformation = extract_sub_tree_partial(df, 'NationalTrademarkInformation', key_columns=key_columns, n_flattens=1)
                for sub_tree in ['CategorizedTextBag', 'ClaimBag', 'DoubtfulCaseBag', 'FootnoteBag', 'IndexHeadingBag', 'InterestedPartyBag', 'TrademarkClass', 'Legislation', 'Section9']:
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

# TODO

# These functions make the individual updates happen in a "safer" way by saving to temp folder and replacing the old data only after success.

def backup() -> None:
    if os.path.exists(backup_path):
        shutil.rmtree(backup_path)
    shutil.copytree(data_path, backup_path)


def commit() -> None:
    shutil.rmtree(data_path)
    shutil.copytree(temp_path, data_path)
    # TODO: updating currency of data information
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
    print('Merging not implemented yet.')
    print('Done')


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

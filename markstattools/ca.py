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
from pandas_read_xml import normalise, auto_separate_tables


save_path = './downloads/ca'
backup_path = './backup/ca'
temp_path = './temp/ca'
data_path = './data/ca'
upload_folder_path = './upload/ca'

root_key_list = ['tmk:TrademarkApplication', 'tmk:TrademarkBag']
common_keys = ['@com:operationCategory', 'com:ApplicationNumber|com:ST13ApplicationNumber']

"""
url = 'https://opic-cipo.ca/cipo/client_downloads/Trademarks_Weekly/WEEKLY_2019-06-25_00-13-46.zip'
data = (pdx.read_xml(zip_file, root_key_list, transpose=True)
        .pipe(normalise, 'com:ApplicationNumber')
        .pipe(auto_separate_tables, common_keys))
"""

# -------------------------------------------------------------------------------------
# These functions will help download all the files and save them locally.
# -------------------------------------------------------------------------------------

# TODO

# -------------------------------------------------------------------------------------
# These functions will help read the downloaded files and combine them.
# -------------------------------------------------------------------------------------

# TODO

# These functions make the individual updates happen in a "safer" way by saving to temp folder and replacing the old data only after success.

def backup() -> None:
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
    # TODO
    print('Not implemented yet.')


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

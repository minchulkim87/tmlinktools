import os
import sys
import shutil
from ftplib import FTP
#from urllib import request
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


save_path = './downloads/gb'
backup_path = './backup/gb'
temp_path = './temp/gb'
data_path = './data/gb'
upload_folder_path = './upload/closed/gb'

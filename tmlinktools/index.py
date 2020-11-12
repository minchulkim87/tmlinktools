#### FIX LATER

import os
import glob
import dask.dataframe as dd # dask's laziness helps as I only need the column names not any data.
from jinja2 import Template


upload_path = './upload'


def get_files_in_folder(folder: str, file_extension: str) -> list:
    return glob.glob(f"{folder}/*.{file_extension}")

folders = ['dataset', 'open/us', 'open/ca', 'open/em', 'closed/gb']

files = {}
columns = {}


for folder in folders:
    files_list = get_files_in_folder(folder=f'{upload_path}/{folder}', file_extension='parquet')
    files_list.sort()
    files[folder] = [os.path.basename(parquet_file) for parquet_file in files_list]
    columns[folder] = {}
    for parquet_file in files_list:
        columns[folder][os.path.basename(parquet_file)] = [column for column in dd.read_parquet(parquet_file,
                                                                                                engine='pyarrow').columns]


template = """
<!DOCTYPE html>
<html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>TM-Link</title>
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bulma@0.8.2/css/bulma.min.css">
    </head>
    <body>
        <section class="section">
            <div class="container">
                <h1 class="title is-1">TM-Link Data</h1>
            </div>
        </section>


        <section class="section">
            <div class="container">
                <h1 class="title is-2">Files</h1>
                <p>All links to the files begin with <code>https://s3.wasabisys.com/tmlink/</code></p>
                <p>Add the folder name and file name after this link base.</p>
                <br>
                <article class="message is-info">
                    <div class="message-header">
                        Example
                    </div>
                    <div class="message-body">
                        https://s3.wasabisys.com/tmlink/open/us/case-file-header.parquet
                    </div>
                </article>
            </div>
        </section>


        <section class="section">
            <div class="container">
                <table class="table is-bordered is-striped is-hoverable is-fullwidth">
                    <thead>
                        <th>Folder (IP Office Code)</th>
                        <th>File (Data Table)</th>
                    </thead>
                    <tbody>
                        {% for folder in folders %}
                            <tr>
                                <td>{{ folder }}</td>
                                <td>
                                    <ul>
                                        {% for parquet_file in files[folder] %}
                                            <li><a href="https://s3.wasabisys.com/tmlink/{{ folder }}/{{ parquet_file }}">{{ parquet_file }}</a></li>
                                        {% endfor %}
                                    </ul>
                                </td>
                            </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </section>


        <section class="section">
            <div class="container">
                <h1 class="subtitle is-3">Parquet</h1>
                <p>Due to the size of the dataset, the data is saved as parquet files.</p>
                <br>
                <p>Depending on what tools you use to work with data, you may need some packages.</p>
                <br>
                <article class="message is-info">
                    <div class="message-header">
                        Example: Python
                    </div>
                    <div class="message-body">
                        Install packages
                        <pre>pip install pyarrow<br>pip install pandas</pre>
                        <br>
                        Use pandas to read parquet
                        <pre>import pandas as pd<br>df = pd.read_parquet('https://s3.wasabisys.com/tmlink/trademarks.parquet')</pre>
                    </div>
                </article>
                <br>
                <article class="message is-info">
                    <div class="message-header">
                        Example: R
                    </div>
                    <div class="message-body">
                        Install packages
                        <pre>install.packages("arrow")</pre>
                        <br>
                        Use arrow to read parquet
                        <pre>library(arrow)<br>download.file('https://s3.wasabisys.com/tmlink/trademarks.parquet', 'trademarks.parquet')<br>df &#60;- read_parquet('trademarks.parquet')</pre>
                    </div>
                </article>
            </div>
        </section>


        <section class="section">
            <div class="container">
                <h1 class="title is-2">Columns</h1>
                <br>
                <article class="message is-warning">
                    <div class="message-header">
                        Duplicate Rows
                    </div>
                    <div class="message-body">
                        The data has been restructured to be "flat". When you select columns, be sure to drop the duplicates as some information can be repeated in the provided structure.
                    </div>
                </article>
                <br>
                <article class="message is-warning">
                    <div class="message-header">
                        Long Column Names
                    </div>
                    <div class="message-body">
                        Because the data had to be flattened out from a "tree" structure, the branch structures are reflected in column names. They can get quite long.
                    </div>
                </article>
            </div>
        </section>

        {% for folder in folders %}
            <section class="section">
                <div class="container">
                    <h1 class="title is-3">{{ folder }}</h1>
                    <br>
                    <table class="table is-bordered is-striped is-hoverable is-fullwidth">
                        <thead>
                            <th>File</th>
                            <th>Columns</th>
                        </thead>
                        <tbody>
                            {% for parquet_file in files[folder] %}
                                <tr>
                                    <td>{{ parquet_file }}</td>
                                    <td>
                                        <ul>
                                            {% for column in columns[folder][parquet_file] %}
                                                <li>{{ column }}</li>
                                            {% endfor %}
                                        </ul>
                                    </td>
                                </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
            </section>
        {% endfor %}

    </body>
</html>
"""


if __name__ == "__main__":
    (Template(template)
     .stream(folders=folders, files=files, columns=columns)
     .dump(f'{upload_path}/index.html'))

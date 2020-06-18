import os
import glob
import pandas as pd
from jinja2 import Template


upload_path = './upload'


def get_files_in_folder(folder: str, file_extension: str) -> list:
    return glob.glob(f"{folder}/*.{file_extension}")


def get_subfolders(folder: str) -> list:
    return [f.name for f in os.scandir(folder) if f.is_dir()]


folders = get_subfolders(upload_path)
files = {}
columns = {}


for folder in folders:
    files_list = get_files_in_folder(folder=f'{upload_path}/{folder}', file_extension='parquet')
    files[folder] = [os.path.basename(parquet_file) for parquet_file in files_list]
    columns[folder] = {}
    for parquet_file in files_list:
        columns[folder][os.path.basename(parquet_file)] = [column for column in pd.read_parquet(parquet_file).columns]


template = """
<!DOCTYPE html>
<html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>MARKSTAT</title>
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bulma@0.8.2/css/bulma.min.css">
    </head>
    <body>
        <section class="section">
            <div class="container">
                <h1 class="title is-1">MARKSTAT Data</h1>
            </div>
        </section>


        <section class="section">
            <div class="container">
                <h1 class="title is-2">Files</h1>
                <p>All links to the files begin with <code>https://s3.wasabisys.com/markstat/</code></p>
                <p>Add the folder name and file name after this link base.</p>
                <br>
                <article class="message is-info">
                    <div class="message-header">
                        Example
                    </div>
                    <div class="message-body">
                        https://s3.wasabisys.com/markstat/us/case-file-header.parquet
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
                        <tr>
                            <td></td>
                            <td>
                                <ul>
                                    <li><a href="https://s3.wasabisys.com/markstat/trademarks.parquet">trademarks.parquet</a></li>
                                </ul>
                            </td>
                        </tr>
                        {% for folder in folders %}
                            <tr>
                                <td>{{ folder }}</td>
                                <td>
                                    <ul>
                                        {% for parquet_file in files[folder] %}
                                            <li><a href="https://s3.wasabisys.com/markstat/{{ folder }}/{{ parquet_file }}">{{ parquet_file }}</a></li>
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
                        <pre>import pandas as pd<br>df = pd.read_parquet('https://s3.wasabisys.com/markstat/us/case-file-header.parquet')</pre>
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
                        <pre>library(arrow)<br>df &#60;- read_parquet('https://s3.wasabisys.com/markstat/us/case-file-header.parquet')</pre>
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


        <section class="section">
            <div class="container">
                <h1 class="title is-3">/</h1>
                <p>This data is the combined core set of data from all the IP offices</p>
                <br>
                <table class="table is-bordered is-striped is-hoverable is-fullwidth">
                    <thead>
                        <th>File</th>
                        <th>Columns</th>
                    </thead>
                    <tbody>
                        <tr>
                            <td>trademarks.parquet</td>
                            <td>
                                <ul>
                                    <li>ip_office</li>
                                    <li>application_number</li>
                                    <li>application_date</li>
                                    <li>registration_date</li>
                                </ul>
                            </td>
                        </tr>
                    </tbody>
                </table>
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

# markstattools

Developed for/by IP Australia for the MARKSTAT project - an international trade mark dataset.

## Background

We want to have access to intellecutal property data from around the world. Trade marks, being applied for registration by businesses and organisations around the world, has a potential to be a rich data source for research. Unlike patents, for which the international dataset PATSTAT exists, trade mark data only exists as summarised and aggregated counts. Details regarding the applications are typically only available through individual searches.

IP Australia is looking to build a dataset that combines trade mark data from around the world. But there are challenges. Often, the data are provided by the individual IP offices as XML files, each with a different set of schemas. Some have TXT or CSV files, and others provide JSON data via APIs. The data wrangling requires a lot of effort. Not only because of the varying file types, data formats, and delivery channels, but also because the file sizes are typically inaccessible through spreadsheets.

`markstattools` is a set of tools to help build the trade mark dataset by accessing open data, working through these challenges, and making the data ready (or close to ready) for use by researchers without the researchers having to deal with the data wrangling.

## Note

Note that this tool is still in development, and the primary users are the Data Engineer/Analyst in IP Australia. While it is possible for anybody to use this tool, it is recommended that you obtained the already built datasets that IP Australia provides, rather than trying to build your own by using this tool.

## How To

As this is a tool only used by one or two Data Analysts at this point, documentation is not available.

### Install

You will need Python (3.6 or newer). Install the pacakge using pip:

```bash
pip install markstattools
```

### Build

#### Structure

A quick note on how the files will be managed:

- To save storage space, data will be saved as parquet files. These files will often be partitioned for memory optimisation.
- You should use a single folder for all of the MARKSTAT data.
- Three subfolders will be created and used within your designated MARKSTAT folder:
    - downloads: This folder will be used to store downloaded files before merging them in. You should not delete these even after combining.
    - backup: This folder will be used to temporarily save current version files as a failsafe. This should automatically empty itself after each update.
    - temp: This folder will be used to temporarily save files as the datasets are being updated. This should automatically empty itself after each update.
    - data: This is where the final datasets will be stored.
    - upload: This is where the final datasets will be stored - but each table will be combined to one parquet file each.
- Subfolders will be created using the two-letter code representing the jurisdiction.
- Within each jurisdiction folder, each "table" will correspond to one parquet folder (which will contain the partitioned parquet files).

The first time you build any dataset, it will take a very long time to catch up.

#### US dataset

To build/update US data, navigate to your MARKSTAT folder within terminal and use the following command:

```bash
python -m markstattools.us
```

#### CA dataset

To build/update CA data, navigate to your MARKSTAT folder within terminal and use the following command:

```bash
python -m markstattools.ca
```

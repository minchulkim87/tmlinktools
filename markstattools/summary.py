import pandas as pd


upload_folder = './upload'
target_columns = ['ip_office', 'application_number', 'application_date', 'registration_date']

def main():
    us = (pd.read_parquet(f'{upload_folder}/us/case-file-header.parquet')
          .loc[:, ['serial-number', 'filing-date', 'registration-date']]
          .drop_duplicates()
          .assign(ip_office = 'us')
          .rename(columns={'serial-number': 'application_number',
                           'filing-date': 'application_date',
                           'registration-date': 'registration_date'})
          [target_columns])


if __name__ == '__main__':
    main()
import pandas as pd

upload_folder = './upload'
datset_folder = upload_folder + '/dataset'


def applications_table() -> pd.DataFrame:
    us = (pd.read_parquet(f'{upload_folder}/open/us/case-file-header.parquet')
          .rename(columns={
              'serial-number': 'application_number',
              'filing-date': 'application_date',
              'registration-date': 'registration_date'
          })
          .assign(ip_office = 'us')
          [['ip_office', 'application_number', 'application_date', 'registration_date']])

    ca = (pd.read_parquet(f'{upload_folder}/open/ca/case-file-header.parquet')
          .rename(columns={
              'ApplicationNumber': 'application_number',
              'ApplicationDate': 'application_date',
              'RegistrationDate': 'registration_date'
          })
          .assign(ip_office = 'ca')
          [['ip_office', 'application_number', 'application_date', 'registration_date']])

    em = (pd.read_parquet(f'{upload_folder}/open/em/case-file-header.parquet')
          .rename(columns={
              'ApplicationNumber': 'application_number',
              'ApplicationDate': 'application_date',
              'RegistrationDate': 'registration_date'
          })
          .assign(ip_office = 'em')
          [['ip_office', 'application_number', 'application_date', 'registration_date']])
    
    return pd.concat([us, ca, em], sort=False)


def main():
    print('Making the applications table')
    applications_table().to_parquet(f'{datset_folder}/applications.parquet', index=False)
    print('Done.')


if __name__ == '__main__':
    main()

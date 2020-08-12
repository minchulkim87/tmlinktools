import pandas as pd

upload_folder = './upload/dataset'
target_columns = ['ip_office', 'application_number', 'application_date', 'registration_date']
data_reference = {
    'us': {
        'file_names': ['case-file-header.parquet'],
        'columns': {
            'application_number': 'serial-number',
            'application_date': 'filing-date',
            'registration_date': 'registration-date'
        }
    },
    'ca': {
        'file_names': ['Trademark.parquet'],
        'columns': {
            'application_number': 'ApplicationNumber',
            'application_date': 'ApplicationDate',
            'registration_date': 'RegistrationDate'
        }
    },
    'em': {
        'file_names': ['TradeMark.parquet', 'InternationalRegistration.parquet'],
        'columns': {
            'application_number': 'ApplicationNumber',
            'application_date': 'ApplicationDate',
            'registration_date': 'RegistrationDate'
        }
    }
}


def get_trademarks(ip_office: str) -> pd.DataFrame:
    print(f'    {ip_office}')
    return (pd.concat([
        (pd.read_parquet(f'{upload_folder}/{ip_office}/{file_name}')
         .loc[:, [
             data_reference[ip_office]['columns']['application_number'],
             data_reference[ip_office]['columns']['application_date'],
             data_reference[ip_office]['columns']['registration_date']
         ]]
         .drop_duplicates()
         .assign(ip_office=ip_office)
         .rename(columns={
             data_reference[ip_office]['columns']['application_number']: 'application_number',
             data_reference[ip_office]['columns']['application_date']: 'application_date',
             data_reference[ip_office]['columns']['registration_date']: 'registration_date'
         })
         [target_columns])
        for file_name in data_reference[ip_office]['file_names']
    ]))


def main():
    print('Making the summary table')
    df = (pd.concat([get_trademarks(ip_office)[target_columns]
                     for ip_office in data_reference.keys()])
          [target_columns])
    print('Saving')
    df.to_parquet(f'{upload_folder}/trademarks.parquet', index=False)
    print('    Done.')


if __name__ == '__main__':
    main()

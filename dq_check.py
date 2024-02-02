#Data Quality Check
import pandas as pd
import glob
import os
from datetime import date

#146655907
#150325348
enh_loan_column_mapping = {
        'Investor Loan #': "int",
        'Scheduled Principal': "float",
        'Curtailments': "float",
        'Curtailment Adjustments': "float",
        'Prepayment': "float",
        'Liquidation Principal': "float",
        'Repurchase Principal': "float",
        'Principal Losses': "float",
        'Determination Date': "date",
    }
#columns_to_check = ["Investor Loan #","Scheduled Principal","Curtailments","Curtailment Adjustments","Prepayment",
#                    "Liquidation Principal","Repurchase Principal","Principal Losses","Determination Date"]
def check_missing_values(csv_file):
    df = pd.read_csv(csv_file)
    print('...Checking for missing values')
    # Check for missing values
    missing_values = df[enh_loan_column_mapping.keys()].isnull().sum()

    # Display the columns with missing values
    columns_with_missing = missing_values[missing_values > 0]
    if not columns_with_missing.empty:
        print("Columns with missing values:")
        print(columns_with_missing)
    else:
        print("No missing values found.")

def check_value_type(csv_file):
    print('...Checking for invalid data type')
    df = pd.read_csv(csv_file)
    invalid_columns = []
    for index, row in df.iterrows():
        for column, expected_type in enh_loan_column_mapping.items():
            value = row[column]
            current_type = type(value).__name__
            if column != "Determination Date":
                try:
                    df[column] = pd.to_numeric(value, downcast='float')
                except ValueError:
                    invalid_columns.append((index, column, value, current_type, expected_type))
            else:
                try:
                    df[column] = pd.to_datetime(value, errors='raise')
                except ValueError:
                    invalid_columns.append((index, column, value, current_type, expected_type))
    if invalid_columns:
        print("Columns with invalid data types:")
        for col_info in invalid_columns:
            print(f"Row: {col_info[0]}, Column: {col_info[1]}, Value: {col_info[2]}, Current Type: {col_info[3]}, Expected Type: {col_info[4]}")
    else:
        print("No columns with invalid data types found.")

def check_uniqueness(csv_file):
    print('...Checking uniqueness of Investor Loan #')
    df = pd.read_csv(csv_file)
    duplicated_values = set(df[df.duplicated(subset='Investor Loan #', keep=False)]['Investor Loan #'].tolist())
    if duplicated_values:
        print(f"The following values have been used more than once: {duplicated_values}")
    else:
        print('All values are unique.')


def main():
    csv_file_path = '/Users/pklzmr/Desktop/Xelur Case Study/xelure_case_study/enhanced_loan_level_data/*.csv'
    csv_files = glob.glob(csv_file_path)

    for csv_file in csv_files:
        print(f'File Name: {os.path.basename(csv_file)}')
        check_missing_values(csv_file)
        check_value_type(csv_file)
        check_uniqueness(csv_file)
        print('\n')

if __name__ == "__main__":
    main()
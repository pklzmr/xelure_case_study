import sqlite3 
import csv
import glob
import argparse
from datetime import datetime

enh_loan_column_mapping = {
        'Investor Loan #': 'investor_loan_num',
        'Scheduled Principal': 'scheduled_principal',
        'Curtailments': 'curtailments',
        'Curtailment Adjustments': 'curtailment_adjustments',
        'Prepayment': 'prepayment',
        'Liquidation Principal': 'liquidation_principal',
        'Repurchase Principal': 'repurchase_principal',
        'Principal Losses': 'principal_losses',
        'Determination Date': 'determination_date',
    }

principal_funds_column_mapping = {
    'Determination Date': 'determination_date',
    'Total Principal Funds': 'total_principal_funds'
}

def load_data():

    conn = sqlite3.connect('/Users/pklzmr/Desktop/Xelur Case Study/xelure_case_study/loan_details.db')
    cursor = conn.cursor()

    #Load enhanced loan level data 
    enh_loan_lvl_path = '/Users/pklzmr/Desktop/Xelur Case Study/xelure_case_study/enhanced_loan_level_data/*.csv'
    enh_loan_lvl_files = glob.glob(enh_loan_lvl_path)

    for enh_loan_lvl_file in enh_loan_lvl_files:
        print(f'Processing: {enh_loan_lvl_file}')

        with open(enh_loan_lvl_file, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)

                # Assuming the first row contains column names
            for row in csv_reader:
                mapped_values = {
                database_column: row.get(csv_column, '').lstrip('0') if csv_column == 'Investor Loan #' else row.get(csv_column, '')

                for csv_column, database_column in enh_loan_column_mapping.items()
            } 
                query = f'INSERT INTO enhanced_loan_level_data ({", ".join(mapped_values.keys())}) VALUES ({", ".join("?" * len(mapped_values))})'
                cursor.execute(query, list(mapped_values.values()))

    
    principal_funds_file = '/Users/pklzmr/Desktop/Xelur Case Study/xelure_case_study/TotalPrincipalFunds.csv'

    print(f'Processing: {principal_funds_file}')

    with open(principal_funds_file, 'r') as csv_file:
         csv_reader = csv.DictReader(csv_file)

         for row in csv_reader:
                mapped_values = {
                database_column: row.get(csv_column, '')

                for csv_column, database_column in principal_funds_column_mapping.items()
            } 
                query = f'INSERT INTO principal_funds ({", ".join(mapped_values.keys())}) VALUES ({", ".join("?" * len(mapped_values))})'
                cursor.execute(query, list(mapped_values.values()))

    conn.commit()
    conn.close()

def validate_all_dates():
    print('Validating all files')
    conn = sqlite3.connect('/Users/pklzmr/Desktop/Xelur Case Study/xelure_case_study/loan_details.db')
    cursor = conn.cursor()

    cursor.execute('SELECT DISTINCT(determination_date) FROM principal_funds')
    distinct_dates = cursor.fetchall()

    date_list = [date[0] for date in distinct_dates]
    for date in date_list:
        print(f'Processing Date: {date}')
        validate_principal_funds(date)
    
    conn.commit()
    conn.close()

def validate_principal_funds(date):
    try:
        date_obj = datetime.strptime(date, '%Y-%m-%d')
    
    except ValueError:
        raise argparse.ArgumentTypeError(f'Invalid date format. Please provide the date in the format YYYY-MM-DD.')

    conn = sqlite3.connect('/Users/pklzmr/Desktop/Xelur Case Study/xelure_case_study/loan_details.db')
    cursor = conn.cursor()

    cursor.execute('SELECT '
        'ROUND(SUM( '
            'scheduled_principal +'
            'curtailment_adjustments +'
            'curtailments +'
            'prepayment +'
            'liquidation_principal +'
            'repurchase_principal -'
            'principal_losses'
        '), 2) '
        'FROM '
            'enhanced_loan_level_data '
        'WHERE ' 
            f'determination_date = "{date}"')

    principal_fund_enh= cursor.fetchone()
    
    cursor.execute('SELECT total_principal_funds '
        'FROM '
            'principal_funds '
        'WHERE '
            f'determination_date = "{date}"')
    
    principal_fund = cursor.fetchone()

    if principal_fund_enh is None or principal_fund is None:
        raise ValueError("Data for date provided is not yet available.")

    if principal_fund[0] == principal_fund_enh[0]:
        print(f'Total Available Principal Funds validated : {principal_fund[0]}\n')
    else: 
        print(f'Total Available Principal Funds is not equal with Loan Level Data')
        print(f'Total Available Principal Funds: {principal_fund[0]}')
        print(f'Calculated Principal Funds: {principal_fund_enh[0]}\n')

    conn.commit()
    conn.close()
    
def main():
    parser = argparse.ArgumentParser(description='Sample script with command-line arguments')
    parser.add_argument('--load_data', action='store_true', help='Load data into the database')
    parser.add_argument('--validate_all_dates', action='store_true', help='Validating Principal Funds from all dates.')
    parser.add_argument('--validate_date', type=str, help='Validating Principal Funds from specified date.')

    args = parser.parse_args()
    
    if args.load_data:
        load_data()
        print('Data loaded successfully.')
    elif args.validate_all_dates:
        validate_all_dates()
        print('Finished Validating.')
    elif args.validate_date:
        validate_principal_funds(args.validate_date)
        print('Finished Validating.')
    else:
        validate_all_dates()
        print('Finished Validating.')

if __name__ == "__main__":
    main()
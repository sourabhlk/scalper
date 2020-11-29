import pandas
import os
import numpy
import datetime

FILES = {
    'Insider': {
        'file_prefix': 'CF-Insider',
        'name_on_disk': ''
    },
    'SAST Reg': {
        'file_prefix': 'CF-SAST- Reg',
        'name_on_disk': ''
    },

    'SAST Pledged': {
        'file_prefix': 'CF-SAST-Pledged',
        'name_on_disk': ''
    },
    'CF Shareholding': {
        'file_prefix': 'CF-Shareholding-',
        'name_on_disk': ''
    },
    'Name Lookup': {
        'file_prefix': 'EQUITY_L',
        'name_on_disk': ''
    },
    'bhav': {
        'file_prefix': 'sec_bhav',
        'name_on_disk': ''
    }
}


def main():
    check_for_files()
    insider_df = pandas.read_csv(FILES['Insider']['name_on_disk'])

    ''' GET THE INITIAL INSIDER DATA TO BE TRANSFORMED INTO MASTER DATAFRAME '''
    # Take the newlines out of each column's name in the insider_df
    insider_df.columns = [x.strip(' \n') for x in insider_df.columns]

    # Filter the insider dataframe to only include entries with a person as a Promoters or Promoter Group
    # Don't filter it in place at first because we need the other data in insider_df later
    working_df = insider_df[insider_df['CATEGORY OF PERSON'].isin(['Promoters', 'Promoter Group'])]
    working_df = working_df[working_df['MODE OF ACQUISITION'] == 'Market Purchase']

    # We only care about 2 columns in the working dataframe
    working_df = working_df[['SYMBOL', 'VALUE OF SECURITY (ACQUIRED/DISPLOSED)']]

    # Convert the Value of Security column to a float for summation below
    working_df['VALUE OF SECURITY (ACQUIRED/DISPLOSED)'] = working_df['VALUE OF SECURITY (ACQUIRED/DISPLOSED)'].astype(float)

    # Consolidate by the symbol, summing the values of the securities
    working_df['VALUE OF SECURITY (ACQUIRED/DISPLOSED)'] = working_df.groupby('SYMBOL')['VALUE OF SECURITY (ACQUIRED/DISPLOSED)'].transform('sum')
    working_df.drop_duplicates(subset='SYMBOL', inplace=True, ignore_index=True)

    ''' GET THE BUYING AND SELLING QUANTITIES AND AVERAGES COLUMNS '''
    bhav_df = pandas.read_csv(FILES['bhav']['name_on_disk'])

    # Make the date column in the working df the first unique date from the bhav file
    # In theory there should only be one date in the entire file, but grab the first unique one just in case
    system_date = datetime.datetime.strptime(bhav_df[' DATE1'].unique()[0].strip(' '), '%d-%b-%Y')
    working_df['DATE'] = system_date

    # Get the bhav symbol and close price to join with the working df
    bhav_df.drop_duplicates(subset='SYMBOL', inplace=True, ignore_index=True)
    bhav_df.columns = [x.strip(' ') for x in bhav_df.columns]
    bhav_df = bhav_df[['SYMBOL', 'CLOSE_PRICE']]
    working_df = pandas.merge(left=working_df, right=bhav_df, left_on='SYMBOL', right_on='SYMBOL', how='left')  # Left merge so that if any symbols are not found in the bhav file they are just N/A instead of dropped

    # Gather buying and selling quantities and averages
    working_df['BUYING QTY OF PROMOTERS/PROMOTER GROUP'] = working_df.apply(lambda row: get_value('purchase_qty', row['SYMBOL'], insider_df), axis=1)
    working_df['BUYING AVG PRICE OF PROMOTERS/PROMOTER GROUP'] = working_df.apply(lambda row: get_value('purchase_avg', row['SYMBOL'], insider_df), axis=1)
    working_df['SELLING QTY PRICE OF PROMOTERS/PROMOTER GROUP (MARKET SELL DATA)'] = working_df.apply(lambda row : get_value('selling_qty', row['SYMBOL'], insider_df), axis=1)
    working_df['SELLING AVG PRICE OF PROMOTERS/PROMOTER GROUP (MARKET SELL DATA)'] = working_df.apply(lambda row: get_value('selling_avg', row['SYMBOL'], insider_df), axis=1)

    ''' GATHER SHAREHOLDER DATA COLUMN '''
    shareholding_df = pandas.read_csv(FILES['CF Shareholding']['name_on_disk'])
    shareholding_df['COMPANY'] = shareholding_df['COMPANY'].str.lower()
    lookup_df = pandas.read_csv(FILES['Name Lookup']['name_on_disk'])
    working_df['SHAREHOLDING PATTERN OF PROMOTERS/PROMOTER GROUP'] = working_df.apply(lambda row: get_shareholder_value(row['SYMBOL'], shareholding_df, lookup_df), axis=1)

    ''' GATHER SAST REGULATIONS COLUMN '''
    sast_reg_df = pandas.read_csv(FILES['SAST Reg']['name_on_disk'])
    working_df['SAST REGULATIONS (SOLD QTY)'] = working_df.apply(lambda row: get_sast_reg_value(row['SYMBOL'], sast_reg_df), axis=1)

    ''' GATHER SAST PLEDGED COLUMN '''
    sast_pledge_df = pandas.read_csv(FILES['SAST Pledged']['name_on_disk'])
    sast_pledge_df['NAME OF COMPANY'] = sast_pledge_df['NAME OF COMPANY'].str.lower()
    working_df['PLEDGED DATA OF PROMOTER/PROMOTER GROUP'] = working_df.apply(lambda row: get_sast_pledge_value(row['SYMBOL'], sast_pledge_df, lookup_df), axis=1)

    ''' MARK SOME TO BE GREEN FILLED BASED ON COMPLEX FILTER '''
    working_df['SIGNAL'] = working_df.apply(lambda row: set_signal_column_value(row), axis=1)

    ## DEBUGGING CHECK
    ##print(working_df[working_df['SYMBOL'] == 'JSWSTEEL'].to_string())

    # Now we have the full dataframe with all the necessary columns
    # Convert the NaNs to dashes and output to Excel
    system_date = system_date.strftime('%d-%m-%Y')
    working_df = working_df.replace(numpy.nan, '-', regex=True)
    try:
        old_df = pandas.read_excel('swing_trading_output.xlsx')
        working_df = working_df.append(old_df)
    except FileNotFoundError:  # Will be thrown if this is the first run
        pass

    writer = pandas.ExcelWriter(f'swing_trading_output.xlsx', engine='xlsxwriter', date_format='dd-mmm-YYYY', datetime_format='dd-mmm-YYYY')
    working_df.to_excel(writer, index=False)
    writer.save()

    # Move all processed files to sorted directory
    if not os.path.isdir('sorted'):
        os.mkdir('sorted')

    for friendly_name in FILES:
        name_on_disk = FILES[friendly_name]['name_on_disk']

        # Delete the EQUITY_L file as it doesn't need to be retained
        if friendly_name == 'Name Lookup':
            os.remove(name_on_disk)
            continue

        os.rename(name_on_disk, os.path.join('sorted', f'{system_date}_{name_on_disk}'))



def set_signal_column_value(row):
    # Assigning these to variables so it doesn't get haywire with all these proper column names
    value = row['VALUE OF SECURITY (ACQUIRED/DISPLOSED)']
    selling_qty = row['SELLING QTY PRICE OF PROMOTERS/PROMOTER GROUP (MARKET SELL DATA)']
    shareholding = row['SHAREHOLDING PATTERN OF PROMOTERS/PROMOTER GROUP']
    sast_reg = row['SAST REGULATIONS (SOLD QTY)']
    pledged_data = row['PLEDGED DATA OF PROMOTER/PROMOTER GROUP']
    buying_avg = row['BUYING AVG PRICE OF PROMOTERS/PROMOTER GROUP']
    close_price = row['CLOSE_PRICE']

    # Actually doing the second filter first, since it's less likely to happen, thus reducing execution time
    if (buying_avg - (buying_avg * .5)) <= close_price <= (buying_avg + (buying_avg * .1)):
        if (value >= 10000000 and
                selling_qty == 0 and
                shareholding >= 50):
            if sast_reg == 0 and pledged_data == 0:
                return "PASSED"
            elif pandas.isnull(sast_reg) and pandas.isnull(pledged_data):
                return "PASSED/Kindly confirm SAST/PLEDGED data on website"
            elif pandas.isnull(sast_reg) and pledged_data == 0:
                return "PASSED/Kindly confirm SAST data on website"
            elif sast_reg == 0 and pandas.isnull(pledged_data):
                return "PASSED/Kindly confirm PLEDGED data on website"
        else:
            return ""
    else:
        return ""


# Called by lambda
def get_sast_pledge_value(symbol, sast_pledge_df, lookup_df):
    company = lookup_name(symbol, lookup_df)
    company_df = sast_pledge_df.loc[sast_pledge_df['NAME OF COMPANY'] == company]

    if len(company_df) == 0:
        return numpy.nan
    return company_df.iloc[0]['PROMOTER SHARES ENCUMBERED AS OF LAST QUARTER % OF PROMOTER SHARES (X/A)']


# Called by lambda
def get_sast_reg_value(symbol, sast_reg_df):
    symbol_df = sast_reg_df.loc[sast_reg_df['SYMBOL'] == symbol]

    if len(symbol_df) == 0:
        return numpy.nan

    # Sum the total sale column and return it
    return symbol_df.sum(axis=0, skipna=True)['TOTAL SALE (SHARES/VOTING RIGHTS/WARRANTS/ CONVERTIBLE SECURITIES/ANY OTHER INSTRUMENT)']


# Called by lambda
def get_shareholder_value(symbol, shareholding_df, lookup_df):
    # Need to look up company name from
    company = lookup_name(symbol, lookup_df)

    if type(company) == float:
        return numpy.nan

    try:
        return shareholding_df.loc[shareholding_df['COMPANY'] == company.lower()].iloc[0]['PROMOTER & PROMOTER GROUP (A)']
    except IndexError:  # Index error means there was no entry
        return numpy.nan


# Responsible for searching a company's proper name from their symbol
def lookup_name(symbol, lookup_df):
    company_df = lookup_df.loc[lookup_df['SYMBOL'] == symbol]

    # If the length is 0 then the company doesn't exist and nan will be placed in the shareholder spot
    if len(company_df) == 0:
        return numpy.nan
    return company_df.iloc[0]['NAME OF COMPANY'].lower()


# Called by lambda
def get_value(mode, symbol, insider_df):
    # Get the dataframe that represents purchasing and selling data for this particular symbol
    # We need this whether we're doing purchase or sales
    symbol_df = insider_df.loc[insider_df['SYMBOL'] == symbol]

    # Get the appropriate dataframe for the mode we're in
    if mode in ['selling_qty', 'selling_avg']:
        symbol_df = symbol_df[symbol_df['MODE OF ACQUISITION'] == 'Market Sale']
    elif mode in ['purchase_qty', 'purchase_avg']:
        symbol_df = symbol_df[symbol_df['MODE OF ACQUISITION'] == 'Market Purchase']

    # Convert the appropriate columns to floats for summation
    symbol_df['NO. OF SECURITIES (ACQUIRED/DISPLOSED)'] = symbol_df['NO. OF SECURITIES (ACQUIRED/DISPLOSED)'].astype(float)
    symbol_df['VALUE OF SECURITY (ACQUIRED/DISPLOSED)'] = symbol_df['VALUE OF SECURITY (ACQUIRED/DISPLOSED)'].astype(float)

    # Filter only for promoters and promoter group
    symbol_df = symbol_df[symbol_df['CATEGORY OF PERSON'].isin(['Promoters', 'Promoter Group'])]

    # If we just want the quantity, return the quantity
    if mode.find('qty') >= 0:
        return symbol_df.sum(axis=0, skipna=True)['NO. OF SECURITIES (ACQUIRED/DISPLOSED)']
    # If we want the average, calc the average and return it
    elif mode.find('avg') >= 0:
        return symbol_df.sum(axis=0, skipna=True)['VALUE OF SECURITY (ACQUIRED/DISPLOSED)'] / symbol_df.sum(axis=0, skipna=True)['NO. OF SECURITIES (ACQUIRED/DISPLOSED)']


# Responsible for checking the working directory for all the necessary files needed to run the process
# When file is found, the name_on_disk attribute in the FILES dictionary is filled in
def check_for_files():
    existing_files = os.listdir('./Files')
    found = False

    # Iterate through the start of each file name that we expect to see and check if the file exists in the current directory
    for file in FILES:
        for existing_file in existing_files:
            prefix = FILES[file]['file_prefix']
            if existing_file.startswith(prefix):
                found = True
                FILES[file]['name_on_disk'] = './Files/'+existing_file
                break
        if found is True:  # If the file is ever found, move onto the next file_pre
            found = False
            continue
        else:  # If the file is never found, raise an environment error and stop
            raise EnvironmentError(f'Cannot find file starting with "{prefix}". Please put the file in the working directory and run the script again.')


if __name__ == '__main__':
    main()

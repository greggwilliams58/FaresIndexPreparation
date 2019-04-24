from rdg_prices_info import get_rdg_prices_info, addRDGfaresinfo
from lennon_data import add_lennon_fares_info, get_lennon_price_info
#from commonfunctions import applydatatypes, exportfile
#from export_data import exportfile
from commonfunctions import handlezeroandnulls,percentagechange,applydatatypes, exportfile

import pandas as pd
import numpy as np


def non_advanced_data(df,destinationpath,RDGfarespath,LENNONfarespath):
    """
    This function takes the combined file as a data frame, adds prepared RDG price data, adds LENNON price data which is used to fill gaps in RDG data.
    The combined file then moves rows where there is no prices information for both years.
    'earnings column' is renamed weightings
    NULL and zero information is removed before a percentage change calculation is made.
    Rows where percentage changes are less than -20% and weightings  < £500,000 are extracted from combined file into a separate data frame, which is exported as 'little changes' for manual data validation
    Rows where percentage changes are more than 20% and weightings  < £500,000 are extracted from combined file into a separate data frame, which is exported as 'big changes' for manual validation
    Rows where weightings are greater than £500,000 are extracted from superfile, which is exported as 'big earnings' for avantix data to be added.
    The remaining rows of populated file are exported as populated data
    
    Parameter:
    df                  - A dataframe containing a combined file of TOC information with dimension information
    destinationfilepath - A string containing the location where output should be sent.
    RDGfarespath        - A string containing the location of the RDG lookup information
    LENNONfarespath     - A string containing the location of the LENNON lookup information

    Returns
    N/A
    exports a file of "little changes" for manual data validation
    exports a file of "big changes for manual data validation
    exports a file of "big earners" for manual addition of avantix data
    exports a file of populated data
    """

    #remove all advanced fares
    df = df[df['Category']!='advance']


    RDGprices2018 = get_rdg_prices_info(RDGfarespath
                        ,'2018 fares extract.txt'
                        , destinationpath
                        ,'prices2018.csv'
                        ,'2018'
                        ,True)

    RDGprices2019 = get_rdg_prices_info(RDGfarespath
                        ,'2019 fares extract.txt'
                        , destinationpath
                        ,'prices2019.csv'
                        ,'2019'
                        ,True)

    print("about to merge RDG info into main dataset.")

    #merging RDG fares information
    df = addRDGfaresinfo(df, RDGprices2018,'_2018')
    df = addRDGfaresinfo(df, RDGprices2019,'_2019')

    exportfile(df,destinationpath,'non_advanced_data_after_RDG')

    print("datatyping of key columns")
    #datatyping
    df['Origin Code'] = df['Origin Code'].str.zfill(4)
    df['Destination Code'] = df['Destination Code'].str.zfill(4)
    df['Route Code'] = df['Route Code'].str.zfill(5)

    #print("datatyping of other columns")
    #df = applydatatypes(df,['Regulated_Status'])
    
    
    #converting RDG fares to numeric 
    print("convert rdg fares to numeric")
    df[['RDG_FARES_2018','RDG_FARES_2019']] = df[['RDG_FARES_2018','RDG_FARES_2019']].apply(pd.to_numeric)
    
    exportfile(df,destinationpath,"non-advanced_data_before_LENNON")
    #getting LENNON fare information
    print("getting fares information from lennon")
    LENNONprices2018 = get_lennon_price_info('2018',LENNONfarespath,'pricefile_nonadvanced_2018.csv','non-advanced')
    LENNONprices2019 = get_lennon_price_info('2019',LENNONfarespath,'pricefile_nonadvanced_2019.csv','non-advanced')

    #merging LENNON fares information
    print("merging LENNON fares information with superfile")
    df = add_lennon_fares_info(df,LENNONprices2018,'_2018','non-advanced')
    df = add_lennon_fares_info(df,LENNONprices2019,'_2019','non-advanced')


    #replace empty RDG data with LENNON data
    df['RDG_FARES_2018'].fillna(df['LENNON_PRICE_2018'],inplace=True)
    df['RDG_FARES_2019'].fillna(df['LENNON_PRICE_2019'],inplace=True)

    #drop unnecessary columns and apply categorical datatyping
    #print("Dropping columns - line 71")
    df = datatypinganddropping(df)
    
    # rename the RDG Fares Columns, Earnings and axis
    df.rename(columns={'RDG_FARES_2018':'FARES_2018','RDG_FARES_2019':'FARES_2019','Adjusted Earnings Amount':'Weightings'},inplace=True)
    df.rename_axis('index')

    #export of full file
    exportfile(df,destinationpath,'superfile')
    bigearners = df.query('Weightings > 500000') 
        
    #drop rows where FARES are NaN or 0
    populated2018and2019 = handlezeroandnulls(df)

    #add percentagechange to populated file
    populated2018and2019 = percentagechange(populated2018and2019,'FARES_2018','FARES_2019')

    #this is for validation of large percentage changes' amended data to be added to coredata later
    bigchange = populated2018and2019.query('percentage_change > 20.0 and Weightings < 500000')
    littlechange = populated2018and2019.query('percentage_change < -20.0 and Weightings < 500000')
    
    #this is for the provision of Avantix data, to be added to core data later

    #not filtering at this stage anymore
    coredata = populated2018and2019.copy()
    #coredata = populated2017and2018.query('Weightings < 500000 and percentage_change < 20.0 and percentage_change > -20.0')

    #export of file where both fares are populated with non-zero figures
    #this below duplicates what's in main as export non-adv avantix data
    #exportfile(coredata,destinationpath,'non-advanced_populated_file')
    exportfile(bigchange.sort_values('Weightings', ascending=False),destinationpath,'big_change_file')
    exportfile(littlechange.sort_values('Weightings', ascending=False), destinationpath,'little_change_file')
    exportfile(bigearners.sort_values('Weightings', ascending=False),destinationpath,'big_earners_file')

    return coredata



def datatypinganddropping(df):
    #drop fields no longer needed
    #del df['orig']
    #del df['dest']
    #del df['route']
    del df['LENNON_PRICE_2018']
    del df['LENNON_PRICE_2019']
    
    #apply datatyping to the four key fields
    #df = applydatatypes(df,['Origin Code','Destination Code','Route Code','Product Code'])
    return df






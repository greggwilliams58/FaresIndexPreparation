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


    RDGprices2017 = get_rdg_prices_info(RDGfarespath
                        ,'2017 fares extract.txt'
                        , destinationpath
                        ,'prices2017.csv', '2017')

    RDGprices2018 = get_rdg_prices_info(RDGfarespath
                        ,'2018 fares extract.txt'
                        , destinationpath
                        ,'prices2018.csv', '2018')
    print("about to merge RDG info into main dataset.")

    #exportfile(df,destinationpath,"non-advanced_data_before RDG")
    #exportfile(RDGprices2017,destinationpath,'2017_RDG')
    #exportfile(RDGprices2018,destinationpath,'2018_RDG')
    #merging RDG fares information
    df = addRDGfaresinfo(df, RDGprices2017,'_2017')
    df = addRDGfaresinfo(df, RDGprices2018,'_2018')

    #exportfile(df,destinationpath,'non_advanced_data_after_RDG')

    #datatyping
    df['Origin Code'] = df['Origin Code'].str.zfill(4)
    df['Destination Code'] = df['Destination Code'].str.zfill(4)
    df['Route Code'] = df['Route Code'].str.zfill(5)

    df = applydatatypes(df,['Regulated_Status'])
    
    
    #print("converting RDG fares to numeric - line 56")
    df[['RDG_FARES_2017','RDG_FARES_2018']] = df[['RDG_FARES_2017','RDG_FARES_2018']].apply(pd.to_numeric)
    
    #exportfile(df,destinationpath,"non-advanced_data_before_LENNON")
    #getting LENNON fare information
    LENNONprices2017 = get_lennon_price_info('2017',LENNONfarespath,'pricefile2017P1112.csv','non-advanced')
    LENNONprices2018 = get_lennon_price_info('2018',LENNONfarespath,'pricefile2018P1112.csv','non-advanced')
    #exportfile(LENNONprices2017,destinationpath,"LENNON_2017")
    #exportfile(LENNONprices2018,destinationpath,"LENNON_2018")
    #merging LENNON fares information
    df = add_lennon_fares_info(df,LENNONprices2017,'_2017','non-advanced')
    df = add_lennon_fares_info(df,LENNONprices2018,'_2018','non-advanced')
    #exportfile(df,destinationpath, "non-advanced data after LENNON")
    #replace empty RDG data with LENNON data
    df['RDG_FARES_2017'].fillna(df['LENNON_PRICE_2017'],inplace=True)
    df['RDG_FARES_2018'].fillna(df['LENNON_PRICE_2018'],inplace=True)

    #drop unnecessary columns and apply categorical datatyping
    #print("Dropping columns - line 71")
    df = datatypinganddropping(df)
    
    # rename the RDG Fares Columns, Earnings and axis
    df.rename(columns={'RDG_FARES_2017':'FARES_2017','RDG_FARES_2018':'FARES_2018','Adjusted Earnings Amount':'Weightings'},inplace=True)
    df.rename_axis('index')

    #export of full file
    exportfile(df,destinationpath,'superfile')
    bigearners = df.query('Weightings > 500000') 
        
    #drop rows where FARES are NaN or 0
    populated2017and2018 = handlezeroandnulls(df)

    #add percentagechange to populated file
    populated2017and2018 = percentagechange(populated2017and2018,'FARES_2018','FARES_2017')

    #this is for validation of large percentage changes' amended data to be added to coredata later
    bigchange = populated2017and2018.query('percentage_change > 20.0 and Weightings < 500000')
    littlechange = populated2017and2018.query('percentage_change < -20.0 and Weightings < 500000')
    
    #this is for the provision of Avantix data, to be added to core data later

    #not filtering at this stage anymore
    coredata = populated2017and2018.copy()
    #coredata = populated2017and2018.query('Weightings < 500000 and percentage_change < 20.0 and percentage_change > -20.0')

    #export of file where both fares are populated with non-zero figures
    #this below duplicates what's in main as export non-adv avantix data
    exportfile(coredata,destinationpath,'non-advanced_populated_file')
    exportfile(bigchange.sort_values('Weightings', ascending=False),destinationpath,'big_change_file')
    exportfile(littlechange.sort_values('Weightings', ascending=False), destinationpath,'little_change_file')
    exportfile(bigearners.sort_values('Weightings', ascending=False),destinationpath,'big_earners_file')

    return coredata



def datatypinganddropping(df):
    #drop fields no longer needed
    del df['orig']
    del df['dest']
    del df['route']
    del df['LENNON_PRICE_2017']
    del df['LENNON_PRICE_2018']
    #del df['RDG_Fares ticket type description_2017']
    #del df['RDG_Fares ticket type description_2018']
    
    #apply datatyping to the four key fields
    df = applydatatypes(df,['Origin Code','Destination Code','Route Code','Product Code'])
    return df






from get_lennon_data import get_lennon_price_info,add_lennon_fares_info
import pandas as pd
from commonfunctions import handlezeroandnulls, percentagechange, exportfile
from calculate_results import calc_weighted_average_price_change


def get_advanced_data(df,destinationpath,LENNONfarespath): 
    """
    This procedure produces a cut of data from the superfile for advanced category of ticket data.  It 
     Sums earnings and journeys, then groups by CarrierTOC; Origin, Destination, Route, Product and Primary Codes; class, sector
     Renames columns
     gets and merges LENNON prices information
     deletes unnecessary columns, renames columns
     removes rows where PRICE information is 0 or NULL
     calculates percentage changes in PRICE

    Parameters
    df:                 A dataframe containing the superfile
    destinationpath:    A string indicating where the output will be exported to
    LENNONfarespath:    A string containing the file path to the advanced LENNON fares information

    Returns
    advanced:           A dataframe containing the calculated advanced dataset 

    """
    #Filter dataset for data that is advanced data
    advanced = df[df['Category']=='advance']

    #sum and group data
    print("The advanced is being grouped \n")
    advanced = advanced.groupby(['carrier_toc_code','origin_code','destination_code','route_code','product_code','pro_group_1_code','class','sector']).agg({'adjusted_earnings':['sum'],"operating_journeys":['sum']})
    #flattening the data into a dataframe
    advanced.columns = ['_'.join(col).strip() for col in advanced.columns.values]
    advanced = advanced.reset_index()
    
    #strip out the '_sum' prefix from the result of grouping
    advanced.rename(columns={'adjusted_earnings_sum':'adjusted_earnings','operating_journeys_sum':'operating_journeys'},inplace=True)

    #getting LENNON fare information
    print("Getting the advanced LENNON information\n")
    LENNONadvancedprices2019 = get_lennon_price_info('2019',LENNONfarespath,'pricefile_advanced_2018.csv','advanced')
    LENNONadvancedprices2020 = get_lennon_price_info('2020',LENNONfarespath,'pricefile_advanced_2019.csv','advanced')
 
    #merging LENNON fares information
    print("adding the advanced LENNON information\n")
    advanced = add_lennon_fares_info(advanced,LENNONadvancedprices2019,'_2019','advanced')
    advanced = add_lennon_fares_info(advanced,LENNONadvancedprices2020,'_2020','advanced')
    
    #deleting unnecessary files
    del advanced['price_2019']
    del advanced['price_2020']

    #renaming columns for year
    advanced.rename(columns={'LENNON_PRICE_2019':'FARES_2019','LENNON_PRICE_2020':'FARES_2020','adjusted_earnings':'Weightings'},inplace=True)

    #remove fares where the values are NULL or 0
    advanced = handlezeroandnulls(advanced)

    #calculate percentage change
    advanced = percentagechange(advanced,'FARES_2020','FARES_2019')
       
    return advanced


    


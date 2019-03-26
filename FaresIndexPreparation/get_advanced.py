from lennon_data import get_lennon_price_info,add_lennon_fares_info
#from export_data import exportfile
import pandas as pd
from commonfunctions import handlezeroandnulls, percentagechange, exportfile
#from calculate_non_advanced_stats import calc_weighted_average_price_change
from calculate_results import calc_weighted_average_price_change


def advanced_data(df,destinationpath,LENNONfarespath): 

    #identify which data is advanced data
    advanced = df[df['Category']=='advance']
    
    del df['orig']
    del df['dest']
    del df['route']

    #add trailing zeros
    advanced['Origin Code'] = advanced['Origin Code'].str.zfill(4)
    advanced['Destination Code'] = advanced['Destination Code'].str.zfill(4)
    advanced['Route Code'] = advanced['Route Code'].str.zfill(5)


    print("This is the advanced file before ")
    print("The advanced is being grouped \n")
    advanced = advanced.groupby(['Carrier TOC / Third Party Code','Origin Code','Destination Code','Route Code','Product Code','Product Primary Code','class','sector']).agg({'Adjusted Earnings Amount':['sum'],"Operating Journeys":['sum']})
    
    print("flattening the df")
    advanced.columns = ['_'.join(col).strip() for col in advanced.columns.values]
    advanced = advanced.reset_index()
    


    #strip out the '_sum' prefix from the result of grouping
    advanced.rename(columns={'Adjusted Earnings Amount_sum':'Adjusted Earnings Amount','Operating Journeys_sum':'Operating Journeys'},inplace=True)



    #getting LENNON fare information
    LENNONadvancedprices2017 = get_lennon_price_info('2017','C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\LENNON_Fares_information\\advanced_data\\','2017_advance_price.csv','advanced')
    exportfile(LENNONadvancedprices2017,destinationpath,"LENNON2017lookup")
    LENNONadvancedprices2018 = get_lennon_price_info('2018','C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\LENNON_Fares_information\\advanced_data\\','2018_advance_price.csv','advanced')
    exportfile(LENNONadvancedprices2018,destinationpath,"LENNON2018lookup")

    print("This is the advanced file before adding lennon data")
    print(advanced.info())
    print(advanced.head(10))
    #merging LENNON fares information
    advanced = add_lennon_fares_info(advanced,LENNONadvancedprices2017,'_2017','advanced')
    #exportfile(advanced,destinationpath,"LENNON_2017_added")
    advanced = add_lennon_fares_info(advanced,LENNONadvancedprices2018,'_2018','advanced')
    #exportfile(advanced,destinationpath,"LENNON_2018_added")

    exportfile(advanced,destinationpath,"advancedline45")

    del advanced['price_2017']
    del advanced['price_2018']

    advanced.rename(columns={'LENNON_PRICE_2017':'FARES_2017','LENNON_PRICE_2018':'FARES_2018','Adjusted Earnings Amount':'Weightings'},inplace=True)

   
    advanced = handlezeroandnulls(advanced)
    exportfile(advanced,destinationpath,"advancedline54")

    advanced = percentagechange(advanced,'FARES_2018','FARES_2017')
    exportfile(advanced,destinationpath,"advancedline57")

    exportfile(advanced,destinationpath,'advancedfile')
    return advanced
#if __name__ == '__main__':
#    main()

    


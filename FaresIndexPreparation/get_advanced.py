from lennon_data import get_lennon_price_info,add_lennon_fares_info
#from export_data import exportfile
import pandas as pd
from commonfunctions import handlezeroandnulls, percentagechange, exportfile
from calculate_non_advanced_stats import calc_weighted_average_price_change


def advanced_data(df,destinationpath,LENNONfarespath): 

    #identify which data is advanced data
    advanced = df[df['Category']=='advance']
    
    del df['orig']
    del df['dest']
    del df['route']

    print("The advanced is being grouped \n")
    advanced = advanced.groupby(['Carrier Profit Centre Code','Origin Code','Destination Code','Route Code','Product Code','Product Level 1 Code','class','sector']).agg({'Adjusted Earning Sterling (*)':['sum'],"Operating Journeys (*)":['sum']})
    
    print("flattening the df")
    advanced.columns = ['_'.join(col).strip() for col in advanced.columns.values]
    advanced = advanced.reset_index()
    
    #strip out the '_sum' prefix from the result of grouping
    advanced.rename(columns={'Adjusted Earning Sterling (*)_sum':'Adjusted Earning Sterling (*)','Operating Journeys (*)_sum':'Operating Journeys (*)'},inplace=True)

    #exportfile(advanced,destinationpath,"grouping")

    #getting LENNON fare information
    LENNONadvancedprices2017 = get_lennon_price_info('2017','C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\LENNON_Fares_information\\advanced_data\\','2017_advance_price.csv','advanced')
    #exportfile(LENNONadvancedprices2017,destinationpath,"LENNON2017lookup")
    LENNONadvancedprices2018 = get_lennon_price_info('2018','C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\LENNON_Fares_information\\advanced_data\\','2018_advance_price.csv','advanced')
    #exportfile(LENNONadvancedprices2018,destinationpath,"LENNON2018lookup")

    #merging LENNON fares information
    advanced = add_lennon_fares_info(advanced,LENNONadvancedprices2017,'_2017','advanced')
    #exportfile(advanced,destinationpath,"LENNON_2017_added")
    advanced = add_lennon_fares_info(advanced,LENNONadvancedprices2018,'_2018','advanced')
    #exportfile(advanced,destinationpath,"LENNON_2018_added")

    del advanced['price_2017']
    del advanced['price_2018']

    advanced.rename(columns={'LENNON_PRICE_2017':'FARES_2017','LENNON_PRICE_2018':'FARES_2018','Adjusted Earning Sterling (*)':'Weightings'},inplace=True)

   
    advanced = handlezeroandnulls(advanced)

    advanced = percentagechange(advanced,'FARES_2018','FARES_2017')


    ###multiplication of weights by percentage_change
    ###advanced.loc[:,'factor'] = advanced.loc[:,'Weightings'] * advanced.loc[:,'percentage_change']
    
    
    ## This section moved to calculate results+
    

    ##print(advanced.info())
    ##advanced_filtered = advanced.query('percentage_change > -20.0 and percentage_change < 20')

    ##advanced_factored = advanced_filtered.copy()



    ###sum of all weights
    ##allweights = advanced_factored['Weightings'].sum()
    ##print(f"All weights are {allweights}\n")

    ##print(calc_weighted_average_price_change(advanced_factored,'sector',allweights,'all'))
    ##print("\n")
    ##print(calc_weighted_average_price_change(advanced_factored,'class',allweights,'all'))
    ##print("\n")

    ##supersplit = calc_weighted_average_price_change(advanced_factored,['sector','class'],allweights,'supersplit')
    ##print(f"The supersplit values are \n {supersplit}")

    ##avgpricechange = advanced_factored['factor'].agg('sum')/advanced_factored['Weightings'].agg('sum')
    ##print(f"All changes are {avgpricechange}")
    

    ###print(advanced_weightings)
    ##exportfile(advanced_factored,destinationpath,'advancedfile')
    ##return advanced_factored
    exportfile(advanced,destinationpath,'advancedfile')
    return advanced
#if __name__ == '__main__':
#    main()

    


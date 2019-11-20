import pandas as pd
from pandas import DataFrame
import pyodbc
import sqlalchemy 
from sqlalchemy import create_engine, MetaData, Table, select, inspect, Column, Integer, func
from sqlalchemy.orm import sessionmaker
import pprint as pp
from commonfunctions import exportfile
from glob import glob
from getDWusagedata import get_journey_by_revenue
import numpy as np


def main():
    #pd.options.mode.chained_assignment = 'raise'
    set_template()

def set_template():
    outputgoesto = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\Template_preparation\\'
    yeartocalculate = 'January 2019'
    JanuaryRPI = 2.5
    

    #get max load_id here' -1 to allow for replication of test data of last year
    lastyearsloadid = getmaxloadid('NETL','factt_205_annual_Fares_Index_stat_release') -1

    #get last year's data
    fares_index_sector_template = getDWdata('NETL','factt_205_annual_Fares_Index_stat_release_test',lastyearsloadid)
    fares_index_tt_template = getDWdata('NETL','factt_205_annual_Fares_Index_tt_stat_release_test',lastyearsloadid)

    #populate the template with blank entries for this year's data
    sector_template = set_blank_template(fares_index_sector_template,'ticket_category',JanuaryRPI)
    tt_template = set_blank_template(fares_index_tt_template,'ticket_type',JanuaryRPI)

    #exportfile(sector_template,outputgoesto,"sector_template")
    #exportfile(tt_template,outputgoesto,"tt_template")

    #populate the template with new data, apart from from passenger revenue
    sector_prep = populatetemplate(sector_template,'ticket_category',outputgoesto,JanuaryRPI,yeartocalculate)
    tt_prep = populatetemplate(tt_template,'ticket_type',outputgoesto,JanuaryRPI,yeartocalculate)

    #get the data required for calculating the percentage change for passenger revenue
    revjourneyraw = get_journey_by_revenue()

    #exportfile(revjourneyraw,outputgoesto,"raw rev journey")
    #insert the index values into the template
    sector_prep = insertrevjourneydata(sector_prep,revjourneyraw)

    #get latest year on year change here
    

    exportfile(sector_prep,outputgoesto,"sector_template_populated")
    exportfile(tt_prep,outputgoesto,"tt_template_populated")

    #drop comparison column no longer needed
    del sector_prep['passrev_variance_from_last_year']

    #import data to warehouse
    importdatatoDW(sector_prep,'NETL','factt_205_annual_Fares_Index_stat_release_test')
    importdatatoDW(tt_prep,'NETL','factt_205_annual_Fares_Index_tt_stat_release_test')




def importdatatoDW(dataset,schema,tablename):
    
    print(f"inserting data into {tablename}\n")
    
    engine = sqlalchemy.create_engine('mssql+pyodbc://AZORRDWSC01/ORR_DW?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes')
    
    conn = engine.connect()

    dataset.to_sql(tablename,conn,schema,if_exists='append',index=False)

    conn.close()



def populatetemplate(new_template,output_type,output,RPI,yeartocalculate):
    """
    Get the blank templates, addthe foreign key to the answerfile, join to the template.
    Populate template with the Avg_change and Exp_Weights through the joined answerfile
    Populate the latest year with shifts to get (prev_year * (avg_change/100))+prev_year

    Parameters:
    new_template:       A dataframe containing the new template with no new values
    output_type:        A string holding the name of the template
    output:             A string holding filepath for export
    RPI:                A float holding the RPI figure
    yeartocalculate:    A string holding the month and year to be calculated

    Output:
    merged_template:    A dataframe holding a template with populated values

    """
    #get the answerfile
    for f in glob(output + 'final answerset*.csv'):
        answerfile = pd.read_csv(f)
            
    #get the answerfile lookup
    answerslookup = pd.read_csv(output + 'answerfile_template_lookup.csv')
    
    #join the foreign key fields from the template
    answerfile_with_lookup = pd.merge(answerfile,answerslookup,how='left',left_on='parts_of_the_grouping',right_on='final_answers')
    
    #join the answerfile to the lookup file
    merged_template = pd.merge(new_template,answerfile_with_lookup[['Sector','Ticket category','average_price_change','superweights','percentage_share_of_superweights_in_grouping']]
                                                                    ,how='left',left_on=['Sector','Ticket category'], right_on=['Sector','Ticket category'],suffixes=('x','y'))
    
    #duplicate rows generated during lookup are deleted here
    merged_template = merged_template.drop_duplicates()
    merged_template.reset_index()
    
    #set the RPI value here
    merged_template.at[merged_template.index.max(),'value'] = RPI
    

    #prepare all tickets, all operator annual change here
    allticketsalloperators = getallticketsalloperators(merged_template, output_type  , yeartocalculate)
    

    merged_template['alltickets'] = np.where(
                                        #sector merge
                                        ((merged_template['Sector']=='All operators') & (merged_template['Ticket category']=='All tickets')  & (merged_template['Year & stats']=='Average change in price (%)')
                                        |
                                        #tt merge
                                        (merged_template['Sector']=='All tickets')&(merged_template['Ticket category']=='All tickets') &  (merged_template['Year & stats']=='Average change in price (%)')),
                                        allticketsalloperators,
                                        merged_template['value']
                                        )
    
    merged_template['value'] = np.where((merged_template['Year & stats']=='Average change in price (%)') &((merged_template['Sector']!='All tickets')| (merged_template['Sector']!='All operators' ) ) 
                                        ,merged_template['average_price_change']
                                        ,merged_template['value'])
    
    
    merged_template['value'] = np.where(merged_template['Year & stats']=='Expenditure weights (%) total',merged_template['percentage_share_of_superweights_in_grouping']*100,merged_template['value'])
    
    # 'all tickets' are fixed at 100 of percentage share 
    merged_template['alltickets'] = np.where(
                                            ((merged_template['Year & stats']=='Expenditure weights (%) total') &(merged_template['Sector']=='All tickets') &(merged_template['Ticket category']=='All tickets') ) |
                                            ((merged_template['Year & stats']=='Expenditure weights (%) total') & (merged_template['Sector']=='All operators')&(merged_template['Ticket category']=='All tickets'))
                                        
                                        ,100.000,merged_template['alltickets'])

    #remove unecessary columns
    del merged_template['average_price_change']
    del merged_template['percentage_share_of_superweights_in_grouping']
    del merged_template['superweights']
    
    #calculated the latest year change; shift 1 = previous year, shift -1 = Average change in year
    merged_template = getlatestyearchange(merged_template,'value',yeartocalculate)
    merged_template = getlatestyearchange(merged_template,'alltickets',yeartocalculate)

    #get yoy change in realterms
    merged_template = getyoychange(merged_template,'value',yeartocalculate,RPI)
    merged_template = getyoychange(merged_template,'alltickets',yeartocalculate,RPI)

    #get allitems index
    merged_template['value']= np.where((merged_template['Sector']=='RPI') & (merged_template['Ticket category']=='All items index') & (merged_template['Year & stats']==yeartocalculate) |
                                       (merged_template['Sector']=='RPI (all items)') & (merged_template['Ticket category']=='RPI (all items)') & (merged_template['Year & stats']==yeartocalculate),
                                       ((merged_template['value'].shift(1) #previous year's value
                                       * RPI)/100)+merged_template['value'].shift(1)
                                       ,
                                       merged_template['value']
                                        )
    
    #define the RPI change since the beginning of the series here
    globalRPI = merged_template['value'].to_list()[-2]
    
    #get yonstart change in realterms
    merged_template = getyonstartchange(merged_template,'value',yeartocalculate,globalRPI)
    merged_template = getyonstartchange(merged_template,'alltickets',yeartocalculate,globalRPI)

    #where value is blank, fill with 'all ticket' values
    merged_template['value'].fillna(merged_template['alltickets'],inplace=True)

    #drop the redundant 'alltickets' column
    del merged_template['alltickets']

    return merged_template


def insertrevjourneydata(st,revjourney):
    """
    This function takes the raw rev/journey data and calculates the year on year change for the full time series

    Parameters:
    st:      A dataframe holding the partially populated sector template
    rj:     A dataframe holding the calculated passrev data
    """
    
    #join the raw pass_rev to the template
    stpassrev = pd.merge(st,revjourney,how='left',on=['Sector','Year & stats'],suffixes=('_st','_rj'))
    
    # assign a new temp_factor with initial values and prep 
    stpassrev['temp_factor'] = np.where((stpassrev['value_rj'].isna()==False) & (stpassrev['Ticket category']=='Revenue per journey')
                                        ,stpassrev['value_rj'].add(100).div(100)
                                        ,np.nan)

    # calculate the cumprod based on the temp_factor (grouped by Sector) and multiply by 100 for index_value
    stpassrev['index_value'] = stpassrev.groupby('Sector')['temp_factor'].cumprod().mul(100)

    #set the inital passrev index to 100
    stpassrev['index_value'] = np.where((stpassrev['value_st'] == 100 ) & (stpassrev['Ticket category']=='Revenue per journey') 
                                        ,100
                                        ,stpassrev['index_value'] )

    ##get variance from last year
    stpassrev['passrev_variance_from_last_year'] = np.where(stpassrev['index_value'].isna()==False
                                    ,stpassrev['value_st'] - stpassrev['index_value']
                                    ,np.nan )

    ##transfer new passrev values into value column
    stpassrev['value_st'] = np.where(stpassrev['index_value'].isna()==False
                                     ,stpassrev['index_value']
                                     ,stpassrev['value_st'])

    # get the average change in price from the value_rj column and pass it into the 
    stpassrev['value_st'] = np.where((stpassrev['Year & stats'] == 'Average change in price (%)' ) & (stpassrev['Ticket category']=='Revenue per journey')
                                     ,stpassrev['value_rj'].shift(1)
                                     ,stpassrev['value_st']
        
        
        )

    #delete unnecessary columns
    del stpassrev['value_rj']
    del stpassrev['temp_factor']
    del stpassrev['index_value']

    #rename value column
    stpassrev.rename(columns={'value_st':'value'},inplace=True)



    return stpassrev

def getlatestyearchange(df,fieldtoworkon,yeartocalculate):
    #calculated the latest year change; shift 1 = previous year, shift -1 = Average change in year
    df[fieldtoworkon]= np.where(df['Year & stats']==yeartocalculate,(df[fieldtoworkon].shift(1) #previous years value
                                                                   * (df[fieldtoworkon].shift(-1)/100)) #average change in year
                                                                   + df[fieldtoworkon].shift(1) #previous year's values
                                                                   , df[fieldtoworkon]) # if not relevant field, keep current field values
    return df


def getyoychange(df,field, nextyear,RPI):
    """
    This calculates the year on year change in prices

    yoy = (latest year- (previous year * (RPI/100 )) / previous year * (RPI/100) 

    Parameters:
    df:         A dataframe containing the template
    field:      A string holding the name of the column to be manipulated
    nextyear:   A string holding the name of the year being calculated
    RPI:        A float holding RPI value for the current year

    Returns:
    df:         A dataframe with the year on year change value

    """
    
    #get yoy change in realterms
    df[field] = np.where((df['Year & stats']==f'Real terms change in average price {nextyear[-4:]} on {int(nextyear[-4:])-1}')|(df['Year & stats']==f'Real terms change in average price year on year'),
                                        ((df[field].shift(3) #latest year change
                                        - ((df[field].shift(4)*(RPI/100))+df[field].shift(4))) #previous year change
                                        / ((df[field].shift(4)*(RPI/100))+df[field].shift(4))
                                        *100)
                                        ,
                                        df[field])
    return df


def getyonstartchange(df,field,nextyear,seriesRPI):
    """
    This calculates the real terms price change from the start of the series to latest year

    y on start change = ((latest year - seriesRPIchange )/seriesRPIchange) * 100

    Parameters:
    df:         A dataframe containing the template
    field:      A string holding the name of the column to be manipulated
    nextyear:   A string holding the name of the year being calculated
    seriesRPI:  A float holding RPI value for whole series

    Returns:
    df:         A dataframe with the year on year change value
    """
    
    #get yonstart change in realterms
    df[field] = np.where((df['Year & stats']==f"Real terms change in average price {nextyear[-4:]} on 1995")
                                        |(df['Year & stats']==f"Real terms change in average price year on 2004")
                            ,((df[field].shift(4) #latest year change
                            - seriesRPI) #RPI for all items
                            / seriesRPI)*100 #RPI for all items
                            ,df[field])
    return df


def getallticketsalloperators(df,typeofoutput,nextyear):
    """
    This calculates the annual change data for the "all tickets" category.  This is needed as the answerfile does not contain a non-split set of values to work with.
    The relevant values are extracted from the template itself  and manipulated to produce the values which are then inserted back to relevant rows of the template.
    As the name so the fields vary between templates a if statement is needed to declare lists holding relevant values.

    alloperators, all tickets = total_pc_and_superweights/all superweights

    total_pc_and_superweights = average_price_change * superweights WHERE sectors = LSE,LD,REgional

    all superweights = superweights WHERE sectors = LSE,LD,Regional

    Parameters:
    df:                                 A dataframe holding the template
    typeofout:                          A string determining the type of template being produced
    nextyear:                           A string holding the year being calculated

    Returns:
    alloperatorsallticketspricechange:  A float holding the pricechange for all operators, all tickets

    """
    
    #prepare all tickets, all operator figures
    
    if typeofoutput == 'ticket_type':
        fieldstosearch =['London and SE operators','Long-distance operators','Regional operators']
    elif typeofoutput =='ticket_category':
        fieldstosearch =['London and South East','Long distance','Regional']
    else:
        print("assignment of output type wrong in getallticketoperators")

    #create np array holding LSE data
    LSE = np.where((df['Sector']==fieldstosearch[0])   &(df['Ticket category']=='All tickets')&(df['Year & stats']==nextyear),
                   df['average_price_change'] * df['superweights'],0  )
    #create np array holding LD data
    LD = np.where((df['Sector']==fieldstosearch[1]) &(df['Ticket category']=='All tickets')&(df['Year & stats']==nextyear),
                   df['average_price_change'] * df['superweights'],0 )

    #create np array holding Regional data
    Regional = np.where((df['Sector']==fieldstosearch[2])   &(df['Ticket category']=='All tickets')&(df['Year & stats']==nextyear),
                df['average_price_change'] * df['superweights'],0   )
    
    #strip out unnecessary zeros by summing np.array
    LSE = np.sum(LSE)
    LD = np.sum(LD)
    Regional = np.sum(Regional)

    #add the three sectors together for a total
    total_pc_and_superweights = LSE + LD + Regional

    #get the superweights by looking for sectors 
    sumofsuperweights = np.where(
        (
        (df['Sector']==fieldstosearch[0])
        |(df['Sector']==fieldstosearch[1])
        |(df['Sector']==fieldstosearch[2]))  &(df['Ticket category']=='All tickets')&(df['Year & stats']==nextyear),
                df['superweights']
                ,0 )

    #strip out zeros by summing
    sumofsuperweights = np.sum(sumofsuperweights)

    #calculate final ratio
    alloperatorsallticketspricechange = total_pc_and_superweights/sumofsuperweights

    return alloperatorsallticketspricechange


def set_blank_template(df,type,RPI ):
    """
    This creates a new blank template by operating on a copy of last year's template stored as a dataframe
    The value for the next year, next load_id and publication_status is calculated and added to names for relevant fields into a list of new items
    dependent functions are  addnewcatrows (which adds newly formatted rows to dataframe) and addnewyearsrows (which adds new "january 20xx" rows to dataframe
    This list of new row items is them appended to the df holding the previous year' template and is sorted by ticket category and year&stat number fields

    Parameters:
    df:             A dataframe holding the previous year's template extracted from warehouse
    type:           A string holding the template being created (tt or sector)
    RPI:            A float holding the RPI value
    
    Returns:
    newtemplate:    A dataframe holding descriptions of data for the new year's publication
    """
    #get max year and the new year value
    max_year =df['Year & stats'].to_list()[-2][8:]
    
    new_year = str(int(max_year) + 1)
    previous_year = str( int(max_year)-1)
    
    if type == 'ticket_category':
        orderingofyearandstats = 'ordering of year & stats'
        orderofticketcat = 'ordering value of ticket category'
        newrowitems = ['Average change in price (%)','Expenditure weights (%) total','Real terms change in average price year on year','Real terms change in average price year on 2004']
    elif type == 'ticket_type':
        orderingofyearandstats = 'ordering value of year and stats'
        orderofticketcat = 'ordering values of ticket category'                                                                                           
        newrowitems = ['Average change in price (%)','Expenditure weights (%) total',f'Real terms change in average price {max_year} on {previous_year}','Real terms change in average price '+ max_year +' on 1995']

    else:
        print("check prep_template values")

    #get max load id and the next ID value
    old_load_id = int(df['Load_ID'].to_list()[-1])
    new_load_id = old_load_id + 1

    #set publication status
    publication_status = 'Approved'

    ##get latest stat order column
    max_stat_order = int(df[orderingofyearandstats][df['Year & stats']=='January '+max_year].to_list()[-1])
    latest_order = max_stat_order + 1

    newdatarows = []
    for increment,items in enumerate(newrowitems,1):
        newset = addnewcatrows(df,type,new_load_id,publication_status,items,latest_order,increment,orderingofyearandstats,previous_year,max_year,new_year,RPI)
        newdatarows.append(newset)
        df.drop(df[df['Year & stats']==items].index,inplace=True)

    
    annualdata = addnewyearsrows(df,type,max_year,new_load_id,publication_status,orderingofyearandstats)

    newdatarows.append(annualdata)

    #join old dataset with new rows
    newtemplate = df.append(newdatarows)
    newtemplate.sort_values(by=[orderofticketcat,orderingofyearandstats],inplace=True)
    newtemplate.loc[:,'Load_ID'] = new_load_id
    newtemplate.loc[:,'Publication_status'] = publication_status
    
    newtemplate.reset_index(drop=True, inplace=True)
    
    return newtemplate
   

def addnewyearsrows(fulldataset,type,maxyear,newloadid,pubstatus,orderyearandstats):
    """
    This creates the dataset for the new year of data 

    Parameters:
    fulldataset     A dataframe containing the full dataset
    maxyear         An integer holding the maximum year value
    pubstatus       A string holding the publication status of the dataset

    Returns:
    janvalue        A dataframe holding the new rows for january data

    """
    
    #get latest year and increment by 1
    latest_year_subset = fulldataset[fulldataset['Year & stats']=='January '+maxyear]
    new_year = str(int(maxyear) + 1)

    #get latest stat order column
    max_stat_order = int(fulldataset[orderyearandstats][fulldataset['Year & stats']=='January '+maxyear].to_list()[-1])
    latest_order = max_stat_order + 1

    #deepcopy to avoid warnings about copying a slice
    janvalues = latest_year_subset.copy(deep=True)

    #amend the values of the subset for Jan XXX
    janvalues.loc[:,'Load_ID'] = newloadid
    janvalues.loc[:,'Publication_status'] = pubstatus
    #sector remains the same
    #ticket category remains the same
    #ordering of ticket category remains the same
    janvalues.loc[:,'Year & stats'] = 'January '+new_year
    janvalues.loc[:,orderyearandstats] = latest_order
    janvalues.loc[:,'value'] = np.nan
    
    return janvalues


def addnewcatrows(fulldataset,type,new_load_id,publication_status,category,sortnumber,increment,orderyearandstats,previous_year,max_year, new_year,RPI):
    """
    This is the add the rows for four generic items

    Parameters
    fulldataset:        A dataframe containing the full template from DW table
    new_load_id:        An integer holding the new load_id
    publication_status: A string holding the publication_status of the data
    category:           A string containing the name of the category
    sortnumber:         An integer to provide the base sort number
    increment:          An integet to increment the sort number by


    """
    
    subset = fulldataset[fulldataset['Year & stats']==category ]

    subset = subset.copy(deep=True)

    subset.loc[:,'Load_ID'] = new_load_id
    subset.loc[:,'Publication_status'] = publication_status
    #sector remains the same
    #ticket category remains the same
    #ordering of ticket category remains the same
    
    if type =='ticket_category':
        #year & stats remains the same
        pass

    elif type == 'ticket_type':
        
        subset.loc[:,'Year & stats'] = subset.loc[:,'Year & stats'].replace(max_year,new_year,regex=True)
        subset.loc[:,'Year & stats'] = subset.loc[:,'Year & stats'].replace(previous_year,max_year,regex=True)

    else:
        print("check addnewcatrows line 144")
    subset.loc[:,orderyearandstats] = sortnumber + increment
    subset.loc[:,'value'] = np.nan

    return subset


def getDWdata(schema_name,table_name,source_item_id):
    """
    This uses SQL Alchemy to connect to SQL Server via a trusted connection and extract a filtered table, which is then coverted into a dataframe.
    This is intended for getting the partial table for fact data.

    Parameters
    schema_name:    A string represetnting the schema of the table
    table_name:     A string representing the name of the table
    source_item_id: An integer representing the source_item_id needed

    returns:        
    df:             A dataframe containing the table   
    """
    engine = sqlalchemy.create_engine('mssql+pyodbc://AZORRDWSC01/ORR_DW?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes')
    
    conn = engine.connect()

    metadata = MetaData()

    example_table = Table(table_name, metadata,autoload=True, autoload_with=engine, schema=schema_name)

    #get raw table data, filtered by source_item_id
    query = select([example_table]).where(example_table.c.Load_ID == source_item_id)

    df = pd.read_sql(query, conn)
    
    conn.close()

    return df


def getmaxloadid(schema_name,table_name):
    """
    This uses SQL Alchemy to connect to SQL Server via a trusted connection and extract a filtered table, which is then coverted into a dataframe.
    This is intended for getting the partial table for fact data.

    Parameters
    schema_name:    A string represetnting the schema of the table
    table_name:     A string representing the name of the table
    

    returns:
    maxid:          An integer containing the highest value for Load_ID in the DW   
    """
    engine = sqlalchemy.create_engine('mssql+pyodbc://AZORRDWSC01/ORR_DW?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes')
    
    conn = engine.connect()

    metadata = MetaData()

    example_table = Table(table_name, metadata,autoload=True, autoload_with=engine, schema=schema_name)

    #get the load_id column
    query = select([example_table.c.Load_ID])

    #get the resultset converted into a single column dataframe
    loadids = pd.read_sql(query, conn)
    
    #get the first (and only) instance of the load_id column as a integer
    maxid = int(loadids.max()[0])
    

    conn.close()
    return maxid



if __name__ == '__main__':
    main()


import pandas as pd
from pandas import DataFrame
import pyodbc
import sqlalchemy 
from sqlalchemy import create_engine, MetaData, Table, select, inspect
from sqlalchemy.orm import sessionmaker
import pprint as pp
from commonfunctions import exportfile
from glob import glob
import numpy as np


def main():
    #pd.options.mode.chained_assignment = 'raise'
    set_template()

def set_template():
    outputgoesto = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\Template_preparation\\'
    yeartocalculate = 'January 2019'
    RPIvalue = 2.5
    lastyearsloadid = 9

    #get last year's data
    fares_index_sector_template = getDWdata('NETL','factt_205_annual_Fares_Index_stat_release',lastyearsloadid)
    fares_index_tt_template = getDWdata('NETL','factt_205_annual_Fares_Index_tt_stat_release',lastyearsloadid)

    #populate the current year's data
    sector_template = set_blank_template(fares_index_sector_template,'ticket_category',RPIvalue,outputgoesto)
    tt_template = set_blank_template(fares_index_tt_template,'ticket_type',RPIvalue,outputgoesto)

    #exportfile(sector_template,outputgoesto,"sector_template")
    #exportfile(tt_template,outputgoesto,"tt_template")

    sector_prep = populatetemplate(sector_template,'ticket_category',outputgoesto,2.5,yeartocalculate)
    tt_prep = populatetemplate(tt_template,'ticket_type',outputgoesto,RPIvalue,yeartocalculate)

    exportfile(sector_prep,outputgoesto,"sector_template_populated")
    exportfile(tt_prep,outputgoesto,"tt_template_populated")


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
    
    #set the RPI value here
    merged_template.at[merged_template.index.max(),'value'] = RPI
    
    #get avg change and exp_weights via lookupfile where not Ticket Category = 'All tickets' in first lookup to prevent duplicate rows being generated
    merged_template['value'] = np.where((merged_template['Year & stats']=='Average change in price (%)') ,merged_template['average_price_change'],merged_template['value'])
    merged_template['value'] = np.where(merged_template['Year & stats']=='Expenditure weights (%) total',merged_template['percentage_share_of_superweights_in_grouping']*100,merged_template['value'])
    
    #remove unecessary columns
    del merged_template['average_price_change']
    del merged_template['percentage_share_of_superweights_in_grouping']
    del merged_template['superweights']

    #calculated the latest year change; shift 1 = previous year, shift -1 = Average change in year
    merged_template['value']= np.where(merged_template['Year & stats']==yeartocalculate,(merged_template['value'].shift(1) #previous years value
                                                                                         * (merged_template['value'].shift(-1)/100)) #average change in year
                                                                                        + merged_template['value'].shift(1) #previous year's values
                                                                                    ,
                                                                                    merged_template['value'])
    
    #get yoy change in realterms
    merged_template['value'] = np.where((merged_template['Year & stats']==f'Real terms change in average price {yeartocalculate[-4:]} on {int(yeartocalculate[-4:])-1}')|(merged_template['Year & stats']==f'Real terms change in average price year on year'),
                                        ((merged_template['value'].shift(3) #latest year change
                                        - ((merged_template['value'].shift(4)*(RPI/100))+merged_template['value'].shift(4))) #previous year change
                                        / ((merged_template['value'].shift(4)*(RPI/100))+merged_template['value'].shift(4))
                                        *100)
                                        ,
                                        merged_template['value'])
    
    #get allitems index
    merged_template['value']= np.where((merged_template['Sector']=='RPI') & (merged_template['Ticket category']=='All items index') & (merged_template['Year & stats']==yeartocalculate) |
                                       (merged_template['Sector']=='RPI (all items)') & (merged_template['Ticket category']=='RPI (all items)') & (merged_template['Year & stats']==yeartocalculate),
                                       ((merged_template['value'].shift(1) #previous year's value
                                       * RPI)/100)+merged_template['value'].shift(1)
                                       ,
                                       merged_template['value']
                                        )
    
    #global RPI change across the whole data 
    globalRPI = merged_template['value'].to_list()[-2]
    
    
    #get yonstart change in realterms
    merged_template['value'] = np.where((merged_template['Year & stats']==f"Real terms change in average price {yeartocalculate[-4:]} on 1995")|(merged_template['Year & stats']==f"Real terms change in average price year on 2004")
                                                                                                                                ,((merged_template['value'].shift(4) #latest year change
                                                                                                                                - globalRPI) #RPI for all items
                                                                                                                                / globalRPI)*100 #RPI for all items
                                                                                                                                ,merged_template['value'])
    
    #set the RPI value here
    merged_template.at[merged_template.index.max(),'value'] = RPI
    #exportfile(merged_template,output, f'{output_type} with price_change and superweight_share')

    return merged_template



def set_blank_template(df,type,RPI,basetemplatepreplocation ):
    
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
    

    #exportfile(newtemplate,basetemplatepreplocation,F"new_{type}")
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

    returns:        A dataframe containing the table   
    """
    engine = sqlalchemy.create_engine('mssql+pyodbc://AZORRDWSC01/ORR_DW?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes')
    
    conn = engine.connect()

    metadata = MetaData()

    example_table = Table(table_name, metadata,autoload=True, autoload_with=engine, schema=schema_name)

    #get raw table data, filtered by source_item_id
    query = select([example_table]).where(example_table.c.Load_ID == source_item_id)

    df = pd.read_sql(query, conn)
    return df


if __name__ == '__main__':
    main()


import pandas as pd
from pandas import DataFrame
import pyodbc
import sqlalchemy 
from sqlalchemy import create_engine, MetaData, Table, select, inspect
from sqlalchemy.orm import sessionmaker
import pprint as pp
from commonfunctions import exportfile



def main():
    #pd.options.mode.chained_assignment = 'raise'
    set_template()

def set_template():

    fares_index_sector_template = getDWdata('NETL','factt_205_annual_Fares_Index_stat_release',9)
    fares_index_tt_template = getDWdata('NETL','factt_205_annual_Fares_Index_tt_stat_release',9)


    print(fares_index_sector_template.head(20))
    print(fares_index_sector_template.info())

    prep_template(fares_index_sector_template,'sector',2.5)



def prep_template(df,type,RPI):
    
    #get max year and the new year value
    max_year =df['Year & stats'].to_list()[-2][8:]
    new_year = str(int(max_year) + 1)

    #get max load id and the next ID value
    old_load_id = int(df['Load_ID'].to_list()[-1])
    new_load_id = old_load_id + 1

    #set publication status
    publication_status = 'Approved'

    #get latest year
    latest_year_subset = df[df['Year & stats']=='January '+max_year]

    #get latest stat order column
    max_stat_order = int(df['ordering of year & stats'][df['Year & stats']=='January '+max_year].to_list()[-1])
    latest_order = max_stat_order + 1

    janvalues = latest_year_subset.copy(deep=True)
    print(latest_year_subset)

    #amend the values of the subset for Jan XXX
    janvalues.loc[:,'Load_ID'] = new_load_id
    janvalues.loc[:,'Publication_status'] = publication_status
    #sector remains the same
    #ticket category remains the same
    #ordering of ticket category remains the same

    janvalues.loc[:,'Year & stats'] = 'January '+new_year
    janvalues.loc[:,'ordering of year & stats'] = latest_order
    janvalues.loc[:,'value'] = None
    
    #print("New values")
    #print(janvalues)

    avg_price_change = addnewrows(df,new_load_id,publication_status,'Average change in price (%)',latest_order,1)
    exp_weights = addnewrows(df,new_load_id,publication_status,'Expenditure weights (%) total',latest_order,2)
    yoy_change = addnewrows(df,new_load_id,publication_status,'Real terms change in average price year on year',latest_order,3)
    yon2004change = addnewrows(df,new_load_id,publication_status,'Real terms change in average price year on 2004',latest_order,4)

    ##amend the values of the subset for Average change in price
    #changeinprice = df[df['Year & stats']=='Average change in price (%)']
    #pricechangevalues = changeinprice.copy(deep=True)

    #pricechangevalues.loc[:,'Load_ID'] = new_load_id
    #pricechangevalues.loc[:,'Publication_status'] = publication_status
    ##sector remains the same
    ##ticket category remains the same
    ##ordering of ticket category remains the same
    ##year & stats remains the same
    #pricechangevalues.loc[:,'ordering of year & stats'] = latest_order + 1
    #pricechangevalues.loc[:,'value'] = None

    ##print(pricechangevalues)


    ##amend the values of the subset for Expenditure Weights
    #expweights = df[df['Year & stats']=='Expenditure weights (%) total']
    #print(expweights)
    #expweightvalues = expweights.copy(deep=True)

    #expweightvalues.loc[:,'Load_ID'] = new_load_id
    #expweightvalues.loc[:,'Publication_status'] = publication_status
    ##sector remains the same
    ##ticket category remains the same
    ##ordering of ticket category remains the same
    ##year & stats remains the same
    #expweightvalues.loc[:,'ordering of year & stats'] = latest_order + 2
    #expweightvalues.loc[:,'value'] = None

    ##print(expweightvalues)

    
    ##amend the values of the subset for YonY change
    #yoychange = df[df['Year & stats']=='Real terms change in average price year on year']
    #print(yoychange)
    #yoychangevalues = yoychange.copy(deep=True)

    #yoychangevalues.loc[:,'Load_ID'] = new_load_id
    #yoychangevalues.loc[:,'Publication_status'] = publication_status
    ##sector remains the same
    ##ticket category remains the same
    ##ordering of ticket category remains the same
    ##year & stats remains the same
    #yoychangevalues.loc[:,'ordering of year & stats'] = latest_order + 3
    #yoychangevalues.loc[:,'value'] = None

    ##print(yoychangevalues)

    ##amend the values of the subset for Yon2004 change
    #yo2004change = df[df['Year & stats']=='Real terms change in average price year on 2004']
    #print(yo2004change)
    #yo2004changevalues = yoychange.copy(deep=True)

    #yo2004changevalues.loc[:,'Load_ID'] = new_load_id
    #yo2004changevalues.loc[:,'Publication_status'] = publication_status
    ##sector remains the same
    ##ticket category remains the same
    ##ordering of ticket category remains the same
    ##year & stats remains the same
    #yo2004changevalues.loc[:,'ordering of year & stats'] = latest_order + 4
    #yo2004changevalues.loc[:,'value'] = None

    ##print(yo2004changevalues)



def addnewrows(fulldataset,new_load_id,publication_status,category,sortnumber,increment):
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
    outputgoesto = outputto = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\advanced_and_non_advanced_output\\adv_non_advanced_and_superfile\\'
    #amend the values of the subset for Yon2004 change
    subset = fulldataset[fulldataset['Year & stats']==category ]

    subset = subset.copy(deep=True)

    subset.loc[:,'Load_ID'] = new_load_id
    subset.loc[:,'Publication_status'] = publication_status
    #sector remains the same
    #ticket category remains the same
    #ordering of ticket category remains the same
    #year & stats remains the same
    subset.loc[:,'ordering of year & stats'] = sortnumber + increment
    subset.loc[:,'value'] = None

    exportfile(subset,outputgoesto,category)


    







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


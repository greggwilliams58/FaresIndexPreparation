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
    outputgoesto = outputto = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\advanced_and_non_advanced_output\\adv_non_advanced_and_superfile\\'
    #amend the values of the subset for Yon2004 change
    fares_index_sector_template = getDWdata('NETL','factt_205_annual_Fares_Index_stat_release',9)
    fares_index_tt_template = getDWdata('NETL','factt_205_annual_Fares_Index_tt_stat_release',9)


    prep_template(fares_index_sector_template,'sector',2.5,outputgoesto)



def prep_template(df,type,RPI,outputpath):
    
    #get max year and the new year value
    max_year =df['Year & stats'].to_list()[-2][8:]

    #get max load id and the next ID value
    old_load_id = int(df['Load_ID'].to_list()[-1])
    new_load_id = old_load_id + 1

    #set publication status
    publication_status = 'Approved'

    #get latest year
    #latest_year_subset = df[df['Year & stats']=='January '+max_year]

    ##get latest stat order column
    max_stat_order = int(df['ordering of year & stats'][df['Year & stats']=='January '+max_year].to_list()[-1])
    latest_order = max_stat_order + 1

    newrowitems = ['Average change in price (%)','Expenditure weights (%) total','Real terms change in average price year on year','Real terms change in average price year on 2004']

    

    newdatarows = []
    for increment,items in enumerate(newrowitems,1):
        newset = addnewcatrows(df,new_load_id,publication_status,items,latest_order,increment)
        newdatarows.append(newset)
        df.drop(df[df['Year & stats']==items].index,inplace=True)

    
    annualdata = addnewyearsrows(df,max_year,new_load_id,publication_status)

    newdatarows.append(annualdata)

    #join old dataset with new rows
    newtemplate = df.append(newdatarows)
    newtemplate.sort_values(by=['ordering value of ticket category','ordering of year & stats'],inplace=True)
    newtemplate.loc[:,'Load_ID'] = new_load_id
    newtemplate.loc[:,'Publication_status'] = publication_status

    newtemplate.reset_index(drop=True, inplace=True)

    
    exportfile(newtemplate,outputpath,"newtemplate")

   
def addnewyearsrows(fulldataset,maxyear,newloadid,pubstatus):
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
    max_stat_order = int(fulldataset['ordering of year & stats'][fulldataset['Year & stats']=='January '+maxyear].to_list()[-1])
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
    janvalues.loc[:,'ordering of year & stats'] = latest_order
    janvalues.loc[:,'value'] = None
    
    return janvalues


def addnewcatrows(fulldataset,new_load_id,publication_status,category,sortnumber,increment):
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
    #year & stats remains the same
    subset.loc[:,'ordering of year & stats'] = sortnumber + increment
    subset.loc[:,'value'] = None

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


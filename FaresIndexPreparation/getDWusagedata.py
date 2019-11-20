import pandas as pd
from pandas import DataFrame
import pyodbc
import sqlalchemy 
from sqlalchemy import create_engine, MetaData, Table, select, inspect,Float,Integer, Column, func
from sqlalchemy.orm import sessionmaker
import pprint as pp
from commonfunctions import exportfile
from glob import glob
import numpy as np

def get_journey_by_revenue():
    """
    This function is to produce the passenger/revenue calculations for the rail fares index

    Parameters:
    schema:     A string containing the schema of the view
    view:       A sting containing the name of the view
    to_exclude: An integer giving the load_id of the non-lennon data to ignore
    """

    #extract the usage data from the warehouse
    rev = getusagedata('ORR','factv_205_cube_lennon_passenger_revenue_by_sector',99999  )
    journey = getusagedata('ORR','factv_205_cube_lennon_passenger_journies_by sector',99999)

    #reshape the data into what is required
    rolling_rev = manipulatethedata(rev)
    rolling_journey = manipulatethedata(journey)

    #produce revenue by journey: divide rev by journey
    rev_by_journey = rolling_rev[['Long_distance','LSE','Regional','Total_Franchised']]/rolling_journey[['Long_distance','LSE','Regional','Total_Franchised']]

    #calculate percentage change and *100 to show it as percentage
    pct_change = rev_by_journey.pct_change()*100

    #put the labels lost by rollling sum back in
    pct_change_with_labels = rolling_rev[['financial_year_key','year_and_quarter']].join(pct_change[['Long_distance','LSE','Regional','Total_Franchised']]).dropna()

    #create the 'Year and stats' column for the template
    pct_change_with_labels['Year & stats'] = "January 20" + pct_change_with_labels['year_and_quarter'].str[5:7]

    #rename columns to match categories in the template
    mapper = {'Long_distance':'Long distance','LSE':'London and South East','Regional':'Regional','Total_Franchised':'All operators'}
    final_dataframe = pct_change_with_labels.rename(columns=mapper)
    
    #delete redundant columns
    del final_dataframe['financial_year_key']
    del final_dataframe['year_and_quarter']

    final_dataframe = final_dataframe.melt(id_vars=['Year & stats'],value_vars=['Long distance','London and South East','Regional', 'All operators'])
    mapper_after_stack = {'variable':'Sector'}
    
    final_dataframe = final_dataframe.rename(columns=mapper_after_stack)
    return final_dataframe


def getusagedata(schema_name,table_name,source_item_id, publication_status=('published','approved')):
    """
    This uses SQL Alchemy to connect to SQL Server via a trusted connection and extract a view, which is then coverted into a dataframe.
    This is intended for extracting usage data for journeys and revenue.

    Note use of func.sum; Column within the table definition for aliasing; the use of .in_;  as well as group and order by


    Parameters
    schema_name:        A string represetnting the schema of the table
    table_name:         A string representing the name of the table
    source_item_id:     An integer representing the source_item_id to be excluded (to avoid double-counting)
    publication_status: A tuple representing the publication status to included (set to published and approved by default)

    returns:        A dataframe containing the table   
    """
    engine = sqlalchemy.create_engine('mssql+pyodbc://AZORRDWSC01/ORR_DW?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes')
    
    conn = engine.connect()

    metadata = MetaData()

    usagedata = Table(table_name,
                         metadata,
                         #handle the oddly named columns
                        Column("financial_year_key",Integer,key = "financial_year_key")
                        ,Column("Long-distance",Float,key = "Long_distance")
                        ,Column("London and South East",Float,key = "LSE") 
                        ,Column("Non-franchised",Float,key = "Non_Franchised")
                          ,autoload=True, autoload_with=engine, schema=schema_name
                          )

    #get raw table data, filtered by source_item_id
    query = select([usagedata.c.financial_year_key
                    ,usagedata.c.year_and_quarter
                    ,func.sum(usagedata.c.Long_distance).label("Long_distance") 
                    ,func.sum(usagedata.c.LSE).label("LSE")
                    ,func.sum(usagedata.c.Regional).label("Regional")
                    ,func.sum(usagedata.c.Non_Franchised).label("Non_Franchised")  ]                   
                   )
    query = query.where(usagedata.c.source_item_id != source_item_id)
    query = query.where(usagedata.c.publication_status.in_(('approved', 'published'))  )
    query = query.where(usagedata.c.year_and_quarter >= '2002-03 Quarter 1')
    query = query.group_by(usagedata.c.financial_year_key,usagedata.c.year_and_quarter)
    query = query.order_by(usagedata.c.financial_year_key.asc())

    df_plain = pd.read_sql(query, conn)


    conn.close()

    return df_plain


def manipulatethedata(df):
    """
    This takes the raw df and performs the necessary manipulations to produce the end figures.
    
    Parameters:
    df:         A dataframe containing the responses from the query to the datawarehouse

    Returns:
    dfQ3only:   A dataframe containing a rolling annual sum for Q3 data
    
    """
    #produces total figures as sum of components
    df['Total_Franchised'] = df['Long_distance'] + df['LSE'] + df['Regional'] 

    #produces a rolling total based on 4 quarters
    df_rolling = df[['Long_distance','LSE','Regional','Non_Franchised','Total_Franchised']].rolling(4).sum()

    #joins the labels back to the data-only view produced by the previous step
    df_with_labels = df[['financial_year_key','year_and_quarter']].join(df_rolling[['Long_distance','LSE','Regional','Total_Franchised']])

    #strips out the non-Q3 data, so irrelevant data is not present
    df_Q3_only = df_with_labels[df_with_labels['year_and_quarter'].str.contains('Quarter 3')]

    return df_Q3_only

if __name__ == '__main__':
    main()

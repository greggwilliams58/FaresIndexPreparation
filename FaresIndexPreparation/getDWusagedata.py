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

def main():
    rev = getusagedata('ORR','factv_205_cube_lennon_passenger_revenue_by_sector',99999)
    journey = getusagedata('ORR','factv_205_cube_lennon_passenger_journies_by sector',99999)

    rolling_rev = manipulatethedata(rev)
    rolling_journey = manipulatethedata(journey)

    rev_by_journey = rolling_rev[['Long_distance','LSE','Regional','Total_Franchised']]/rolling_journey[['Long_distance','LSE','Regional','Total_Franchised']]

    pct_change = rev_by_journey.pct_change()*100

    pct_change_with_labels = rolling_rev[['financial_year_key','year_and_quarter']].join(pct_change[['Long_distance','LSE','Regional','Total_Franchised']]).dropna()

    del pct_change_with_labels['financial_year_key']
    print(pct_change_with_labels['year_and_quarter'].str[5:7])
   
    pct_change_with_labels['Year & stats'] = "January 20" + pct_change_with_labels['year_and_quarter'].str[5:7]


    mapper = {'Long_distance':'Long distance','LSE':'London and South East','Regional':'Regional','Total_Franchised':'All operators'}
    final_dataframe = pct_change_with_labels.rename(columns=mapper)


    print(final_dataframe)

def getusagedata(schema_name,table_name,source_item_id):
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
    query = query.where(usagedata.c.publication_status == 'published')
    query = query.where(usagedata.c.year_and_quarter >= '2002-03 Quarter 1')
    query = query.group_by(usagedata.c.financial_year_key,usagedata.c.year_and_quarter)
    query = query.order_by(usagedata.c.financial_year_key.asc())

    df_plain = pd.read_sql(query, conn)

    return df_plain


def manipulatethedata(df):
    df['Total_Franchised'] = df['Long_distance'] + df['LSE'] + df['Regional'] 

    df_rolling = df[['Long_distance','LSE','Regional','Non_Franchised','Total_Franchised']].rolling(4).sum()

    df_with_labels = df[['financial_year_key','year_and_quarter']].join(df_rolling[['Long_distance','LSE','Regional','Total_Franchised']])

    df_Q3_only = df_with_labels[df_with_labels['year_and_quarter'].str.contains('Quarter 3')]


    return df_Q3_only

if __name__ == '__main__':
    main()

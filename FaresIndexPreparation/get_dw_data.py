import pypyodbc
from sqlalchemy import create_engine, Table,MetaData
from pprint import pprint
import pandas as pd

def main():

    #FaresIndex = gettext("[Load_ID],[Publication_status],[Sector],[Ticket category],[ordering value of ticket category],[Year & stats],[ordering of year & stats],[value]",
    #                     "[ORR_DW].[NETL].[factt_205_annual_Fares_Index_stat_release]",
    #                     "Publication_status= 'published'")
    #
    #FI = pd.DataFrame(FaresIndex,columns=['Load_ID','Publication_status','Sector','Ticket category','ordering value of ticket category','Year & stats','ordering of year & stats','value'])
    #pprint(FI)

    #FaresIndexTT = gettext("[Load_ID],[Publication_status],[Sector],[Ticket category],[ordering values of ticket category] ,[Year & stats] ,[ordering value of year and stats],[value]",
    #                       "[ORR_DW].[NETL].[factt_205_annual_Fares_Index_tt_stat_release]",
    #                       "Publication_status= 'published'"
    #                       
    #                       )
    #FITT = pd.DataFrame(FaresIndexTT,columns=['Load_ID','Publication_status','Sector','Ticket category','ordering values of ticket category' ,'Year & stats' ,'ordering value of year and stats','value'])
    #pprint(FITT)
    testdf = pd.DataFrame(columns=['Load_ID','Publication_status','Sector','Ticket category','ordering value of ticket category','Year & stats','ordering of year & stats','value'],
                          data= [[1,'draft','London','season',1,'January 2019',20, 8.81],
                                 [1,'draft','London','season',2,'January 2019',20, 9.81]]
                          )
    
    #print(testdf['Load_ID'])

    #senddata(testdf)
    #senddata2()

    getsql()

def senddata2():
    engine = create_engine('mssql://AZORRDWSC01/ORR_DW?trusted_connection=yes')




def senddata(df):
    connection = pypyodbc.connect(
            #new DW
            'Driver={SQL Server};'
            'Server=AZORRDWSC01;'
            'Database=ORR_DW;'
            'uid=Live_SQLadmin;pwd=OrrCube2014'
    )

    print("connection established\n")
    cursor = connection.cursor()
    print("Trying to insert data\n")


    # this doesn't work - can't handle the parameters
    for index,rows in df.iterrows():
        print(index)
        print(rows)
        cursor.execute(
            """INSERT INTO [ORR_DW].[NETL].[factt_205_annual_Fares_Index_stat_release_testing]
                ([Load_ID]
                ,[Publication_status]
                ,[Sector]
                ,[Ticket category]
                ,[ordering value of ticket category]
                ,[Year & stats]
                ,[ordering of year & stats]
                ,[value])
                Values
                (2,
                'Published',
                'London',
                'Season',
                1,
                'January 2019',
                20,
                8.77) """
                )
            

        cursor.commit()
    connection.close()

    




def gettext(fieldlist, tablename, search_term):
    """
    This function uses a odbc connection to connect to the ORR_DW database and extract data
    
    Parameters:
    - fieldlist: A string containing the fields to be extracted from the database table.
    - tablename: A string containing the table to be extracted from the warehouse.
    - search terms: A string containing the elements of the WHERE clause, filtering the return set
    Returns:
    - extract: A list of tuples, outer list is a row, inner tuples are each field.  Text fields are broken into separate strings.
     

    """
    extract = list()
    
    connection = pypyodbc.connect(
            #new DW
            'Driver={SQL Server};'
            'Server=AZORRDWSC01;'
            'Database=ORR_DW;'
            'uid=Live_SQLadmin;pwd=OrrCube2014'
    )
    print("this is connection\n")
    print(type(connection))
    #old DW
    #"Driver={SQL Server};"
    #"Server=192.168.10.26;"
    
    #"Database=ORR_DW;"
    #"uid=Live_SQLadmin;pwd=OrrCube2014"



    print("Connecting to ORR_DW: 192.168.10.26")
    cursor = connection.cursor()

    print("setting the SQL Command...... \n")
    SQLCommand = ("SELECT " + fieldlist + " FROM " + tablename + " WHERE " + search_term)

    
    cursor.execute(SQLCommand)
    print("Executing the command: \n \n" + SQLCommand + "\n")

    popcount = 0
    while True:
        row = cursor.fetchone()
        if row is None:
            break
    
        #convert row object into a list object
        extract.append(row)

        print(f"Appending {popcount} row of data", end = '\r')
        popcount += 1

    connection.close()
    print("closed connection ")
    print(f"{popcount} rows appended \n")
    print(f"{len(extract)} rows in extract \n")

    return (extract)


def getsql():
    """
    Dummy function which attempts to implement sql alchemy method of getting data from database:

    """
    #create MS SQL connection engine
    engine = create_engine('mssql+pyodbc://Live_SQLadmin:OrrCube2014@AZORRDWSC01/ORR_DW?driver={SQL Server}')
    
    #create metadata object
    metadata = MetaData()

    #create connection
    connection = engine.connect()

    #printlist of tables here
    print(engine.table_names())

    FI = Table('dimt_date',
  #  FI = Table('[ORR_DW].[NETL].[factt_205_annual_Fares_Index_stat_release]',
               metadata,#this variable holds the table metadata
               autoload=True,
               autoload_with=engine)

    #see the details of this new table
    print("metadata of FI table")
    print(repr(FI))

    #print column names
    print("Columns of the FI table")
    print(repr(metadata.tables['FI']))

    #select all
    query = select([FI])

    #select where
    query = query.where(FI.columns.Publication_status=='Published' )

    results = connection.execute(query).fetchall()
    
    print(metadata)
    #print out the resultset
    #for result in results:
    #    print(result.Load_ID,result.Publication_status,result.Sector,result.'Ticket category',result.'ordering value of ticket category',result.'Year & state',result.'ordering of year & stats',result.value)


if __name__ == '__main__':
    main()

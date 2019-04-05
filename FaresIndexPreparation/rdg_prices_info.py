import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
from commonfunctions import exportfile
import xlsxwriter
import re


def get_rdg_prices_info(infilepath,infilename,outfilepath,outfilename,year,excludeflowid = False):
    """
    This procedure gets the RDG .txt file, splits it into flow and fare_price information dataframes, combines them into 
    a joined csv file, which has a lookup to add LENNON ticket codes for later use in the LENNON-based superfile.

    Parameters:
    infilepath      - a string containing the filepath location of the RDG file
    infilename      - a string containing the filename of the RDG file
    outfilepath     - a string containing the destination of the combined file
    outfilename     - a string containing the file name of the combined file
    year            - a string representing the year the prices info relates to
    excludeflowid   - a boolean representing whether duplicated flow ids should be excluded or not

    Returns:
    combined_data   - a dataframe containing the confirmed prices data.
    """
    
    print(f"getting RDG prices data for {year} \n ")
    flow_list, fare_list = getdata(infilepath,infilename)
    
    print("splitting the data into flow and fares \n")
    flow_df, fares_df = splitter(flow_list, fare_list)


    #joining the flow and fares information
    print("joining flow and fares information\n")
    combined_data = pd.merge(flow_df,fares_df, on='FLOW_ID')
    combined_data.reset_index(drop=True, inplace=True)
    combined_data.index.name="FLOW_AND_FARES_INDEX"


    #add the filter for given year for flow_id to remove duplicate flow id information
    combined_data_no_duplicates = removeRDGduplicates(combined_data, year,excludeflowid)

    #reading in the lookup value for the LENNON codes lookup
    lookupinfo = pd.read_excel(infilepath +'Lennon_product_codes_and_Fares_ticket_types_2017.xlsx','Fares to Lennon coding')
    
    ##join lookupinfo with Lennon keys
    combined_data_with_lennon = pd.merge(combined_data_no_duplicates,lookupinfo,'left',left_on=['TICKET_CODE'],right_on=['Fares ticket type code'])

    # remove duplicates where fares are the same
    combined_data_with_lennon.drop_duplicates(subset=['ORIGIN_CODE','DESTINATION_CODE','ROUTE_CODE','TICKET_CODE','FARE'],keep='first',inplace=True)
    
    #flag up duplicates where fares are different
    flowandfaresduplicateflag = combined_data_with_lennon.duplicated(subset=['ORIGIN_CODE','DESTINATION_CODE','ROUTE_CODE','TICKET_CODE'],keep=False)
    duplicateswithdifferentfares = combined_data_with_lennon[flowandfaresduplicateflag]
    exportfile(duplicateswithdifferentfares,outfilepath,"Duplicates with different fares in flow and fares file for_" + year)

    ##return the completed file
    return combined_data_with_lennon


def getdata(filepath, filename):
    """
    This is a simple reading of a text file, using a context handler.  
    There is a test to exclude commentlines ('/!!') 
    and to split the file into two streams to reflect there are two distinct datasets within the file

    Parameters:
    -filepath: string giving the filepath of the required file
    -filename: string givin the name and file extension of the file to be loaded
    
    Returns:
    -dataset1: a list of lists containing dataset 1 (Train flow data)
    -dataset2: a list of lists containing dataset 2 (Fares data)
    """
    datasetlist1 = list()
    datasetlist2 = list()

    counter = 0
    with open(filepath +filename, newline='\n') as file:
        for line in file:
            if "/!!" in line:
                continue
            
            else:
                print(f"adding another row of data: {counter} rows added", end='\r')
                counter += 1
                if line[0:2] == 'RF':
                    datasetlist1.append(line)
                else:
                    datasetlist2.append(line)
  
        return datasetlist1, datasetlist2



    
def splitter (data1, data2):
    """
    This function splits two lists of lists by indices and converts them to a data frame  
   
    Parameters:
    - data1: A list of lists containing raw flow data
    - data2: a list of lists containing raw fare data
    
    Returns:
    -dataset1: a dataframe containing parsed flow data
    -dataset2: a dataframe containing parsed fare data
    """
    flow_data = list()
    fare_record_data = list()

    for line in data1:
        line = [line[2:6],line[6:10],line[10:15],line[15:18],line[18],line[19],line[36:39],line[42:49]]
        flow_data.append(line)

    flow = pd.DataFrame(flow_data, columns=["ORIGIN_CODE","DESTINATION_CODE","ROUTE_CODE","STATUS_CODE","USAGE_CODE","DIRECTION","TOC","FLOW_ID"])
    flow['ROUTE_CODE'] = flow['ROUTE_CODE'].astype(object)
    flow.index.name="flow_idx"

    for line in data2:
        line=[line[2:9],line[9:12],line[12:20]]
        fare_record_data.append(line)

    fare_record = pd.DataFrame(fare_record_data, columns=["FLOW_ID","TICKET_CODE","FARE"])
    fare_record.index.name = "fare_record_idx"

    return flow,fare_record



def addRDGfaresinfo(df,lookupdf,postfix):
    """
    This procedure joins the RDG data to the superfile on a common key of origin, destination, route and product codes.  it also drops columns that aren't needed and renames columns.

    Parameters:
    df          - A dataframe containing the superfile data
    lookupdf    - A dataframe containing the RDG data with flow and fares information
    postfix     - A string containing a value to be added to the column header

    Returns:
    df          - A conformed data frame with RDG data
    """
    #copy of data frame made to avoid SettingWithCopyWarning by making the copy explicit
    df_dt = df.copy()


    df_dt.loc[:,'Origin Code'] = df.loc[:,'Origin Code'].astype(str)
    df_dt.loc[:,'Destination Code'] = df.loc[:,'Destination Code'].astype(str)
    df_dt.loc[:,'Route Code'] = df.loc[:,'Route Code'].astype(str)
    df_dt.loc[:,'Product Code'] = df.loc[:,'Product Code'].astype(str)

    #print("lookup info from addRDG\n")
    #print(lookupdf.info())
    df_dt = pd.merge(left=df_dt,right=lookupdf[['ORIGIN_CODE','DESTINATION_CODE','ROUTE_CODE','Lennon product code (CTOT)','FARE']],
                         how='left',
                         left_on=['Origin Code','Destination Code','Route Code','Product Code'],
                         right_on=['ORIGIN_CODE','DESTINATION_CODE','ROUTE_CODE','Lennon product code (CTOT)'])


    df_dt.drop(['ORIGIN_CODE','DESTINATION_CODE','ROUTE_CODE','Lennon product code (CTOT)'],axis=1,inplace=True)


    df_dt.rename(columns={'FARE':'RDG_FARES'+postfix,'Fares ticket type description':'RDG_Fares ticket type description'+postfix},inplace=True)

    return df_dt

def removeRDGduplicates(df, yr, excludeflowid):
    """
    This removes rows from RDG data where the origin_code or destination_code fields include an alphabetical character.  It also removes rows where the flow_id is a given flow_id for that specific year.
    An initial run is required without this code being run to determine what the potential duplicates are.  Peter Moran then uses avantix data to determine which flow_ids should be removed

    Parameters:
    df:                                 A dataframe containing the flow and fares information from the extracted and joined RDG text file.
    yr:                                 A string indicating which year this RDG data relates to.
    excludeflowid:                      A boolean flag for whether flowids should be excluded from this run.

    Returns:
    filtered_fully_and_flow_removed:    A dataframe of RDG data with alpha orgin/dest codes and specified flow_ids excluded

    """
    #remove origin and destination code with any letters
    filtered_by_origin = df[~(df['ORIGIN_CODE'].str.match(r'[a-zA-Z]')) ]

    filtered_fully = filtered_by_origin[~(filtered_by_origin['DESTINATION_CODE'].str.match(r'[A-Za-z]')) ]
    
    if excludeflowid is True:
        #These codes will need to be reviewed each year to see what flow ids need to be removed to avoid duplication of routes with different fares
        # These assignments for flow ids was completed by Peter Moran on 5th April 2019.
        if yr == '2018':
            flowtoremove= ['1141787','1141803','1141932','1141947','1141783','1141784','1141838','1141844','1141876','1141975','1141992']

            # potential duplicates for 2018 which are left in the RDG data
            # ['1142117','1142105','1141786','1141802','1141803','1142076']
            filtered_fully_and_flow_removed =   filtered_fully[~(filtered_fully['FLOW_ID'].isin(flowtoremove))]

        elif yr == '2019':
            flowtoremove= ['0140673','0666616','0140777','0666379','0138003','0666568','0138319','0666480','1141844','0666625','0140657','0666552','0140803','0666579','0138170','0138060','0666449','0138310','0138365']
            filtered_fully_and_flow_removed =   filtered_fully[~(filtered_fully['FLOW_ID'].isin(flowtoremove))]
        else:
            print(f"Check the assignment of the year value for RDG data.  {yr} isn't a valid value.")

    else:
        #set the partially modified dataframe to the returned dataframe
        filtered_fully_and_flow_removed = filtered_fully.copy()
 

    
    
    return filtered_fully_and_flow_removed






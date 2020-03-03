from glob import glob
import pandas as pd
from savReaderWriter import SavReader
from commonfunctions import applydatatypes, exportfile
import os 
import numpy as np
import re


def generatedata(originpath,destinationpath,regulatedfarespath,categorypath):
    """
    This joins a number of CSV source files into a common dataframe; handles the cases of sub profit centres being used in place of profit centres; 
    maps sectors, ticket_types, classes, regulated status and category information to the combined file.  This is then used to calculated advanced and non-advanced data.

    Parameters:
    originpath          - A string specifying the location of the individual TOC files are found
    destinationpath     - A string specifying the location of where output should be exported
    regulatedfarespath  - A string specifying the location of the lookup file for regulated fares file
    categorypath        - A string specifying the location of the lookup file for category

    Returns:
    superfile           - A dataframe consisting of the combined and mapped data.
    """
    list_of_tocs,filecount = getdata(originpath)

    joinedfile = combinefiles(list_of_tocs,filecount)

    if filecount > 50:
        print("As you are processing a large number of files, this may possibly cause the PC to freeze or crash due to memory issues.\n")  
        print("If this happens, restart the computer, then close down IE, Outlook and any other memory/resource hungry applications and try again.\n")


        
        
    superfile = joinedfile.copy()

    #drop where category_code not starting with 1 or 2
    superfile = superfile[superfile['Product Code'].str.contains('1[A-Z][A-Z][A-Z]|2[A-Z][A-Z][A-Z]',regex=True)]

    #fields to convert to categorical data type
    superfile = applydatatypes(superfile,['Carrier TOC / Third Party Code','Product Code','Product Primary Code'])

    #mapping of lookups starts here
    #mapping of sectors
    print("mapping sectors within superfile\n")
    sector_mapping = {'EK':'Lon SE','HO':'Lon SE','HQ':'Lon SE','HS':'Lon SE','HT':'Lon SE','HU':'Lon SE','HW':'Lon SE','HY':'Lon SE','HZ':'Lon SE','EX':'Lon SE',
                        'EA':'Regional','EI':'Regional','EJ':'Regional','HA':'Regional','HC':'Regional','HD':'Regional','HE':'Regional','HL':'Regional','ES':'Regional',
                        'EC':'Long D','EH':'Long D','HB':'Long D','HF':'Long D','HI':'Long D','HK':'Long D','HM':'Long D'}
    superfile = assignlookupvalues(superfile,'sectors', sector_mapping, "Carrier TOC / Third Party Code",  'sector' ,destinationpath)

    #mapping of tickettypes
    print("mapping ticket types within superfile\n")
    tickettypemapping = {'PG01':'Full','PG05':'Full','PG02':'Reduced','PG03':'Reduced','PG06':'Reduced','PG07':'Reduced','PG04':'Season','PG08':'Season'}
    superfile = assignlookupvalues(superfile,'ticket_type',tickettypemapping,"Product Primary Code",'ticket_type',destinationpath,'Other')
    
    #mapping of ticketclasses
    print("mapping ticket types within superfile\n")
    classmapping = {'1':'1', '2':'2','9':'2'}
    superfile['classreference'] = superfile['Product Code'].str[0]
    superfile = assignlookupvalues(superfile,'class',classmapping,'classreference','class',destinationpath,'2')
    del superfile['classreference']

    print("mapping regulated status within superfile\n")
    #getting the regulated fares lookup to add flag_2 information for faretypes
    superfile = regulatedfarelookup(regulatedfarespath,superfile )
    
    #setting rows as regulated/unregulated fares here   
    superfile = setregulatedfares(superfile,destinationpath)

    #mapping of categories
    print("mapping categories within superfile\n")
    superfile = getcategorylookup(superfile,categorypath,'Product_category_lookup_2019.xlsx',destinationpath)

    #dropping columns no longer needed
    superfile = superfile.drop(['orig','dest','route'], axis=1)

    #apply final superfile datatyping
    superfile = applydatatypes(superfile,['Carrier TOC / Third Party Code','Origin Code','Destination Code','Route Code','Product Code','sector','ticket_type','class','Regulated_Status_Start','Regulated_Status_toc','Regulated_Status_Products','Regulated_Status_exceptions','Regulated_Status_class','Regulated_Status_PCC','Regulated_Status','Category'])

    #export full superfile for later testing of regulated status setting, if needed
    exportfile(superfile,destinationpath,"superfile with full regulated data")
   
    #delete the surplus Regulated status columns
    superfile = superfile.drop(['Regulated_Status_Start','Regulated_Status_toc','Regulated_Status_Products','Regulated_Status_exceptions','Regulated_Status_class','Regulated_Status_PCC'], axis=1)

    #producing distinct list of product codes with their assigned regulated status
    regulatedcheck = superfile[['Product Code','Product Primary Code','Regulated_Status']].drop_duplicates()
    exportfile(regulatedcheck,destinationpath,"regulated products check")

    return superfile


def getdata(originfilepath):
    """
    This procedure reads in a series of csv files into a dataframe, using the glob method.
    It also removes currency and spacing commas and adds trailing zeros to key lookupfields

    Parameter:
    originfilepath     - a string containing the filepath where the files are stored

    Returns:
    dataframe          - a list containing a dataframe for each individual csv loaded in
    numberoffiles      - an int with the number file files held on the dataframe list 
    """
    filepathsandnames = glob(f'{originfilepath}*.*')
    numberoffiles = len(filepathsandnames)
    
    # printout names of the files to be loaded
    print(f"{numberoffiles} files need to be processed. \n")   
    print(f"reading in CSV files from {originfilepath}\n\n")

    dataframes = []
    #define data types according to the new RDG field names
    dtypedictionary = {'carrier_toc_code':str,'origin_code':str,  'destination_code':str, 'route_code':str, 'product_code':str,'pro_group_1_code':str,'adjusted_earnings':str,'operating_journeys':str}
    for count, file in enumerate(filepathsandnames,1):
        print(f"Loading {os.path.basename(file)} into memory.")
        print(f"That's {count} out of {numberoffiles}, or {str(int((count/numberoffiles)*100))} percent loaded.\n")
        temp = pd.read_csv(file,dtype=dtypedictionary,encoding='Windows-1252')
        

        #rename fields from old Tableau format to new RDG format
        temp.rename(columns={"carrier_toc_code":"Carrier TOC / Third Party Code",
                               "origin_code":"Origin Code",
                               "destination_code":"Destination Code",
                               "route_code":"Route Code",
                               "product_code":"Product Code",
                               "pro_group_1_code":"Product Primary Code",
                               "adjusted_earnings":"Adjusted Earnings Amount",
                               "operating_journeys":"Operating Journeys"},inplace=True)

        #remove currency markers and 1000 markers from each toc file
        temp['Adjusted Earnings Amount'] = temp['Adjusted Earnings Amount'].str.replace(',','')
        temp['Adjusted Earnings Amount'] = temp['Adjusted Earnings Amount'].str.replace('£','')
        temp['Operating Journeys'] = temp['Operating Journeys'].str.replace(',','')
        temp['Operating Journeys'] = temp['Operating Journeys'].str.replace('£','')

        temp['Adjusted Earnings Amount'] = temp['Adjusted Earnings Amount'].astype(float)
        temp['Operating Journeys'] = temp['Operating Journeys'].astype(float)

        #add trailing zeros for later LENNON lookups
        temp['Destination Code'] = temp['Destination Code'].str.zfill(4)
        temp['Route Code'] = temp['Route Code'].str.zfill(5)
        temp['Origin Code'] = temp['Origin Code'].str.zfill(4)

        dataframes.append(temp)

    #elegant but sadly redundant
    #dataframes = [pd.read_csv(f) for f in filenames]
    return dataframes, numberoffiles


def combinefiles(toc_list,file_count):
    """
    This procedure take a list of dataframe and combines them into a single dataframe, with a new index
    The non-numerical fields are converted into Categorical data types

    Parameters:
    toc_list        - a list of dataframes containing indivdual toc data 
    file_count      - an int with the total number of dataframes in the list

    Returns:
    tocs            - a single dataframe
    """

    print(f"appending {file_count} files into single datafile.  Please wait\n\n")

    tocs = pd.concat(toc_list, axis=0,ignore_index=True,verify_integrity=True, sort=False)
    tocs.reset_index(drop=True, inplace=True)
    tocs.index.name='id_code'

    return tocs

# This has been superceded by changes to TOC Data format; formerly used in combinefiles.
#def populatenullprofitcentres(df):
#    """
#    This procedure populates blank Carrier TOC / Third Party Codes with 2 char from Carrier Subdivision Code

#    Parameters:
#    df          - A data frame of combined TOC data

#    Return      - A data frame with fully populated Carrier TOC / Third Party Code and no Carrier Subdivision Code
#    """
#    print("manipulating the carrier subdivision code")
#    #converting three letter codes into two letter codes and moving them into Profit centre code
#    df['Carrier Subdivision Code'] = df['Carrier Subdivision Code'].str[:2]
#    df['Carrier TOC / Third Party Code'].fillna(df['Carrier Subdivision Code'],inplace=True)
#    del df['Carrier Subdivision Code']
#    return df



def assignlookupvalues(df,lookuptype, mapping_dictionary,reference_column, column_name,exportpath,ifnullreplace= 'Missing'):
    """
    This procedure performs a lookup against a given column using .map() method and replaces null values with a default value of 'Missing', or another custom values.  
    Where values are set to 'Missing' csv extract is exported, with number of missing values printed to screen.
    This is used for lookups other than regulated_status
    
    Parameters:
    df                  - a dataframe containing all data
    looktype            - a string containing a lable for the lookup
    mapping_dictionary  - a dictionary containing the lookup values for the mapping
    reference_column    - a string containing the value of the column to be lookedup against
    column_name         - a string containing the name of the column to be created
    exportpath          - a string containing the file path for the export of the 
    ifnullreplace       - a string containing the value to replace NaN values with.  Default value set to 'Missing'

    returns:
    df                  - a datatype containing the modified data.

    """
    
    print(f"assigning {lookuptype} values\n")
    #this populates new column with mapped value from the supplied dictionary
    df[column_name] = df.loc[:,reference_column].map(mapping_dictionary)

    #this populates new column with "missing" flag if NULL
    df[column_name].fillna(ifnullreplace,inplace=True)

    #this converts column into a categorical data type
    df[column_name] = pd.Categorical(df[column_name])
    
    #test of an file with missing values need to be created
    missing_values_count = len(df[df[column_name]=='Missing'].index)

    if missing_values_count > 0:
        formatted_date = datetime.datetime.now().strftime('%Y%m%d_%H-%M')
        filename = f'missing_{lookuptype}_{formatted_date}.csv'

        print(f"missing {missing_values_count} for {lookuptype}")
        print(f"exporting the missing {missing_values_count} rows to {exportpath} as {filename}\n")
        missing_values = df[df[column_name]=='Missing']
        
        missing_values.to_csv(exportpath + filename)
    else:
        print(f"There are no missing {lookuptype} values\n") 
     
    return df

 
def assignedregulatedlookupvalues(df,lookuptype, mapping_dictionary,reference_column, column_name, orig_column_name):
    """
    This procedure performs a lookup against a given column using .map() method.  Existing entries are left alone. 
    This is used solely for the mapping of data to a given regulated/regulated status in a series of steps within the function setregulatedvalues.
    
    Parameters:
    df                  - a dataframe containing all data
    looktype            - a string containing a lable for the lookup
    mapping_dictionary  - a dictionary containing the lookup values for the mapping
    reference_column    - a string containing the value of the column to be lookedup against
    column_name         - a string containing the name of the column to be created
    orig_column_name    - a string containing the name of the previous column to be fill NAs with

    returns:
    df                  - a datatype containing the modified data.

    """
    print(f"assigning {lookuptype} values\n")
    
    df[column_name] = df.loc[:,reference_column].map(mapping_dictionary)
    df[column_name].fillna(df[orig_column_name],inplace=True)

    
    return df


def regulatedfarelookup(lkupfilepath, df):
    """
    This imports a csv file as a source of lookup data for regulated fares.  A merge statement is used to join superfile with the regulated data.  
    Only about 20% of rows will match.
    No match will return NaN in the new fields.

    Parameters:
    lkupfilepath        - a string containing the filepath where the lookup text file is stored
    df                  - a dataframe containing the combined TOC data

    Returns:
    df                  - a dataframe with new fields (orig, dest, route,ticketa, flaga, ticket, flag2) added on the right hand on the dataframe.
    """
    #import fares lookup data
    print("Regulated fares lookup being loaded\n")
    fareslkupdata = pd.read_csv(lkupfilepath + 'regulated fares sort.csv')
    fareslkupvalues = pd.DataFrame(fareslkupdata)


    #data typing for farelookup values
    print("Data typeing fare lookup values\n")
    fareslkupvalues['orig'] = fareslkupvalues['orig'].astype('str')
    fareslkupvalues['dest'] = fareslkupvalues['dest'].astype('str')
    fareslkupvalues['route'] = fareslkupvalues['route'].astype('str')
    fareslkupvalues['ticket'] = fareslkupvalues['ticket'].astype('str')
    
    #set string length by padding fields with leading zeros
    print("Amending lookupvalue keys\n")
    fareslkupvalues['orig'] = fareslkupvalues['orig'].str.zfill(4)
    fareslkupvalues['dest'] = fareslkupvalues['dest'].str.zfill(4)
    fareslkupvalues['route'] = fareslkupvalues['route'].str.zfill(5)
    

    #merging lookup values with superfile
    print("Regulated fares lookup being performed.\n")
    df = pd.merge(left=df,right=fareslkupvalues,how='left',
             left_on=['Origin Code','Destination Code','Route Code', 'Product Code']
             ,right_on=['orig','dest','route','ticket'])

    #drop lookup dataframe no longer needed
    del fareslkupvalues

    # amend datatypes to categorical
    df = applydatatypes(df, ['Product Code','Origin Code','Destination Code','Route Code','orig','dest','route','ticketa','flaga','ticket','flag2'])
    
    return df





def setregulatedfares(df,destinationpath):
    """
    This processes the lookups of exceptional products, normal products and profit centres to create the fields needed  to assign the status of regulated/regulated to fares.
    It also chains these three fields into a logic can creates a new regulated_status field
    It also removes various fields that are no longer needed at the end of processing

    Parameters:
    df:             - a dataframe containg the superfile
    destinationpath - a string with the file path for any error file with missing rows to be sent
    
    output:
    df:             - a modified dataframe with a new field "Regulated_status" and 7 fields removed. 
    """
    
    #created new 'Regulated_Status' column with initial value of "Not Assigned"
    df['Regulated_Status_Start'] = 'Not Assigned'

    #assign values based on TOC Codes
    toc = {'SCR':'Regulated_by_TOC','LER':'Regulated_by_TOC','GWR':'Regulated_by_TOC','GXR':'Regulated_by_TOC','IEC':'Regulated_by_TOC','IWC':'Regulated_by_TOC','LTS':'Regulated_by_TOC'
                ,'MML':'Regulated_by_TOC','NCH':'Regulated_by_TOC','NLR':'Regulated_by_TOC','NSC':'Regulated_by_TOC','NSE':'Regulated_by_TOC','SWT':'Regulated_by_TOC','NIW':'Regulated_by_TOC'
                ,'RCL':'Regulated_by_TOC','RMS':'Regulated_by_TOC','RWB':'Regulated_by_TOC','TPE':'Regulated_by_TOC','HUL':'Regulated_by_TOC','IXC':'Regulated_by_TOC','FCC':'Regulated_by_TOC','NTH':'Regulated_by_TOC'
                ,'GWA':'Regulated_by_TOC','GCR':'Regulated_by_TOC','RNE':'Regulated_by_TOC','RNW':'Regulated_by_TOC','PDR':'Regulated_by_TOC','PSV':'Regulated_by_TOC','PWS':'Regulated_by_TOC','PGM':'Regulated_by_TOC'
                ,'PMR':'Regulated_by_TOC','PSC':'Regulated_by_TOC','PSY':'Regulated_by_TOC','PTW':'Regulated_by_TOC','PWM':'Regulated_by_TOC','DBA':'Regulated_by_TOC'
               ,'NFD':'Regulated_by_TOC','LRC':'Regulated_by_TOC','LBR':'Regulated_by_TOC','SMR':'Regulated_by_TOC'
               ,'ANG':'Regulated_by_TOC', 'GER':'Regulated_by_TOC','NTT':'Regulated_by_TOC','RCV':'Regulated_by_TOC','RWW':'Regulated_by_TOC','TLK':'Regulated_by_TOC','WGN':'Regulated_by_TOC'}
    
    df = assignedregulatedlookupvalues(df,'Regulation by TOCs',toc,'flag2','Regulated_Status_toc','Regulated_Status_Start')

    #assign values based on Product Codes
    products = {'2MTA':'Regulated_by_Product','2MTB':'Regulated_by_Product','2MTE':'Regulated_by_Product',
                '2MTJ':'Regulated_by_Product','2MTK':'Regulated_by_Product','2MTN':'Regulated_by_Product','2VQA':'Regulated_by_Product',
                '2MQA':'Regulated_by_Product', '2MSA':'Regulated_by_Product', '2MSH':'Regulated_by_Product', '2MSL':'Regulated_by_Product', '2MSW':'Regulated_by_Product', '2MTH':'Regulated_by_Product',
                '2MTL':'Regulated_by_Product', '2MTW':'Regulated_by_Product', '2CGE':'Regulated_by_Product','2CGJ':'Regulated_by_Product', '2CGK':'Regulated_by_Product', '2CGN':'Regulated_by_Product',
                '2CGO':'Regulated_by_Product', '2CGS':'Regulated_by_Product', '2CGT':'Regulated_by_Product', '2OBC':'Regulated_by_Product', '2OBD':'Regulated_by_Product', '2OBE':'Regulated_by_Product',
                '2OBF':'Regulated_by_Product', '2OCH':'Regulated_by_Product', '2OCI':'Regulated_by_Product', '2OCJ':'Regulated_by_Product', '2OCL':'Regulated_by_Product', '2OCN':'Regulated_by_Product',
                '2OEH':'Regulated_by_Product', '2CIC':'Regulated_by_Product', '2CID':'Regulated_by_Product', '2CIE':'Regulated_by_Product', '2CIF':'Regulated_by_Product'}
    
    df = assignedregulatedlookupvalues(df,'Regulation by Product',products,'Product Code','Regulated_Status_Products','Regulated_Status_toc')

    #assign values based on an exceptional products mapping
    regulatedexception = {'2ADA':'Unregulated_by_Product','2BDY':'Unregulated_by_Product'}
    df = assignedregulatedlookupvalues(df,'Regulation by Exception',regulatedexception,'Product Code','Regulated_Status_exceptions','Regulated_Status_Products')

    #assign values based on class
    unregulatedclass = {'1':'Unregulated_by_Class'}
    df = assignedregulatedlookupvalues(df,'Regulation by Class',unregulatedclass,'class','Regulated_Status_class','Regulated_Status_exceptions')
    
    #assign values based on Profit Centre Code
    regulatedprofitcentrecode = {'EC':'Unregulated_by_PCC','HM':'Unregulated_by_PCC'}
    df = assignedregulatedlookupvalues(df,'Regulation by Profit Centre',regulatedprofitcentrecode,'Carrier TOC / Third Party Code','Regulated_Status_PCC','Regulated_Status_class')

    #create final copy of regulated column
    df['Regulated_Status'] = df['Regulated_Status_PCC']
    
    #convert the final column from intermediate labels to final labels
    df['Regulated_Status'].replace('Not Assigned','Unregulated', inplace=True)
    df['Regulated_Status'].replace('Unregulated_by_Product','Unregulated', inplace=True)
    df['Regulated_Status'].replace('Unregulated_by_Class','Unregulated', inplace=True)
    df['Regulated_Status'].replace('Unregulated_by_PCC','Unregulated', inplace=True)
    df['Regulated_Status'].replace('Regulated_by_TOC','Regulated', inplace=True)
    df['Regulated_Status'].replace('Regulated_by_Product','Regulated', inplace=True)


    #drop fields no longer required
    del df['ticketa']
    del df['flaga']
    del df['ticket']
    del df['flag2']

    # Amend the relevant data types
    df = applydatatypes(df,['Regulated_Status_Start',
                            'Regulated_Status_toc',
                            'Regulated_Status_Products',
                            'Regulated_Status_exceptions',
                            'Regulated_Status_class',
                            
                            'Regulated_Status'])
    return df

# this is legacy code relating to SPSS which has been replaced by Excel as a lookup source
#def getcategorylookup_spss(df,filepath, filename,destinationpath):
#    """
#    This procedure loads category lookup data from SPSS/SAV file, conforms the format of fields to be return and joins it to the main file.  Non-matches are categorised as 'Missing' and 
#    these rows are exported to a logging file.  Categorical datatyping is applied to new fields.

#    Parameters:
#    df                  - a dataframe containing the 'superfile'
#    filepath            - a string containing the file path for the lookup sav/spss file
#    filename            - a string containing the name of the sav/spss file
#    destinationpath     - a string containing the file path for the output to be sent

#    Returns:
#    df                  - a dataframe containing the 'superfile' with categpory information
#    """
#    print("Category Lookup Data being loaded \n")
#    with SavReader(filepath + filename,ioUtf8=True) as reader:
#        records = reader.all()

#    savtodf = pd.DataFrame(records)

#    #define column names
#    savtodf.columns = ['Product Code','Product Primary Code','Category']
    
#    #force all categories to lower case
#    savtodf['Category'] = savtodf['Category'].str.lower()
    
#    #replace 'off-peak' with 'offpeak'
#    savtodf['Category'] = savtodf['Category'].str.replace('off-peak','offpeak')
        

#    print("Category Information being added")
#    df = pd.merge(df,savtodf,how='left', left_on=['Product Code','Product Primary Code'],right_on=['Product Code','Product Primary Code'])

#    df['Category'].fillna('Missing',inplace=True)
#    nonmatches = df[df['Category']=='Missing']
    
#    #formatted_date = datetime.datetime.now().strftime('%Y%m%d_%H-%M')
#    #filename = f'missing_categories_{formatted_date}.csv'

#    filtered_nonmatches = nonmatches[['Carrier TOC / Third Party Code','Product Code','Product Primary Code']].copy()
#    print(filtered_nonmatches.info())
#    unique_filtered_nonmatches = filtered_nonmatches.unique()
#    exportfile(unique_filtered_nonmatches,destinationpath, 'missing_categories')

#    df = applydatatypes(df,['Product Code','Product Primary Code','Category'])
#    del savtodf

#    return df

def getcategorylookup(df,filepath, filename,destinationpath):
    """
    This procedure loads category lookup data from excel file, conforms the format of fields to be return and joins it to the main file.  Non-matches are categorised as 'Missing' and 
    these rows are exported to a logging file.  Categorical datatyping is applied to new fields.

    Parameters:
    df                  - a dataframe containing the 'superfile'
    filepath            - a string containing the file path for the excel file
    filename            - a string containing the name of the excel file
    destinationpath     - a string containing the file path for the output to be sent

    Returns:
    df                  - a dataframe containing the 'superfile' with category information
    """
    print("Category Lookup Data being loaded \n")
    records = pd.read_excel(filepath + filename,sheet_name='Sheet1',heading=['Product Code','Product Primary Code','Category'])

    savtodf = records

    #define column names
    savtodf.columns = ['Product Code','Product Primary Code','Category']
    
    #force all categories to lower case
    savtodf['Category'] = savtodf['Category'].str.lower()

    print("Category Information being added\n")
    df = pd.merge(df,savtodf,how='left', left_on=['Product Code','Product Primary Code'],right_on=['Product Code','Product Primary Code'])

    #handle missing category information
    df['Category'].fillna('Missing',inplace=True)
    nonmatches = df[df['Category']=='Missing']
    unique_filtered_nonmatches = nonmatches[['Product Code','Product Primary Code']].copy().drop_duplicates()
    exportfile(unique_filtered_nonmatches,destinationpath, 'missing_categories')

    #apply datatyping
    df = applydatatypes(df,['Product Code','Product Primary Code','Category'])
    
    #remove unnecessary column
    del savtodf

    return df

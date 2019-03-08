import numpy as np
import pandas as pd
import datetime


def handlezeroandnulls(df):
    #replace the 'inf' and '-inf' value with NAN
    df.replace([np.inf, -np.inf],np.nan,inplace=True)   

    #replace the zeros in fares2017 and fares2018 with NAN
    df.replace({'FARES_2017':0,'FARES_2018':0},np.nan,inplace=True)
    
    #drop the rows where FARES are NAN
    df.dropna(axis='index',subset=['FARES_2017','FARES_2018'],how='any',inplace=True)

    return df


def percentagechange(df,col1,col2):
    #calculate percentage change for each row
    print("Adding percentage change info\n")
    df.loc[:,'percentage_change'] = (((df.loc[:,col1]-df.loc[:,col2])/df.loc[:,col2])*100).copy()

    return df



def applydatatypes(combinedfile,col_headers):
    """
    This procedure manipulates the data frame of combined toc data
    - populates blank Carrier Profit Centre Codes with 2 char from Carrier Subdivision Code
    - applies categorical datatyping to non-numeric columns, taken from a list of column names

    Parameters:
    combinedfile        - A dataframe containing all toc data
    col_headers         - A list containing names of columns to be coverted to categorical

    Returns:
    combined file       - A dataframe containing category datatypes
    """
    
    print("manipulating data types\n")
    #col_headers = list(combinedfile.columns.values)

    #datatype definition
    for col in col_headers:
        print(f"typing {col} as categorical data type\n")
        combinedfile[col] = pd.Categorical(combinedfile[col])

    return combinedfile

def writeoutinfo(df,fileoutput,filename):
    f = open(fileoutput + filename, 'w+')
    df.info(buf=f)
    f.close()


def exportfile(df,destinationpath,filename,numberoffiles=1):
    """
    This procedure exports the finalised file as a CSV file with a datetime stamp in filename

    Parameters:
    df        - a dataframe containing the finalised data
    destinationpath     - a string providing the filepath for the csv file
    numberoffiles       - an int with the number of files being processed
    
    Returns:
    None, but does export dataframe df as a csv object
    """
     
    formatted_date = datetime.datetime.now().strftime('%Y%m%d_%H-%M')
    destinationfilename = f'{filename}_{formatted_date}.csv'
    print(f"Exporting {filename} to {destinationpath}{destinationfilename}\n")
    checkmessage = "If you want to check on progress, refresh the folder "+ destinationpath + " and check the size of the " + filename + ".csv file. \n"  

    if filename == 'superfile':
        if numberoffiles < 9:
            print("This is just testing so will be quick")
            print(checkmessage)
    
        elif numberoffiles > 10 and numberoffiles < 29:
            print("This may take a few minutes.  Why not go and have a nice cup of tea?\n")
            print(checkmessage)

        elif numberoffiles > 30:
            print("This may possibly hang the PC due to memory issues.  If it hangs, turn off IE, Outlook and any other memory/resource hungry applications and try again.\n")
            print(checkmessage)

        else:
            pass
    else:
        print(f"the {filename} file should be quick.")
   
    df.to_csv(destinationpath + destinationfilename)

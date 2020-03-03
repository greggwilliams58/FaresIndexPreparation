import numpy as np
import pandas as pd
import datetime


def handlezeroandnulls(df):
    """
    This procedure:
    identifies "inf" and "-inf" values (results of division by zero), converts them to NaN
    identifies 0 values (where no fares data is present), converts them to NaN
    rows containing NaN are then dropped

    Parameters:
    df:     A dataframe containing LENNON/RDG Fares information

    Returns:
    df:     A dataframe with zeros/NULLS removed

    """

    #replace the 'inf' and '-inf' value with NAN
    df.replace([np.inf, -np.inf],np.nan,inplace=True)   

    #replace the zeros in fares2019 and fares2020 with NAN
    df.replace({'FARES_2019':0,'FARES_2020':0},np.nan,inplace=True)
    
    #drop the rows where FARES are NAN
    df.dropna(axis='index',subset=['FARES_2019','FARES_2020'],how='any',inplace=True)

    return df


def percentagechange(df,col1,col2):
    """
    This procedure:
    calculates precentage changes

    Parameters:
    df:     A dataframe containing two numerical fields
    col1:   A string with the name of the first column
    col2:   A string with the name of the second column

    Returns:
    df:     A dataframe with a new column displaying percentage change as percentage increase.
    """

    #calculate percentage change for each row
    print("Adding percentage change info\n")
    df.loc[:,'percentage_change'] = (((df.loc[:,col1]-df.loc[:,col2])/df.loc[:,col2])*100).copy()

    return df



def applydatatypes(combinedfile,col_headers):
    """
    This procedure manipulates the data frame of combined toc data
    - populates blank carrier_toc_codes with 2 char from Carrier Subdivision Code
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
    """
    This procedure is used for diagnostic purposes and writes out dataframe info as a text file.

    Parameters:
    df:         A generic dataframe
    fileoutput: A string containing the filepath where the output will be saved
    filename:   A string containing the name of the file to be output.

    Returns:
    None:       But does export a text file containing info about the dataframe
    """
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
    print(f"If you want to check on progress, refresh the folder "+ destinationpath + " and check the size of the " + filename + ".csv file. \n")  
    df.to_csv(destinationpath + destinationfilename)

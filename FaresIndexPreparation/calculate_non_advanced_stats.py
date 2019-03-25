from toc_file_processing import generatedata
#from toc_file_processing import exportfile
from append_revised_data import appenddata
from commonfunctions import exportfile
import pandas as pd
#import numpy as np
#import csv


def main():
    filepath = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexOutput\\'
    filename = 'nonadvancedfile_noavantix_20190131_14-56.csv'
    manualfilepath = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\Manually_checked_data\\'


    print(f" getting source file called '{filename}'. \n from {filepath} \n Please wait\n\n\n")
    #rawdata = getsourcefile(filepath,filename)
    validdata = getsourcefile(filepath,filename)

    rowcount = validdata.shape[0]
    print("Loaded {0:,} rows into memory\n".format(rowcount))

    #print(rawdata.info())
    print(validdata.shape)

    
    ##place holder for adding big_change_data
    #rawdata_and_big = appenddata(Big_change_data\\','big_change_file_revised.csv', rawdata)
    
    ##place holder for adding little_change_data
    #rawdata_and_little = appenddata(manualfilepath + 'Little_change_data\\','little_change_file_revised.csv', rawdata_and_big)
    
    ##place holder for adding big earner data
    #validdata = (manualfilepath + 'Big_earner_data\\','big_earners_file_revised.csv',rawdata_and_little)


    #multiplication of weights by percentage_change
    validdata.loc[:,'factor'] = validdata['Weightings']*validdata['percentage_change']

    #sum of all weights
    allweights = validdata['Weightings'].sum()
    print(f"All weights are {allweights}\n")

    #sum of all percentages
    #allpercentages = rawdata['percentage_change'].sum()
    #print(f"All percentages are {allpercentages}\n")

    #general splits here
    print(calc_weighted_average_price_change(validdata,['sector'],allweights,'all'))
    print("\n")
    print(calc_weighted_average_price_change(validdata,['class'],allweights,'all'))
    print("\n")
    print(calc_weighted_average_price_change(validdata,['Regulated_Status'],allweights,'all'))
    missingstatusrevenue = validdata.loc[validdata['Regulated_Status']=='not assigned','Weightings'].sum()
    print(f"The value of missing status revenue is £{'{:,}'.format(int(missingstatusrevenue))}")

    supersplit = calc_weighted_average_price_change(validdata,['sector','class','Regulated_Status'],allweights,'supersplit')
    print(f"The supersplit is {supersplit}")

    print("\n")
    print(calc_weighted_average_price_change(validdata,['Category'],allweights,'all'))
    missingcategoryrevenue = validdata.loc[validdata['Category']=='Missing','Weightings'].sum()
    print(f"The value of missing revenue is £{'{:,}'.format(int(missingcategoryrevenue))}") 
    print("\n")
    
    avgpricechange = validdata['factor'].agg('sum')/validdata['Weightings'].agg('sum')
    print(f"The average overall non-advanced price change is {avgpricechange}") 
    #print(advanceddata.info())
    #print(advanceddata.head(10))


    exportfile(validdata,filepath,'non_advanced_calculations')



def getsourcefile(path,filename):
    """
    This function is a place holder to get the data from a csv file for the wider calculation of this code.  
    This will require the original data from 'toc_file_processing' to have avantix data supplementing the files bigchange, littlechange, bigearners 
    which is then appended to the export file. 
    
    Parameters:
    path            - a string representing the file path where the source file is held
    filename        - a string representing the file to be loaded

    Returns:
    populateddata   - a dataframe holding the populated data
    """
    #placeholder for value returned by generatedata()
    #populateddata = generatedata()

    datatyping ={'Carrier Profit Centre Code':'category', 'Origin Code':'category','Destination Code':'category','Route Code':'category','Product Code':'category'
                 ,'Product Level 1 Code':'category','sector':'category','ticket_type':'category','class':'category','Regulated_Status':'category','Category':'category'}
    
    populateddata = pd.read_csv(path + filename,dtype=datatyping)


    return populateddata


#def calc_weighted_average_price_change(df,grouping,weighting,status):
#    """
#    This function sums the factor column and then divides the result by all weights.  It also prints a status message to console.

#    Parameters:
#    df              - A dataframe containing the global dataset
#    grouping        - A string representing the column to group the data by
#    weighting       - A int64 representing the sum of all weights
#    status          - A string representing the status of the global data

#    Returns:
#    avgpricechange  - A dataframe containing results of calculation
#    """
    
#    print(f"\nThese are {grouping} splits by {status}")
#    avgpricechange = (df.groupby(grouping)['factor'].agg('sum'))/(df.groupby(grouping)['Weightings'].agg('sum'))
#    return avgpricechange


if __name__  == '__main__':
    main()
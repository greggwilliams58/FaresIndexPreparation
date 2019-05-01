from toc_file_processing import getdata
from commonfunctions import exportfile
import pandas as pd


def main():
    """
    This was written at Peter Moran's request to perform a comparison between the old and new "regulated fares sort" files.  It was used once and is now kept in case a further update
    to this file is needed on an adhoc basis.

    Parameters
    None:   but inmports is 
    """
    
    lookupslocation = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\regulated_fares_data\\'
    destination = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\regulated_fares_data\\comparison output\\'
    lookupfileslist, count = getdata(lookupslocation)

    print(f"there are {count} files found.")

    newlookup = lookupfileslist[0]
    oldlookup = lookupfileslist[1]

    #join new to old // old to new
    new_uniquevalues = pd.merge(left=newlookup,right=oldlookup,how='left',
        left_on=['orig','dest','route','ticket'],right_on=['orig','dest','route','ticket'])

    old_uniquevalues = pd.merge(left=newlookup,right=oldlookup,how='right',
        left_on=['orig','dest','route','ticket'],right_on=['orig','dest','route','ticket'])

    print("These are values unique to new lookup")  
    new_uniquevalues = new_uniquevalues[new_uniquevalues.ticketa.isnull()==True]
    exportfile(new_uniquevalues,destination,'unique_new_values',1)

    print("These are values unique to old lookup")
    old_uniquevalues = old_uniquevalues[old_uniquevalues.new_flag.isnull()==True]
    exportfile(old_uniquevalues,destination,'unique_old_values',1)



def getreglkupdata(originfilepath):
    filepathsandnames = glob(f'{originfilepath}*.*')
    numberoffiles = len(filepathsandnames)
    
    print(f"{numberoffiles} files need to be processed. \n")   # printout names of the files to be loaded
    print(f"reading in CSV files from {originfilepath}\n\n")

    dataframes = []
   
    for count, file in enumerate(filepathsandnames,1):
        dataframes.append(temp)


    return dataframes, numberoffiles








if __name__  == '__main__':
    main()

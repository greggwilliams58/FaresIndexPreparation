from toc_file_processing import getdata
from commonfunctions import exportfile
import pandas as pd


def main():
    lookupslocation = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\regulated_fares_data\\'
    destination = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\regulated_fares_data\\comparison output\\'
    lookupfileslist, count = getdata(lookupslocation)

    print(f"there are {count} files found.")

    newlookup = lookupfileslist[0]
    oldlookup = lookupfileslist[1]

    #print("This is new lookup\n")
    #print(newlookup.info())

    #print("\n")
    #print(oldlookup.info())

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












if __name__  == '__main__':
    main()

import pandas as pd
import datetime 
from pandas import ExcelWriter
from pandas import ExcelFile

#def main():
#    #substitute for proper parameters in a function
#    filelocation = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexOutput\\'
#    outputto = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexOutput\\'#


#    print("getting superfile data\n")
#    finalsuperfile = pd.read_csv(filelocation + 'rawsuperfile_20190405_16-26.csv',
#                               dtype={'Carrier TOC / Third Party Code':'category','Origin Code':'category','Destination Code':'category','Route Code':'category',
#                                      'Product Code':'category','Product Primary Code':'category','class':'category','sector':'category'})
    
#    getrailfinancial(finalsuperfile,outputto)


def getrailfinancial(df,outputlocation):
    """
    This takes the supefile data frame, group and sum by two different levels and exports them to a new excel file for use by RME for producing a section of the annual rail financial return.

    Parameters:
    df:             A dataframe containing the superfile information
    outputlocation: A string indicating where the file should be exported to


    """
    formatted_date = datetime.datetime.now().strftime('%Y%m%d_%H-%M')
    destinationfilename = f'rail_financial_data_{formatted_date}.xlsx'

    revsplitbytocticketreg = df.groupby(['Carrier TOC / Third Party Code','Product Code','Regulated_Status'],as_index=False).agg({'Adjusted Earnings Amount':['sum']})
    revsplitbytocsectorclasscatreg = df.groupby(['Carrier TOC / Third Party Code','sector','class','Category','Regulated_Status'], as_index=False).agg({'Adjusted Earnings Amount':['sum']})

    revsplitbytocticketreg.rename(columns = {'Carrier TOC / Third Party Code':'TOC','Product Code':'Ticket','Regulated_Status':'Reg/Unreg','Adjusted Earnings Amount':'Earnings'},inplace=True)
    revsplitbytocsectorclasscatreg.rename(columns = {'Carrier TOC / Third Party Code':'TOC','sector':'Sector','class':'Class','Category':'Category','Regulated_Status':'Reg/Unreg','Adjusted Earnings Amount':'Earnings'},inplace=True) 

    writer = pd.ExcelWriter(outputlocation + destinationfilename, engine='xlsxwriter')

    revsplitbytocticketreg.to_excel(writer,sheet_name='rail_financial_data')
    revsplitbytocsectorclasscatreg.to_excel(writer,sheet_name='rail_financial_data',startcol=10 )

    writer.save()

#if __name__ == '__main__':
#    main()

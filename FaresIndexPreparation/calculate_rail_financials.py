import pandas as pd
import datetime 
from pandas import ExcelWriter
from pandas import ExcelFile

def getrailfinancial(df,outputlocation):
    """
    This takes the supefile data frame, group and sum by two different levels and exports them to a new excel file for use by RME for producing a section of the annual rail financial return.

    Parameters:
    df:             A dataframe containing the superfile information
    outputlocation: A string indicating where the file should be exported to


    """
    #create filename with date_and_timestamp
    formatted_date = datetime.datetime.now().strftime('%Y%m%d_%H-%M')
    destinationfilename = f'rail_financial_data_{formatted_date}.xlsx'

    # group and sum the superfile by two cuts
    revsplitbytocticketreg = df.groupby(['carrier_toc_code','product_code','Regulated_Status'],as_index=False).agg({'adjusted_earnings':['sum']})
    revsplitbytocsectorclasscatreg = df.groupby(['carrier_toc_code','sector','class','Category','Regulated_Status'], as_index=False).agg({'adjusted_earnings':['sum']})

    # rename columns of the group and summed data
    revsplitbytocticketreg.rename(columns = {'carrier_toc_code':'TOC','product_code':'Ticket','Regulated_Status':'Reg/Unreg','adjusted_earnings':'Earnings'},inplace=True)
    revsplitbytocsectorclasscatreg.rename(columns = {'carrier_toc_code':'TOC','sector':'Sector','class':'Class','Category':'Category','Regulated_Status':'Reg/Unreg','adjusted_earnings':'Earnings'},inplace=True) 

    #prepare excel writer object, export dataframes to two different ranges and save excel file
    writer = pd.ExcelWriter(outputlocation + destinationfilename, engine='xlsxwriter')
    revsplitbytocticketreg.to_excel(writer,sheet_name='rail_financial_data')
    revsplitbytocsectorclasscatreg.to_excel(writer,sheet_name='rail_financial_data',startcol=10 )
    writer.save()

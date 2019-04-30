import pandas as pd
from commonfunctions import applydatatypes, exportfile
#from export_data import exportfile
from calculate_results import calc_weighted_average_price_change, calc_final



def main():
    #substitute for proper parameters in a function
    filelocation = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\advanced_and_non_advanced_output\\'
    outputto = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\advanced_and_non_advanced_output\\adv_non_advanced_and_superfile\\'


    print("getting advanced data\n")
    advanceddata = pd.read_csv(filelocation + 'advancedfile_20190429_09-52.csv',
                               dtype={'Carrier TOC / Third Party Code':'category','Origin Code':'category','Destination Code':'category','Route Code':'category',
                                      'Product Code':'category','Product Primary Code':'category','class':'category','sector':'category'})

    #exportfile(advanceddata.info(),filelocation,"advanced_metadata")
    
    print("Getting non-advanced data\n")
    nonadvanceddata = pd.read_csv(filelocation + 'nonadvancedfile_20190429_10-11.csv',
                                  dtype={'Carrier TOC / Third Party Code':'category','Origin Code':'category','Destination Code':'category','Route Code':'category',
                                      'Product Code':'category','Product Primary Code':'category','class':'category','sector':'category'})

    #exportfile(nonadvanceddata.info(),filelocation,"non_advanced_metadata")
    print("getting superfile for weights")
    rawsuperfile = pd.read_csv(filelocation + 'superfile without regulated steps_20190429_09-45.csv',
                               dtype={'Carrier TOC / Third Party Code':'category','Origin Code':'category','Destination Code':'category','Route Code':'category',
                                      'Product Code':'category','Product Primary Code':'category','class':'category','sector':'category','ticket_type':'category'}
                               )
    ### preparatory work for function signature
    ### appenddata(advanceddata,nonadvanceddata,rawsuperfile)

    print("preparing the superfile...\n")
    preparedsuperfile = preparesuperfile(rawsuperfile)
    #exportfile(preparedsuperfile,outputto,"preparedsuperfile")
    print("data joined.  showing metadata\n")
     
    advandnonadv = appenddata([advanceddata,nonadvanceddata])
    #exportfile(advandnonadv,outputto,'preparedadvandnonadv')



    advandnonadv.sort_values(by=['sector','class','Category','Regulated_Status'],ascending=True,inplace=True)
    preparedsuperfile.sort_values(by=['sector','class','Category','Regulated_Status'],ascending=True,inplace=True)

    ####return(advandnonadv,preparedsuperfile)


    ### move to calculate result from here to end
    answergrid = calc_weighted_average_price_change(advandnonadv,preparedsuperfile,['sector','class','Category','Regulated_Status'])


    #change name of weighted_price_change
    answergrid.rename(columns={answergrid.columns[0]:'weighted_price_change'},inplace=True)

    #remove the group superweights where the sum is zero
    answergrid = answergrid.drop(answergrid[answergrid['Weightings_super'] == 0].index)

    #flatten the answergrid
    answergrid.columns = [''.join(col).strip() for col in answergrid.columns.values]
    answergrid = answergrid.reset_index()

    #wpc * superweightings
    answergrid['wpc_and_weights'] = answergrid['weighted_price_change'] * answergrid['Weightings_super']
    print("this is the answergrid\n")
    #print(answergrid.info())
    exportfile(answergrid,outputto,"answerfile")

    
    sectorsplit = calc_final(answergrid,['sector'],'sector')
    classsplit = calc_final(answergrid,['class'],'class')
    sectorclasssplit = calc_final(answergrid,['sector','class'],'sector and class')
    regulatedstatussplit = calc_final(answergrid,['Regulated_Status'],'regulation')
    categorysplit = calc_final(answergrid,['Category'],'category')
    sectorcategorysplit = calc_final(answergrid,['sector','Category'],'sector and category')
    sectorclassregulatedstatus = calc_final(answergrid,['sector','class','Regulated_Status'],'sector, class and regulation')
    classregulatedstatus = calc_final(answergrid, ['class','Regulated_Status'],'class and regulation')


    combined = pd.concat([sectorsplit,classsplit,sectorclasssplit,regulatedstatussplit,categorysplit,sectorcategorysplit,sectorclassregulatedstatus,classregulatedstatus])
    #combined_df = combined.to_frame()
    #print(type(combined_df))


    exportfile(combined,outputto,"combined")

    #this is to be added to integrate this with the main function
    #return combined
   


def appenddata(nonadvandadv):
    """
    This file joins two dataframes which are presented as a list.  
    A Factor of weightings * percentage change  is calculated
    Unnecessary columns are dropped, others renamed and the remaining columns are reordered
    The advanced and non-advanced datasets are filtered by hard-coded values in line 128

    Parameters
    nonadvandadv    - A list of dataframes representing the advanced and non-advanced datasets to be appended 
    sf              - A data frame containing the raw superfile
    
    Returns
    sourcedata      - a dataframe with appended data
    """
    fileoutputpath ='C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\advanced_and_non_advanced_output\\adv_non_advanced_and_superfile\\'
    
    print("About to combine advanced and non-advanced data\n")
    advanced_and_non_advanced = pd.concat(nonadvandadv,ignore_index=True, sort=False)
    #print(advanced_and_non_advanced.head())


    #populate values for advanced data
    advanced_and_non_advanced['Regulated_Status'].fillna('Unregulated',inplace=True)
    advanced_and_non_advanced['Category'].fillna('advance',inplace=True)

    #function to contain last minute changes to advanced/non-advanced dataframe
    advanced_and_non_advanced = lastminutechanges(advanced_and_non_advanced)
    
    print("calculate factor\n")
    advanced_and_non_advanced.loc[:,'factor'] = advanced_and_non_advanced['Weightings'] * advanced_and_non_advanced['percentage_change']
#    print(advanced_and_non_advanced.info())
    

    #placeholder for Peter's subquery
    subcutofdataLD2seasonUnregulated = advanced_and_non_advanced[(advanced_and_non_advanced['sector'] == 'Long D') & (advanced_and_non_advanced['class'] == '2') & (advanced_and_non_advanced['Category'] == 'season') & (advanced_and_non_advanced['Regulated_Status'] == 'Unregulated')  ]
    exportfile(subcutofdataLD2seasonUnregulated,fileoutputpath,'LD2seasonunregulated')


    #drop unnecessary columns
    columnstodel = ['Unnamed: 0','Carrier TOC / Third Party Code','Origin Code','ticket_type','Destination Code','Route Code','Product Code','Product Primary Code','Operating Journeys','FARES_2018','FARES_2019']
    print("dropping columns")
    advanced_and_non_advanced.drop(columnstodel,axis=1,inplace=True)


    #test for filter
    #exportfile(advanced_and_non_advanced,fileoutputpath,"advandnonadv before filter")
    print("filtering data")
    advanced_and_non_advanced_to_be_filtered = advanced_and_non_advanced.copy()
     #apply filter for upper and lower percentage changes
    advanced_and_non_advanced_filtered = advanced_and_non_advanced_to_be_filtered.query('percentage_change > -20 and percentage_change < 20')
    #exportfile(advanced_and_non_advanced_filtered,fileoutputpath,"advandnonadv after filter")


    #rename weightings column
    print("rename columns")
    advanced_and_non_advanced_filtered_renamed = advanced_and_non_advanced_filtered.rename(columns={'Weightings':'Weightings_advnonadv'})

    print("change order of columns")
    #change order of columns to match combined data
    advanced_and_non_advanced_reordered = advanced_and_non_advanced_filtered_renamed[['sector',	'class',	'Category',	'Regulated_Status','Weightings_advnonadv','factor']]


    return advanced_and_non_advanced_reordered 


def preparesuperfile(superfile):
    """
    This takes the superfile and drops unnecessary columns, renames earnings as 'weightings', re-orders columns and performs a sum-aggregation

    Parameters
    superfile   A dataframe containing the raw superfile

    Returns
    superfile   A dataframe modified as indicated above
    """

    columnstodel = ['Unnamed: 0','Carrier TOC / Third Party Code','Origin Code','Destination Code','Route Code','Operating Journeys','ticket_type']
    superfile.drop(columnstodel,axis=1,inplace=True)


    #Rename column of "Earnings as Weightings"
    superfile.rename(columns={'Adjusted Earnings Amount':'Weightings_super'},inplace=True)

    #change order of columns to match combined data
    #superfile = superfile[['sector',	'class',	'Category',	'Regulated_Status','Weightings_super']]
    
    #temp for peter change order of columns to match combined data
    superfile = superfile[['Product Code','Product Primary Code','sector',	'class',	'Category',	'Regulated_Status','Weightings_super']]

    #temp grouping for peter
    superfile = superfile.groupby(['Product Code','Product Primary Code','sector',	'class',	'Category',	'Regulated_Status']).agg('sum')
    #print(superfile_grouped)
    return superfile


def lastminutechanges(df):
    """
    This is the location to make any last minute changes to the advanced non-advanced dataset prior to calculation of the calculations.
    All origin and destination codes that contain an alphabetical character
    Selected product codes are removed (refunds to season tickets?)

    Parameters
    df:     A dataframe containing the merged advanced and non advanced data

    Returns:
    df:     An amended dataframe
    
    
    """
    fileoutputpath ='C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\advanced_and_non_advanced_output\\adv_non_advanced_and_superfile\\'
    #remove the orgin and destination codes that contain an alphabetic character
    df = df[df['Origin Code'].str.contains('[0-9][0-9][0-9][0-9]')]
    df = df[df['Destination Code'].str.contains('[0-9][0-9][0-9][0-9]')]

    #remove these specific product codes
    productcodestoremove = ['2MTC','2MTD','2MTF','2MTG']
    df = df[~df['Product Code'].isin(productcodestoremove)]

    #export for Nisha
    filtereddf = df[df.Category == 'season']
    groupedadvandnonadvanced = filtereddf.groupby(['Category','Product Code','sector','class','Regulated_Status'])['Weightings','Operating Journeys'].agg('sum')
    exportfile(groupedadvandnonadvanced,fileoutputpath, "advandnonadv grouped")


    return df


if __name__ == '__main__':
    main()
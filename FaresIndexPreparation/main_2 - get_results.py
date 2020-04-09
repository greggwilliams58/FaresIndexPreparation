import pandas as pd
from commonfunctions import applydatatypes, exportfile
from calculate_results import calc_weighted_average_price_change, calc_final
#,calc_final_all



def main():
    """
    This is a second stage of the fares index process which imports the CSV files advanced, nonadvanced and superfile.  The last two files are combined and then joined in term to the superfile.
    Another function mimics the SUMPRODUCT function of excel.  A second function runs over the dataframe from the previous function and them combines the answers to produce the final answerset

    Parameters
    None:       But it does import adv, nonadv and superfile from file locations as CSV and converts them to dataframes

    Returns:
    None:       But it does export a csv file containing the final answerset.
    """

    #define where the files are and where they will go
    filelocation = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\advanced_and_non_advanced_output\\'
    outputto = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\advanced_and_non_advanced_output\\adv_non_advanced_and_superfile\\'
    final_answerto = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\Template_preparation\\'

    print("getting advanced data\n")
    advanceddata = pd.read_csv(filelocation + 'advancedfile_20200409_10-15.csv',
                               dtype={'Carrier TOC / Third Party Code':'category','Origin Code':'category','Destination Code':'category','Route Code':'category',
                                      'Product Code':'category','Product Primary Code':'category','class':'category','sector':'category'})
    
    print("Getting non-advanced data\n")
    nonadvanceddata = pd.read_csv(filelocation + 'nonadvancedfile_20200409_10-41.csv',
                                  dtype={'Carrier TOC / Third Party Code':'category','Origin Code':'category','Destination Code':'category','Route Code':'category',
                                      'Product Code':'category','Product Primary Code':'category','class':'category','sector':'category'})

    print("getting superfile for weights")
    rawsuperfile = pd.read_csv(filelocation + 'superfile without regulated steps_20200409_10-10.csv',
                               dtype={'Carrier TOC / Third Party Code':'category','Origin Code':'category','Destination Code':'category','Route Code':'category',
                                      'Product Code':'category','Product Primary Code':'category','class':'category','sector':'category','ticket_type':'category'})

    print("preparing the superfile...\n")
    preparedsuperfile = preparesuperfile(rawsuperfile)
    
    #join the advanced and nonadvanced data
    print("data joined.  showing metadata\n") 
    advandnonadv = joinadvandnonadv([advanceddata,nonadvanceddata])

    #sort the advandnonadv and superfile by common fields so they match up when paired later
    advandnonadv.sort_values(by=['sector','class','Category','Regulated_Status'],ascending=True,inplace=True)
    preparedsuperfile.sort_values(by=['sector','class','Category','Regulated_Status'],ascending=True,inplace=True)

    #calculate the weighted averages by sector, class, category and regulated_status
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
 
    exportfile(answergrid,outputto,"answerfile")

    #calculate the final set of group splits from the answer file as separate dataframes
    sectorsplit1 = calc_final(answergrid,['sector'],'sector1')
    sectorsplit2 = calc_final(answergrid,['sector'],'sector2')
    classsplit = calc_final(answergrid,['class'],'class')
    sectorclasssplit = calc_final(answergrid,['sector','class'],'sector and class')
    regulatedstatussplit = calc_final(answergrid,['Regulated_Status'],'regulation')
    categorysplit = calc_final(answergrid,['Category'],'category')
    sectorcategorysplit = calc_final(answergrid,['sector','Category'],'sector and category')
    sectorclassregulatedstatus = calc_final(answergrid,['sector','class','Regulated_Status'],'sector, class and regulation')
    classregulatedstatus = calc_final(answergrid, ['class','Regulated_Status'],'class and regulation')



    #create a nosplit calcfinal and add to the list of final answer subsets below
    listoffinalanswersubsetnames = ['sectorsplit1','sectorsplit2', 'classsplit', 'sectorclasssplit' ,'regulatedstatussplit', 'categorysplit','sectorcategorysplit','sectorclassregulatedstatus','classregulatedstatus']
    listoffinalanswersubsets = [sectorsplit1, sectorsplit2,classsplit, sectorclasssplit ,regulatedstatussplit, categorysplit,sectorcategorysplit,sectorclassregulatedstatus,classregulatedstatus ]
    
    dictoffinalanswersubset = dict(zip(listoffinalanswersubsetnames, listoffinalanswersubsets))
    
    
    #combine the group splits as one dataframe
    combined_answers_data = pd.concat([sectorsplit1,sectorsplit2,classsplit,sectorclasssplit,regulatedstatussplit,categorysplit,sectorcategorysplit,sectorclassregulatedstatus,classregulatedstatus])

    #for names,subsets in dictoffinalanswersubset.items():
    #    print(names + "\n")
    #    print(subsets)
    #    print("\n")

    #rename column headers
    combined_answers_data.index.rename("parts_of_the_grouping", inplace=True)
    combined_answers_data.columns = ['grouping_name','average_price_change','superweights','percentage_share_of_superweights_in_grouping']

    #end the process by exporting the final answer
    
    exportfile(combined_answers_data,final_answerto,"final answerset") 
    
    #for names,subsets in dictoffinalanswersubset.items():
    #    exportfile(subsets,final_answerto, names)

   


def joinadvandnonadv(nonadvandadv):
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

    #populate values for advanced data
    advanced_and_non_advanced['Regulated_Status'].fillna('Unregulated',inplace=True)
    advanced_and_non_advanced['Category'].fillna('advance',inplace=True)

    
    print("calculate factor\n")
    advanced_and_non_advanced.loc[:,'factor'] = advanced_and_non_advanced['Weightings'] * advanced_and_non_advanced['percentage_change']
    
    #function to contain last minute changes to advanced/non-advanced dataframe
    advanced_and_non_advanced = lastminutechanges(advanced_and_non_advanced)

    #drop unnecessary columns
    columnstodel = ['Unnamed: 0','Carrier TOC / Third Party Code','Origin Code','ticket_type','Destination Code','Route Code','Product Code','Product Primary Code','Operating Journeys','FARES_2019','FARES_2020']
    print("dropping columns")
    advanced_and_non_advanced.drop(columnstodel,axis=1,inplace=True)

    #filtering data to remove outliers
    print("filtering data\n")
    advanced_and_non_advanced_to_be_filtered = advanced_and_non_advanced.copy()
    advanced_and_non_advanced_filtered = advanced_and_non_advanced_to_be_filtered.query('percentage_change > -20 and percentage_change < 20')

    #rename weightings column
    print("rename eightings columns \n")
    advanced_and_non_advanced_filtered_renamed = advanced_and_non_advanced_filtered.rename(columns={'Weightings':'Weightings_advnonadv'})

    print("change order of columns \n")
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
    
    #drop columns no longer needed
    columnstodel = ['Unnamed: 0','Carrier TOC / Third Party Code','Origin Code','Destination Code','Route Code','Operating Journeys','ticket_type']
    superfile.drop(columnstodel,axis=1,inplace=True)

    #Rename column of "Earnings as Weightings"
    superfile.rename(columns={'Adjusted Earnings Amount':'Weightings_super'},inplace=True)
    
    #Change order of columns to match combined data
    superfile = superfile[['Product Code','Product Primary Code','sector',	'class',	'Category',	'Regulated_Status','Weightings_super']]

    #Grouping of superfile by relevant groups
    superfile = superfile.groupby(['Product Code','Product Primary Code','sector',	'class',	'Category',	'Regulated_Status']).agg('sum')

    return superfile


def lastminutechanges(df):
    """
    This is the location to make any last minute changes to the advanced non-advanced dataset prior to calculation of the calculations.
    These changes will tend to be diagnostic in character, usually around the assignment of product codes to regulated/unregulated status.
    All origin and destination codes that contain an alphabetical character
    Selected product codes are removed (refunds to season tickets?)

    Parameters
    df:     A dataframe containing the merged advanced and non advanced data

    Returns:
    df:     An amended dataframe
    Exports various cuts and summed groups of advanced and non advanced data
    
    
    """
    fileoutputpath ='C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\advanced_and_non_advanced_output\\adv_non_advanced_and_superfile\\'
    
    #remove the orgin and destination codes that contain an alphabetic character
    df = df[df['Origin Code'].str.contains('[0-9][0-9][0-9][0-9]')]
    df = df[df['Destination Code'].str.contains('[0-9][0-9][0-9][0-9]')]

    #remove these specific product codes: defined by Peter Moran by unknown process
    productcodestoremove = ['2MTC','2MTD','2MTF','2MTG']
    df = df[~df['Product Code'].isin(productcodestoremove)]

    #export for Nisha to test if reassignment has worked
    filtereddf = df[df.Category == 'season']
    groupedadvandnonadvanced = filtereddf.groupby(['Category','Product Code','sector','class','Regulated_Status'])['Weightings'].agg('sum')
    exportfile(groupedadvandnonadvanced,fileoutputpath, "advandnonadv grouped")

    #export for Nisah to see whether season unregulated reassignment has worked
    subcutofdataseasonUnregulated = df[ (df['Category'] == 'season') & (df['Regulated_Status'] == 'Unregulated')  ]
    exportfile(subcutofdataseasonUnregulated,fileoutputpath,'seasonunregulated')

    return df


if __name__ == '__main__':
    main()
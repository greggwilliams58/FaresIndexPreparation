import pandas as pd
from commonfunctions import applydatatypes, exportfile
#from export_data import exportfile
from calculate_results import calc_weighted_average_price_change, calc_final

def main():

    filelocation = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\advanced_and_non_advanced_output\\'
    outputto = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\advanced_and_non_advanced_output\\adv_non_advanced_and_superfile\\'


    print("getting advanced data\n")
    advanceddata = pd.read_csv(filelocation + 'advancedfile_20190306_11-07.csv',
                               dtype={'Carrier Profit Centre Code':'category','Origin Code':'category','Destination Code':'category','Route Code':'category',
                                      'Product Code':'category','Product Level 1 Code':'category','class':'category','sector':'category'})

    #exportfile(advanceddata.info(),filelocation,"advanced_metadata")
    

    nonadvanceddata = pd.read_csv(filelocation + 'nonadvancedfile_noavantix_20190306_11-23.csv',
                                  dtype={'Carrier Profit Centre Code':'category','Origin Code':'category','Destination Code':'category','Route Code':'category',
                                      'Product Code':'category','Product Level 1 Code':'category','class':'category','sector':'category'})

    #exportfile(nonadvanceddata.info(),filelocation,"non_advanced_metadata")
    
    print("getting superfile for weights")
    rawsuperfile = pd.read_csv(filelocation + 'rawsuperfile_20190306_11-01.csv',
                               dtype={'Carrier Profit Centre Code':'category','Origin Code':'category','Destination Code':'category','Route Code':'category',
                                      'Product Code':'category','Product Level 1 Code':'category','class':'category','sector':'category','ticket_type':'category'}
                               )

    print("preparing the superfile...\n")
    preparedsuperfile = preparesuperfile(rawsuperfile)
    #exportfile(preparedsuperfile,outputto,"preparedsuperfile")
    print("data joined.  showing metadata\n")
     
    #print(f"The datatype for the list of df is {type([advanceddata,nonadvanceddata])}\n")
    #print(f"The datatype for the preparedsuperfile is {type(preparedsuperfile)}\n")

    advandnonadv = appenddata([advanceddata,nonadvanceddata])
    #exportfile(advandnonadv,outputto,'preparedadvandnonadv')

    
    advandnonadv.sort_values(by=['sector','class','Category','Regulated_Status'],ascending=True,inplace=True)
    preparedsuperfile.sort_values(by=['sector','class','Category','Regulated_Status'],ascending=True,inplace=True)


    answergrid = calc_weighted_average_price_change(advandnonadv,preparedsuperfile,['sector','class','Category','Regulated_Status'])

    #change name of weighted_price_change
    answergrid.rename(columns={answergrid.columns[0]:'weighted_price_change'},inplace=True)

    #remove the group superweights where the sum is zero
    answergrid = answergrid.drop(answergrid[answergrid['Weightings_super'] == 0].index)

    #flatten the answergrid
    answergrid.columns = [''.join(col).strip() for col in answergrid.columns.values]
    answergrid = answergrid.reset_index()

    
    #print(answergrid.info())
    #exportfile(answergrid,outputto,"answerfile")

    #wpc * superweightings
    answergrid['wpc_and_weights'] = answergrid['weighted_price_change'] * answergrid['Weightings_super']
    print("this is the answergrid\n")
    #print(answergrid.info())
    exportfile(answergrid,outputto,"answerfile")

    #final answer for T1.8: sector and ticket type
    #this is a good candidate for turning into a function with 'grouping' to replace 'sector','class' for the various numbers needed
    #finalanswerT1_8 = answergrid.groupby(['sector','class'])['wpc_and_weights'].agg('sum') / answergrid.groupby(['sector','class'])['Weightings_super'].agg('sum')
    
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
   
    #exportfile(sectorsplit,outputto,'sector')
    #exportfile(classsplit,outputto,'class')
    #exportfile(sectorclasssplit,outputto,'sectorclass')
    #exportfile(regulatedstatussplit,outputto,'regulated_status')
    #exportfile(categorysplit,outputto,'category')
    #exportfile(sectorcategorysplit,outputto,'sectorcategory')
    #exportfile(sectorclassregulatedstatus,outputto,'sectorclassregulatedstatus')
    #exportfile(classregulatedstatus,outputto,'classregulatedstatus')



    #exportfile(finalanswerT1_81,outputto,'T1_81')


    #calculate the answer of class and regulation
    #avgpricechange = (df.groupby(['class','Regulated_Status'])['weighted_price_change','Weightings_super'].agg('sum'))/(df.groupby(grouping)['Weightings_advnonadv'].agg('sum'))
    #need to create sum product function here.  weight_price_change * weightings_super, then sum the resulting products

def appenddata(nonadvandadv):
    """
    This file joins two dataframes

    Parameters
    nonadvandadv    - A list of dataframes representing the advanced and non-advanced datasets to be appended 
    sf              - A data frame containing the raw superfile
    
    Returns
    sourcedata      - a dataframe with appended data
    """
    fileoutputpath ='C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\advanced_and_non_advanced_output\\adv_non_advanced_and_superfile\\'
    #add flag for datasource
    #print("adding flag for advanced and nonadvanced datasets\n")
    #df[0].loc[:,'source'] = 'advanced data'
    #df[1].loc[:,'source'] = 'nonadvanced data'

    
    print("About to combine advanced and non-advanced data\n")
    advanced_and_non_advanced = pd.concat(nonadvandadv,ignore_index=True, sort=False)
    #print(advanced_and_non_advanced.head())


    #populate values for advanced data
    advanced_and_non_advanced['Regulated_Status'].fillna('Unregulated',inplace=True)
    advanced_and_non_advanced['Category'].fillna('advance',inplace=True)

    print("calculate factor\n")
    advanced_and_non_advanced.loc[:,'factor'] = advanced_and_non_advanced['Weightings'] * advanced_and_non_advanced['percentage_change']
    #print(advanced_and_non_advanced.info())
    
    #drop unnecessary columns
    columnstodel = ['Unnamed: 0','Carrier Profit Centre Code','Origin Code','ticket_type','Destination Code','Route Code','Product Code','Product Level 1 Code','Operating Journeys (*)','FARES_2017','FARES_2018']
    print("dropping columns")
    advanced_and_non_advanced.drop(columnstodel,axis=1,inplace=True)

    print("filtering data")
    advanced_and_non_advanced_to_be_filtered = advanced_and_non_advanced.copy()
     #apply filter for upper and lower percentage changes
    advanced_and_non_advanced_filtered = advanced_and_non_advanced_to_be_filtered.query('percentage_change > -20 and percentage_change < 20')

    #rename weightings column
    print("rename columns")
    advanced_and_non_advanced_filtered_renamed = advanced_and_non_advanced_filtered.rename(columns={'Weightings':'Weightings_advnonadv'})

    print("change order of columns")
    #change order of columns to match combined data
    advanced_and_non_advanced_reordered = advanced_and_non_advanced_filtered_renamed[['sector',	'class',	'Category',	'Regulated_Status','Weightings_advnonadv','factor']]


    return advanced_and_non_advanced_reordered 


def preparesuperfile(superfile):
    # drop irrelevant columns
    #columnstodel = ['Unnamed: 0','Carrier Profit Centre Code','Origin Code','Destination Code','Route Code','Product Code','Product Level 1 Code','Operating Journeys (*)','ticket_type','orig','dest','route']
    #superfile.drop(columnstodel,axis=1,inplace=True)
    
    #temp for peter columns to drop
    columnstodel = ['Unnamed: 0','Carrier Profit Centre Code','Origin Code','Destination Code','Route Code','Operating Journeys (*)','ticket_type','orig','dest','route']
    superfile.drop(columnstodel,axis=1,inplace=True)


    #Rename column of "Earnings as Weightings"
    superfile.rename(columns={'Adjusted Earning Sterling (*)':'Weightings_super'},inplace=True)

    #change order of columns to match combined data
    #superfile = superfile[['sector',	'class',	'Category',	'Regulated_Status','Weightings_super']]
    
    #temp for peter change order of columns to match combined data
    superfile = superfile[['Product Code','Product Level 1 Code','sector',	'class',	'Category',	'Regulated_Status','Weightings_super']]

    #temp grouping for peter
    superfile = superfile.groupby(['Product Code','Product Level 1 Code','sector',	'class',	'Category',	'Regulated_Status']).agg('sum')
    #print(superfile_grouped)
    return superfile



if __name__ == '__main__':
    main()
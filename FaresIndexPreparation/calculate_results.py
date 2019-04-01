from commonfunctions import handlezeroandnulls,percentagechange, exportfile
#from commonfunctions import exportfile
import pandas as pd
import numpy as np


def calc_final(df, grouping,nameofsplit):
    """
    This function takes a dataframe of 
    """
    
    
    answer = df.groupby(grouping)['wpc_and_weights'].agg('sum') / df.groupby(grouping)['Weightings_super'].agg('sum') 
    answer_df = answer.to_frame()

    weightings = df.groupby(grouping)['Weightings_super'].agg('sum') 
    weightings_df = weightings.to_frame()
    
    #insert the names of splits as a new column into dataframe
    answer_df.insert(0,'split_name',value = nameofsplit)

    
    answerwithweights = pd.concat([answer_df,weightings_df],axis=1)

    return answerwithweights


def calculate_endresults(advandnonadv,preparedsuperfile):
    """
    This function takes the combined data of advanced and non-advanced and then calculates a factor of weights*percentage change
    zero and nulls are handeled and percentage change is calculated
    data is split into advanced and non-advanced and all dt, is passed into a dictionary
    The dictionary is looped over and different permutations of weighted averages are calculated, depending on dataset
    
    Parameters:
    df          - A dataframe containing the combined data of advanced and non-advanced data
    uandlbands  - a list of list containing the upper and lower bounds of non-advanced data to be stripped from calculation of non-advanced data
    sfile       - a dataframe containing the superfile dataset
    """
    
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




def calc_weighted_average_price_change(df,superfile,grouping):
    """
    This function sums the factor column and then divides the result by all weights.  It also prints a status message to console.

    Parameters:
    df              - A dataframe containing the advanced and nonadvanced data
    grouping        - A string representing the column to group the data by
    weighting       - A int64 representing the sum of all weights
    status          - A string representing the status of the global data

    Returns:
    avgpricechange  - A dataframe containing results of calculation
    """
    
    print(f"\nThese is the split of the data by {grouping}")
    avgpricechange = (df.groupby(grouping)['factor'].agg('sum'))/(df.groupby(grouping)['Weightings_advnonadv'].agg('sum'))


    #return sum of superfile weights
    summed_super_weights = superfile.groupby(grouping)['Weightings_super'].agg('sum')
    
    summed_avdnonadv_weights = df.groupby(grouping)['Weightings_advnonadv'].agg('sum')


    avgpricechange = avgpricechange.to_frame()
    print("this is the avg price change")
    print(avgpricechange.info())
    summed_super_weights = summed_super_weights.to_frame()
    summed_avdnonadv_weights = summed_avdnonadv_weights.to_frame()

    #print(summed_super_weights.info())
    #print(summed_avdnonadv_weights.info())

    print("this is the summed weights")
    #print(summed_weights.info())

    #check that this produces the right output.... prefer to use pd.merge, if possible
    avgpricechange = pd.concat([avgpricechange,summed_avdnonadv_weights,summed_super_weights],axis=1)

    
    return avgpricechange

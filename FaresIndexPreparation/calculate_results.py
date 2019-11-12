from commonfunctions import handlezeroandnulls,percentagechange, exportfile
import pandas as pd
import numpy as np


def calc_final(df, grouping,nameofsplit):
    """
    This function takes a dataframe of the answer file and produces a SUMPRODUCT which is returned as a dataframe, 
    with the names of splits, elements of split, final weights average prices changes and percentage of weights for each grouping

    Parameters
    df:                 A dataframe containing the answerfile from the calculated weighted percentage change
    grouping:             A list containing column names which the sum will be grouped by
    nameofsplit:        A string containing the name of the calculation for later use in exported final dataset

    Returns:
    answerwithweights: A dataframe containing the calculated results from the answerfile and superfile combined
    """
    
    #A SUMPRODUCT over the answerfile
    answer = df.groupby(grouping)['wpc_and_weights'].agg('sum') / df.groupby(grouping)['Weightings_super'].agg('sum') 
    answer_df = answer.to_frame()


    #Production of matching aggregation from super_weights
    weightings = df.groupby(grouping)['Weightings_super'].agg('sum') 
    weightings_df = weightings.to_frame()
    
    #insert the names of splits as a new column into dataframe
    answer_df.insert(0,'grouping_name',value = nameofsplit)


    #get sum of all weights
    allsuperweights = df['Weightings_super'].sum()
    #get percentage of weights by grouping
    shareofweights = df.groupby(grouping)['Weightings_super'].agg('sum') / allsuperweights

    #join the  answers, superweights and weightshare together
    answerwithweights = pd.concat([answer_df,weightings_df,shareofweights],axis=1)

    #rename the index and columns of the answerfile
    answerwithweights.index.name = 'parts_of_the_grouping'

    return answerwithweights



def calc_weighted_average_price_change(df,superfile,grouping):
    """
    This function sums the factor column and then divides the result by all weights 
    Excel equivalent is SUMPRODUCT.  
    It also prints a status message to console.

    Parameters:
    df              - A dataframe containing the advanced and nonadvanced data
    grouping        - A string representing the column to group the data by
    weighting       - A int64 representing the sum of all weights
    status          - A string representing the status of the global data

    Returns:
    avgpricechange  - A dataframe containing results of calculation
    """
    
    print(f"\nThese is the split of the data by {grouping}\n")
    avgpricechange = (df.groupby(grouping)['factor'].agg('sum'))/(df.groupby(grouping)['Weightings_advnonadv'].agg('sum'))

    #calculate the denominator for later export, for advanced and nonadvanced as well as superweights
    summed_super_weights = superfile.groupby(grouping)['Weightings_super'].agg('sum')
    summed_avdnonadv_weights = df.groupby(grouping)['Weightings_advnonadv'].agg('sum')

    #convert results sets to dataframes
    avgpricechange = avgpricechange.to_frame()
    summed_super_weights = summed_super_weights.to_frame()
    summed_avdnonadv_weights = summed_avdnonadv_weights.to_frame()

    #join the three results dataframes together
    avgpricechange = pd.concat([avgpricechange,summed_avdnonadv_weights,summed_super_weights],axis=1)

    return avgpricechange

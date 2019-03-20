from commonfunctions import handlezeroandnulls,percentagechange, exportfile
#from commonfunctions import exportfile
import pandas as pd
import numpy as np


def calc_final(df, grouping):
    answer = df.groupby(grouping)['wpc_and_weights'].agg('sum') / df.groupby(grouping)['Weightings_super'].agg('sum')
    return answer


def calculate_endresults(advnonadv,sfile,uandlbands=[-20,20]):
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
    
    #df = handlezeroandnulls(df)

    #df = percentagechange(df,'FARES_2018','FARES_2017')
        
    #multiplication of weights by percentage_change
    #df.loc[:,'factor'] = df['Weightings']*df['percentage_change']


    #advanced = df[df['source']=='advanced data']
    #nonadvanced = df[df['source']== 'nonadvanced data']
    #all = df.copy()

    #data = {'advanced_data':advanced, 'nonadvanced_data':nonadvanced,'all_data':all}
    #exportfile(all,'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexOutput\\','all_raw_data')


    #for key,value in data.items():
        #sum of all weights
        #filterupper and lower bands
            #for each lower and upper bound This needs to be added here
    #df.query('percentage_change > -20 and percentage_change < 20')
    #change weighting source from superfile....
    #
            
    #print(f"All weights in {key} are {allweights}\n")

        
    print(calc_weighted_average_price_change(advnonadv,sfile,['sector'],'all'))
    print("\n")
    print(calc_weighted_average_price_change(advnonadv,sfile,['class'],'all'))
    print("\n")
    print(calc_weighted_average_price_change(advnonadv,sfile,['Regulated_Status'],'all'))
    missingstatusrevenue = df.loc[datum['Regulated_Status']=='not assigned','Weightings'].sum()
    print(f"The value of missing status revenue in {key} is £{'{:,}'.format(int(missingstatusrevenue))}")

    #supersplit = calc_weighted_average_price_change(advnonadv,['sector','class','Regulated_Status'],allweights,'supersplit',key)
    #print(f"The supersplit in {key} is {supersplit}")

    print("\n")
    print(calc_weighted_average_price_change(advnonadv,sfile,['Category'],allweights,'all',key))
    missingcategoryrevenue = validdata.loc[advnonadv['Category']=='Missing','Weightings'].sum()
    print(f"The value of missing revenue in {key} is £{'{:,}'.format(int(missingcategoryrevenue))}") 
    print("\n")
    
    avgpricechange = advnonadv['factor'].agg('sum')/advnonadv['Weightings'].agg('sum')
    print(f"The average overall {key} price change is {avgpricechange}") 

    exportfile(advnonadv,filepath,f'{key}')




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

# moved to common functions
#def handlezeroandnulls(df):
#    #replace the 'inf' and '-inf' value with NAN
#    df.replace([np.inf, -np.inf],np.nan,inplace=True)   

#    #replace the zeros in fares2017 and fares2018 with NAN
#    df.replace({'FARES_2017':0,'FARES_2018':0},np.nan,inplace=True)
    
#    #drop the rows where FARES are NAN
#    df.dropna(axis='index',subset=['FARES_2017','FARES_2018'],how='any',inplace=True)

#    return df


#def percentagechange(df,col1,col2):
#    #calculate percentage change for each row
#    print("Adding percentage change info\n")
#    df.loc[:,'percentage_change'] = (((df.loc[:,col1]-df.loc[:,col2])/df.loc[:,col2])*100).copy()

#    return df
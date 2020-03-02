import pandas as pd
from commonfunctions import exportfile 


def main():
    #substitute for proper parameters in a function
    filelocation = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\advanced_and_non_advanced_output\\'
    outputto = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\advanced_and_non_advanced_output\\adv_non_advanced_and_superfile\\'
    
    
    print("getting superfile for weights")
    rawsuperfile = pd.read_csv(filelocation + 'superfile without regulated steps_20190425_16-04.csv',
                               dtype={'carrier_toc_code':'category','origin_code':'category','destination_code':'category','route_code':'category',
                                      'product_code':'category','pro_group_1_code':'category','class':'category','sector':'category','ticket_type':'category'}
                               )

    
    superfilefiltered = rawsuperfile[rawsuperfile['Category']=='season']

    print(type(superfilefiltered))
    print(superfilefiltered.head(5))
    print(superfilefiltered.info())

    groupedrawsuperfile = superfilefiltered.groupby(['sector',''])['adjusted_earnings'].agg('sum')
    


    exportfile(groupedrawsuperfile,outputto, "superfile others by product_code")

if __name__ == '__main__':
    main()








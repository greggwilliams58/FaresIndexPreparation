import pandas as pd
from commonfunctions import exportfile 

def get_lennon_price_info(year,filepath, filename):
   df = pd.read_csv(filepath + filename) 
   df['Issues (*)'] = df[['Issues (*)']].apply(pd.to_numeric,errors='coerce')
   df['Net Receipt Sterling (*)'] = df['Net Receipt Sterling (*)']*100

   df['LENNON_PRICE_'+year] = df['Net Receipt Sterling (*)']/df['Issues (*)']
   df['Route Code'] = df['Route Code'].astype(object)

   print(df.info())
   exportfile(df,'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexOutput\\',"LENNONPRICE")
   return df


#placeholder for getting LENNON fare information
LENNONprices2017 = get_lennon_price_info('2017','C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\LENNON_Fares_information\\','pricefile2017P1112.csv')


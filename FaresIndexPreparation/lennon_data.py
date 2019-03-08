import pandas as pd
from commonfunctions import applydatatypes

def get_lennon_price_info(year,filepath, filename,typeofjoin):
   """
   This gets lennon prices information held as a csv file and puts it into a dataframe.  It also applies datatyping, 
   converts the cash values into pence and divides Receipt by Issues.  It finally drops unnecessary columns 


   Parameters:
   year         - a string giving the year to which the reference data relays
   filepath     - a string giving the file path where the reference data is held
   filename     - a string holding the name of the file holding the reference data
   jointype     - a string indicating the type of data being joined to

   Returns:
   df           - a data frame containing the LENNON reference data
   """
   
   #write new dictionary to handle non-advanced lennon datatyping here
   #'Origin Code','Destination Code','Route Code','Product Code'
   advanceddtypedict = {'Origin Code':str,'Destination Code':str,'Route Code':str,'class':str}
   nonadvanceddtypedict = {'Route Code':str}

   if typeofjoin == 'non-advanced':
       dtypedictionary = nonadvanceddtypedict
   elif typeofjoin == 'advanced':
       dtypedictionary = advanceddtypedict
   else:
       print("incorrect join type specified for LENNON")

   df = pd.read_csv(filepath + filename,dtype = dtypedictionary) 
   df['Issues (*)'] = df[['Issues (*)']].apply(pd.to_numeric,errors='coerce')
   df['Net Receipt Sterling (*)'] = df['Net Receipt Sterling (*)']*100

   df['LENNON_PRICE_'+year] = df['Net Receipt Sterling (*)']/df['Issues (*)']
   


   del df['Net Receipt Sterling (*)']
   del df['Issues (*)']

   return df


def add_lennon_fares_info(df,lookupdf,year,typeofjoin):
    """
    This merges the LENNON data with the superfile and adds a year reference to the column title

    Parameters:       
    df          - a dataframe containing the combined data which is to be joined against
    lookupdf    - a dataframe containing the relevant LENNON information
    year        - a string representing the year the data relates to
    typeofjoin  - a string representing whether the  join is for advanced or non-advanced data
    
    Returns:
    df          - a dataframe with LENNON data joined
    """

    if typeofjoin == 'non-advanced':
        #the non-advanced join
        df = pd.merge(left=df, right=lookupdf, how='left',
                  left_on=['Origin Code','Destination Code','Route Code','Product Code'],
                  right_on=['Origin Code','Destination Code','Route Code','Product Code'],
                  suffixes=('','_LENNON'+year))
    
    elif typeofjoin == 'advanced':
        #original join 
        #['Origin Code','Destination Code','Route Code','class']
        print("this is non advanced datatyping")
        df['Origin Code']=df['Origin Code'].astype(str)
        df['Destination Code']=df['Destination Code'].astype(str)
        df['Route Code'] = df['Route Code'].astype(str)
        df['class']=df['class'].astype(str)

        df = pd.merge(left=df,right=lookupdf,how='left',
                      left_on=['Origin Code','Destination Code','Route Code','class'],
                      right_on=['Origin Code','Destination Code','Route Code','class'],
                      suffixes=('','_LENNON'+year)
                      )
        df = applydatatypes(df,['Origin Code','Destination Code','Route Code','class'])
    
    else:
        print("Type of join not recognised")
    return df

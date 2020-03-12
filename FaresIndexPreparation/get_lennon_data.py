import pandas as pd
from commonfunctions import exportfile
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
   
   #dictionaries to handle non-advanced lennon datatyping here
   advanceddtypedict = {'Origin Code':str,'Destination Code':str,'Route Code':str,'class':str}
   nonadvanceddtypedict = {'Route Code':str}

   if typeofjoin == 'non-advanced':
       dtypedictionary = nonadvanceddtypedict
   elif typeofjoin == 'advanced':
       dtypedictionary = advanceddtypedict
   else:
       print("incorrect join type specified for LENNON")

   df = pd.read_csv(filepath + filename,dtype = dtypedictionary) 

   #applying data typing to key fields

   print("applying data typing to LENNON data\n")
   df['Origin Code'] = df['Origin Code'].str.zfill(4)
   df['Destination Code'] = df['Destination Code'].str.zfill(4)
   df['Route Code'] = df['Route Code'].str.zfill(5)
   #remove the currency formatting if it is present in file
   
   #required for the 2020 set of pricefiles
   #if typeofjoin == 'non-advanced':
   #    df['NetReceiptSterling'] = df['NetReceiptSterling'].str.replace(',','')
   #    df['NetReceiptSterling'] = df['NetReceiptSterling'].str.replace('£','')
   #    df['NetReceiptSterling'] = df[['NetReceiptSterling']].apply(pd.to_numeric,errors='coerce')

   #convert the number of ticket issues to numeric datatype
   df['Issues'] = df[['Issues']].apply(pd.to_numeric,errors='coerce')


   
   #convert values in pounds to pence
   df['NetReceiptSterling'] = df['NetReceiptSterling']*100

   #create actual lennon price by dividing neceipts by number of tickets issued 
   df['LENNON_PRICE_'+year] = df['NetReceiptSterling']/df['Issues']
   
   #delete unnecessary columns
   del df['NetReceiptSterling']
   del df['Issues']
   
   print("This is the processed lennon data")
   print(df.info())
   print(df.head())
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
    # apply appropriate merge type based on name of join
    if typeofjoin == 'non-advanced':
        #the non-advanced join
        #exportfile(df,'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexOutput\\',"non-advanced input")
        df = pd.merge(left=df, right=lookupdf, how='left',
                  left_on=['Origin Code','Destination Code','Route Code','Product Code'],
                  right_on=['Origin Code','Destination Code','Route Code','Product Code'],
                  suffixes=('','_LENNON'+year))
    
        df = applydatatypes(df,['Origin Code','Destination Code','Route Code','Product Code'])

        
        #exportfile(lookupdf,'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexOutput\\',"non-advanced lookup")
        #exportfile(df,'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexOutput\\',"non-advanced output")

    elif typeofjoin == 'advanced':
        # advanced join 
        # datatyping of key fields
        print("this is non advanced datatyping")
        df['Origin Code']=df['Origin Code'].astype(str)
        df['Destination Code']=df['Destination Code'].astype(str)
        df['Route Code'] = df['Route Code'].astype(str)
        df['class']=df['class'].astype(str)

        #exportfile(df,'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexOutput\\',"advanced input")

        df = pd.merge(left=df,right=lookupdf,how='left',
                      left_on=['Origin Code','Destination Code','Route Code','class'],
                      right_on=['Origin Code','Destination Code','Route Code','class'],
                      suffixes=('','_LENNON'+year)
                      )

        df = applydatatypes(df,['Origin Code','Destination Code','Route Code','class'])
        #exportfile(lookupdf,'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexOutput\\',"advanced lookup")
        #exportfile(df,'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexOutput\\',"advanced output")
        
    else:
        print("Type of join not recognised")
        return None

    return df

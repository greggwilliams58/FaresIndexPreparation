import pandas as pd
from commonfunctions import exportfile


def main():
    """
    This was written as a 'quick and dirty' product code check for Peter Moran

    """
    df = pd.read_csv('C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexOutput\\rawsuperfile_20190305_12-57.csv')

    df2aaa = df[df['Product Code']=='2AAA']
    df2aaareg = df2aaa[df2aaa['Regulated_Status']=='Regulated'] 

    df2baf = df[df['Product Code']=='2BAF' ]
    df2bafreg = df2baf[df2baf['Regulated_Status']=='Regulated'] 

    df2bfp = df[df['Product Code']=='2BFP' ]
    df2bfpreg = df2bfp[df2bfp['Regulated_Status']=='Regulated'] 

    df2aaasum = df2aaareg.groupby(['Product Code','Origin Code', 'Destination Code'])['Adjusted Earnings Amount'].agg('sum')
    df2bafsum = df2bafreg.groupby(['Product Code','Origin Code', 'Destination Code'])['Adjusted Earnings Amount'].agg('sum')
    df2bfpsum = df2bfpreg.groupby(['Product Code','Origin Code', 'Destination Code'])['Adjusted Earnings Amount'].agg('sum')

    exportfile(df2aaasum,'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexOutput\\','2aaa - pm.csv')
    exportfile(df2bafsum,'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexOutput\\','2baf - pm.csv')
    exportfile(df2bfpsum,'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexOutput\\','2bfp - pm.csv')


if __name__ == '__main__':
    main()

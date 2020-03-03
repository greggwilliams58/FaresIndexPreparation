from toc_file_processing import generatedata
from get_non_advanced import get_non_advanced_data
from get_advanced import get_advanced_data
from commonfunctions import exportfile
from get_lennon_data import get_lennon_price_info,add_lennon_fares_info
from calculate_rail_financials import getrailfinancial


def main():
    """
    This is the main entry point for the creation of the superfile, advanced and non-advanced files from the raw data and lookups.
    This calls various other functions to export the prepared files to be used in main_2 - get_results

    Parameters:
    NONE

    Returns:
    NONE, but exports the following CSV files:
    'superfile without regulated steps': The superfile, not including intermediate columns of the regulated fares process.  To be used in 2nd stage
    'advancedfile': The advanced file.  To be used in 2nd stage
    'nonadvancedfile'.  The non-advanced file.  To be used in 2nd stage

    """

    #parameters to be edited depending on users' file set up
    root = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\'
    originpath = root + 'TOC_files\\'
    regulatedfarespath = root + 'regulated_fares_data\\'
    RDGfarespath = root + 'RDG_Fares_information\\'  
    LENNONnonadvancedfarespath = root + 'LENNON_Fares_information\\non_advanced_data\\' 
    LENNONadvancedfarepath = root + 'LENNON_Fares_information\\advanced_data\\'
    categorypath = root + 'Product_Category_information\\ProdCatLookup\\'
    destinationpath = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexOutput\\'

    # the calculation of the root 'superfile'
    superfile = generatedata(originpath,destinationpath,regulatedfarespath,categorypath)
    

    ##copies of superfile, so superfile remains unamended for the various other functions
    superfileforadvanced = superfile.copy()
    superfilefornonadvanced = superfile.copy()
    superfileforrailfinance = superfile.copy()
    
    #export superfile
    print("the superfile is coming....\n")
    exportfile(superfile,destinationpath,'superfile without regulated steps')
    
    #extraction of summed earnings and journeys for check of initial TOC extraction
    totalscheck = superfile.groupby(['Carrier TOC / Third Party Code'])['Adjusted Earnings Amount','Operating Journeys'].agg('sum')
    exportfile(totalscheck,destinationpath,'sum_of_earnings_and_journies_by_toc')

    print("the advanced data is coming.... main\n")
    advanced = get_advanced_data(superfileforadvanced,destinationpath,LENNONadvancedfarepath)
    exportfile(advanced,destinationpath,'advancedfile')
     
    ##calculation of non-advanced data prior to manual validation and advantix data
    print ("The non-advanced data is coming....")
    nonadvanced = get_non_advanced_data(superfilefornonadvanced,destinationpath,RDGfarespath,LENNONnonadvancedfarespath)
    exportfile(nonadvanced,destinationpath,'nonadvancedfile')
    
    print("The data for rail financials is being prepared")
    getrailfinancial(superfileforrailfinance,destinationpath)



if __name__ == '__main__':
    main()

from toc_file_processing import generatedata
from get_non_advanced import non_advanced_data
from get_advanced import advanced_data
from commonfunctions import exportfile
from calculate_non_advanced_stats import main
from lennon_data import get_lennon_price_info,add_lennon_fares_info
from calculate_results import calculate_endresults 

def main():
    #token change

    #parameters to be edited depending on users' file set up
    root = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexSourceData\\'
    originpath = root + 'TOC_files\\'
    regulatedfarespath = root + 'regulated_fares_data\\'
    RDGfarespath = root + 'RDG_Fares_information\\'  
    LENNONnonadvancedfarespath = root + 'LENNON_Fares_information\\non_advanced_data\\' 
    LENNONadvancedfarepath = root + 'LENNON_Fares_information\\advanced_data\\'
    categorypath = root + 'SPSS\SPSS_Source_Data\\'
    manualdatapath = root + '\\Manually_checked_data\\ '
    destinationpath = 'C:\\Users\\gwilliams\\Desktop\\Python Experiments\\work projects\\FaresIndexOutput\\'
    #upperandlowerbands[[-20,20],[-15,15],[-10,10],[-5,10]]



    # the calculation of the root 'superfile'
    superfile = generatedata(originpath,destinationpath,regulatedfarespath,categorypath)
    

    ##copies of superfile, so superfile remains unamended for the various other functions
    superfileforadvanced = superfile.copy()
    superfilefornonadvanced = superfile.copy()
    superfileforweights = superfile.copy()
    superfileforrailfinance = superfile.copy()
    print("the superfile is coming....\n")
    exportfile(superfile,destinationpath,'superfile')

    #print("the advanced data is coming.... main\n")
    #advanced = advanced_data(superfileforadvanced,destinationpath,LENNONadvancedfarepath)
    #exportfile(advanced,destinationpath,'advancedfile')
     
    ##calculation of non-advanced data prior to manual validation and advantix data
    #print ("The non-advanced pre-advantix data is coming....")
    #nonadvanced = non_advanced_data(superfilefornonadvanced,destinationpath,RDGfarespath,LENNONnonadvancedfarespath)
    #exportfile(nonadvanced,destinationpath,'nonadvancedfile')
    
    ##place holder for appending 
    ##finaldataset = append_revised_data(advanced,nonadvanced,superfileforweights)

    ##upperandlowerbands[[-20,20],[-15,15],[-10,10],[-5,10]]

    ##place holder for end calculation
    ##calculate_endresults(finaldataset,superfileforweights)


if __name__ == '__main__':
    main()

# FaresIndexPreparation
This code was written to automate the production of National Statistics relating to Rail Fares at the Office of Rail and Road.  It has automated a manual 35-step process.  Given the correct input files, the finished data will be inserted into the ORR Data Warehouse.  

# Installation
This was written using Visual Studio, so ideally this would be present on the local machine.  

# Useage
Full documentation is held in the _FaresIndexDocumentation_ folder within this repository.  As this code uses a secure connection to the ORR Data Warehouse, it will not insert data into the warehouse unless the code is executed within the ORR's VPN.

The following data files will be required as input: The formats are specified in the _Fares INdex Data Dictionary_ document within the documentation

## Stage 1
- TOC File.csv         (\\FaresIndexSourceData\TOC_files)
- regulated fares sort.csv (\\FaresIndexSourceData\regulated_fares_data)
- 2020 fares extract.txt (\\FaresIndexSourceData\RDG_Fares_information)
- Lennon product codes and fares ticket types.xlsx (\\FaresIndexSourceData\RDG_Fares_information)
- Product category lookup.xlsx (\\FaresIndexSourceData\Product_Category_information\ProdCatLookup)
- pricefile_advanced.csv  (\\FaresIndexSourceData\LENNON_Fares_information\advanced_data)
- pricefile_nonadvanced.csv  (\\FaresIndexSourceData\LENNON_Fares_information\non_advanced_data)

## Stage 2
- nonadvancedfile_YYYYMMDD_HH:MM (\\advanced_and_non_advanced_output)
- advancedfile_YYYYMMDD_HH:MM  (\\advanced_and_non_advanced_output)
- superfile without regulated steps_YYYYMMDD_HH:MM (\\advanced_and_non_advanced_output)
- answerfile_YYYYMMDD_HH:MM  (\\advanced_and_non_advanced_output\adv_non_advanced_and_superfile)
- combined_YYYYMMDD_HH:MM  (\\advanced_and_non_advanced_output\adv_non_advanced_and_superfile)

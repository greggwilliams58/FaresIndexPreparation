import datetime


def exportfile(df,destinationpath,filename,numberoffiles=1):
    """
    This procedure exports the finalised file as a CSV file with a datetime stamp in filename

    Parameters:
    df        - a dataframe containing the finalised data
    destinationpath     - a string providing the filepath for the csv file
    numberoffiles       - an int with the number of files being processed
    
    Returns:
    None, but does export dataframe df as a csv object
    """
     
    formatted_date = datetime.datetime.now().strftime('%Y%m%d_%H-%M')
    destinationfilename = f'{filename}_{formatted_date}.csv'
    print(f"Exporting {filename} to {destinationpath}{destinationfilename}\n")
    checkmessage = "If you want to check on progress, refresh the folder "+ destinationpath + " and check the size of the " + filename + ".csv file. \n"  

    if filename == 'superfile':
        if numberoffiles < 9:
            print("This is just testing so will be quick")
            print(checkmessage)
    
        elif numberoffiles > 10 and numberoffiles < 29:
            print("This may take a few minutes.  Why not go and have a nice cup of tea?\n")
            print(checkmessage)

        elif numberoffiles > 30:
            print("This may possibly hang the PC due to memory issues.  If it hangs, turn off IE, Outlook and any other memory/resource hungry applications and try again.\n")
            print(checkmessage)

        else:
            pass
    else:
        print(f"the {filename} file should be quick.")
   
    df.to_csv(destinationpath + destinationfilename)


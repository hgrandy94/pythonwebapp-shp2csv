# Import libraries for GDAL, SQL
import ogr,csv,sys
import pyodbc

## Use this only for Azure AD end-user authentication
#from azure.common.credentials import UserPassCredentials

## Use this only for Azure AD MFA
#from msrestazure.azure_active_directory import AADTokenCredentials

## Required for ADLS account management
#from azure.mgmt.datalake.store import DataLakeStoreAccountManagementClient
#from azure.mgmt.datalake.store.models import DataLakeStoreAccountManagementClient

## Required for ADLS filesystem management
from azure.datalake.store import core, lib, multithread

## Common Azure imports
#from azure.mgmt.resource.resources import ResourceManagementClient
#from azure.mgmt.resource.resources.models import ResourceGroup

## Use these as needed for your application
#import logging, getpass, pprint, uuid, time

## Create filesystem client for ADLS
subscriptionId = 'YOUR SUBSCRIPTION ID'
adlsAccountName = 'YOUR ACCOUNT NAME'

## Make ADLS credentials
adlCreds = lib.auth(tenant_id='YOUR TENANT ID', resource = 'https://datalake.azure.net/')

## Create a filesystem client object
adlsFileSystemClient = core.AzureDLFileSystem(adlCreds, store_name=adlsAccountName)

## Get the shp file from ADLS
multithread.ADLDownloader(adlsFileSystemClient, 'shpfiles', 'tempdir', 4, 4194304, overwrite=True)

## Get the shapefile from the ADL downloader
shpfile = 'tempdir/BICYCLE_PARKING_ON_STREET_WGS84.shp'

## Provide a name for the csv file
csvfilename = 'testingcsv.csv'

#Open files
csvfile = open(csvfilename,'wb')
ds = ogr.Open(shpfile)
lyr = ds.GetLayer()

#Get field names
dfn = lyr.GetLayerDefn()
nfields = dfn.GetFieldCount()
fields = []
for i in range(nfields):
    fields.append(dfn.GetFieldDefn(i).GetName())
fields.append('kmlgeometry')
csvwriter = csv.DictWriter(csvfile, fields)
try:csvwriter.writeheader() #python 2.7+
except:csvfile.write(','.join(fields)+'\n')

# Write attributes and kml out to csv
for feat in lyr:
    attributes=feat.items()
    geom = feat.GetGeometryRef()
    attributes['kmlgeometry']=geom.ExportToKML()
    csvwriter.writerow(attributes)

#clean up
del csvwriter,lyr,ds
csvfile.close()

## Upload the csv file to ADLS
multithread.ADLUploader(adlsFileSystemClient,'csvfiles/testfile.csv', csvfilename, overwrite=True)

## Do SQL stuff!
## Create SQL connection
server = 'YOURSERVERNAME.database.windows.net,1433'
database = 'YOUR DATABASE NAME'
username = 'YOUR USER NAME'
password = 'YOUR PASSWORD'
driver= '{ODBC Driver 13 for SQL Server}'
conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = conn.cursor()

## Get the the column names and data types from ADLS
multithread.ADLDownloader(adlsFileSystemClient, 'sql', 'tempdir_sql', 4, 4194304, overwrite=True)

## Get the column names and datatypes for the SQL table from the ADL downloader
sqlTable = 'tempdir_sql/sqlTableInfo.csv'

## Create string to create a table in the Azure SQL DB
sqlCreateCmd = "CREATE TABLE bikeParking("
## Loop through the sqlTableInfo file to add the column names
with open(sqlTable, 'rb') as csvfile:
    mycsvreader = csv.reader(csvfile, delimiter = ',')
    for row in mycsvreader:
            ## Append to the sqlCreateCmd
            sqlCreateCmd += row[0] + " " + row[1] + "," + " "
## Finish the sqlCreateCmd string
sqlCreateCmd += ");"
#print(sqlCreateCmd)

# Drop previous table of same name if one exists
cursor.execute("DROP TABLE IF EXISTS bikeParking;")
#print("Finished dropping table (if existed).")

## Execute cmd to create SQL table 
cursor.execute(sqlCreateCmd)

## Start writing the SQL insert command string
sqlInsertCmd = "INSERT INTO bikeParking("

## Insert some data into table
## Open the metadata file first because this only has to run once
with open(sqlTable, 'rb') as sqlcsv:
    mysqlreader = csv.reader(sqlcsv, delimiter = ',')
    # Count number of cols in 'mysqlreader'
    col_count = sum(1 for r in mysqlreader)
    # Reset file object
    sqlcsv.seek(0)

    ## Loop to add column names into the SQL table
    for count, col in enumerate(mysqlreader):
        sqlInsertCmd += col[0]
        if count != col_count-1:
            sqlInsertCmd += ", "

    ## Add closing bracket for column names, and open bracket to insert values
    sqlInsertCmd += ") VALUES ("
    
    ## Loop to add values into SQL table
    for i in range(col_count):
        sqlInsertCmd += "?"
        if i != col_count-1:
            sqlInsertCmd += ", "

    ## Finish & execute the sqlCreateCmd string
    sqlInsertCmd += ");"
    #print(sqlInsertCmd)

## Insert data into SQL table
## Open the csv file containing data 
with open(csvfilename, 'rb') as csvfile:
    mycsvreader = csv.reader(csvfile, delimiter = ',')
    # Create counter to keep track of row number
    rowcounter = 0
    for row in mycsvreader:
        rowcounter+=1
        # Only print if the row number is greater than 1 (we don't want the header)
        if rowcounter > 1:
            #print(sqlInsertCmd)
            cursor.execute(sqlInsertCmd, row)

conn.commit()
cursor.close()
conn.close()

# Create table in SQL db
''' cursor.execute("""CREATE TABLE bikeParking(parkingAddress varchar(500),
postalCode varchar(8),
city varchar(100),
xCoord float,
yCoord float,
longitude float,
latitude float,
parkingType varchar(200),
flanking varchar(100),
bicCapacity int,
sizeM float,
yrInstalled int,
byLaw varchar(10),
details varchar(300),
objectID int,
kmlGeo varchar(300));""") '''
#print("Finished creating table.")

''' ## Import csv
with open(csvfilename, 'rb') as csvfile:
    mycsvreader = csv.reader(csvfile, delimiter = ',')
    # Create counter to keep track of row number
    rowcounter = 0
    for row in mycsvreader:
        rowcounter+=1
        # Only print if the row number is greater than 1 (we don't want the header)
        if rowcounter > 1:
            ## Insert some data into table
            cursor.execute("""INSERT INTO bikeParking(parkingAddress, postalCode, city, xCoord, yCoord, longitude, latitude, 
            parkingType, flanking, bicCapacity, sizeM, yrInstalled, byLaw, details, objectID, kmlGeo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""", row)
            #print("Inserted",cursor.rowcount,"row(s) of data.")

conn.commit()
cursor.close()
conn.close() '''
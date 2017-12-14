# Import libraries for GDAL, SQL
import ogr,csv,sys
import pyodbc

## Use this only for Azure AD end-user authentication
from azure.common.credentials import UserPassCredentials

## Use this only for Azure AD MFA
from msrestazure.azure_active_directory import AADTokenCredentials

## Required for ADLS account management
from azure.mgmt.datalake.store import DataLakeStoreAccountManagementClient
#from azure.mgmt.datalake.store.models import DataLakeStoreAccountManagementClient

## Required for ADLS filesystem management
from azure.datalake.store import core, lib, multithread

## Common Azure imports
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.resource.resources.models import ResourceGroup

## Use these as needed for your application
import logging, getpass, pprint, uuid, time

## Create filesystem client
subscriptionId = 'dcf4a239-316e-416c-b36c-7d1e336fb0d7'
adlsAccountName = 'adlatest2017adls'

## Make ADLS credentials
adlCreds = lib.auth(tenant_id='72f988bf-86f1-41af-91ab-2d7cd011db47', resource = 'https://datalake.azure.net/')

## Create a filesystem client object
adlsFileSystemClient = core.AzureDLFileSystem(adlCreds, store_name=adlsAccountName)

## Get the shp file from ADLS
multithread.ADLDownloader(adlsFileSystemClient, 'shpfiles', 'tempdir', 4, 4194304, overwrite=True)

## Get the shapefile from the ADL downloader
shpfile = 'tempdir/BICYCLE_PARKING_ON_STREET_WGS84.shp'

## csv file should output into Azure SQL DB ibidb1
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

## Do SQL stuff!
## Create SQL connection
server = 'tcp:ibisqlserver.database.windows.net,1433'
database = 'ibidb1'
username = 'hgrandy94@ibisqlserver'
password = 'Demo@password'
driver= '{ODBC Driver 13 for SQL Server}'
conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = conn.cursor()

# Drop previous table of same name if one exists
cursor.execute("DROP TABLE IF EXISTS bikeParking;")
#print("Finished dropping table (if existed).")

# Create table
cursor.execute("""CREATE TABLE bikeParking(parkingAddress varchar(500),
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
kmlGeo varchar(300));""")
#print("Finished creating table.")

## Import csv
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
conn.close()
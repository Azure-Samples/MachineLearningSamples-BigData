##########################
# This script is used to download content from blob storage to local machines
# It takes two arguments:
# 1. The configuration file that contains the Azure
#    storage account name, key and data source location.
#    By default, it is "./Config/fulldata_storageconfig.json"
# 2. local path where you want to download the data to in your machine
#    By default, it is "os.environ['AZUREML_NATIVE_SHARE_DIRECTORY']"
##########################

import os
import sys
import json

from azure.storage.blob import BlockBlobService


##########################
# Download the blobs from the specified account 
# localFileFolder: the local path where blobs are downloaded to
# blobFolder: the prefix of the blobs that will be downloaded
# container:  the container from which blobs will be downleded
# account: the Azure blob storage account 
# key: access key to the storage account 
##########################

def download_blob(localFileFolder,blobFolder,container, account, key):
    from azure.storage.blob import BlockBlobService
    import glob
    import os

    # Define your blob service
    blob_service = BlockBlobService(account_name=account, account_key=key)

    for blob in blob_service.list_blobs(container):
        if blobFolder in blob.name:
            if blob.properties.content_length < 1:
                continue 
            dest_path = [localFileFolder] + blob.name.split('/')
            dest_path = os.path.join(*dest_path)
            if not os.path.exists(os.path.dirname(dest_path)):
                os.makedirs(os.path.dirname(dest_path))
            blob_service.get_blob_to_path(container,
                                           blob.name, dest_path)
            
configFilename = "./Config/fulldata_storageconfig.json"

if len(sys.argv) > 1:
    configFilename = sys.argv[1]

localPath = "./Model"

if  'AZUREML_NATIVE_SHARE_DIRECTORY' in os.environ.keys():
    print(os.environ['AZUREML_NATIVE_SHARE_DIRECTORY'])
    localPath = os.environ['AZUREML_NATIVE_SHARE_DIRECTORY']
elif len(sys.argv) > 2:
    localPath = sys.argv[2]
    

# local configuration
with open(configFilename) as configFile:    
    config = json.load(configFile)
    global storageAccount, storageContainer, storageKey, dataFile
    storageAccount = config['storageAccount']
    storageContainer = config['storageContainer']
    storageKey = config['storageKey']
    print("storageContainer " + storageContainer)
    

download_blob(localPath, "featureScaleModel", "fullmodel", storageAccount, storageKey)
download_blob(localPath, "stringIndexModel", "fullmodel", storageAccount, storageKey)
download_blob(localPath , "oneHotEncoderModel", "fullmodel", storageAccount,storageKey)
download_blob(localPath , "mlModel", "fullmodel", storageAccount,storageKey)
download_blob(localPath, "info", "fullmodel", storageAccount,storageKey)
    
    
    
    
    

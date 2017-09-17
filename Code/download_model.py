import os
import sys
import json

from azure.storage.blob import BlockBlobService
def download_blob(localFileFolder,blobFolder,container, account, key):
    from azure.storage.blob import BlockBlobService
    import glob
    import os
    ## Create a new container if necessary, otherwise you can use an existing container
    # my_service.create_container('<container name>')

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

localPath = os.environ['AZUREML_NATIVE_SHARE_DIRECTORY']

if len(sys.argv) > 2:
    localPath = sys.argv[2]

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
    
    
    
    
    

def attach_storage_container(spark, account, key):
    config = spark._sc._jsc.hadoopConfiguration()
    setting = "fs.azure.account.key." + account + ".blob.core.windows.net"
    if not config.get(setting):
        config.set(setting, key)


#########################################
def write_blob(info, filename, container, account, key):
    from azure.storage.blob import BlockBlobService
    import glob
    import os
    ## Create a new container if necessary, otherwise you can use an existing container
    # blobService.create_container('<container name>')

    # Define your blob service
    blobService = BlockBlobService(account_name=account, account_key=key)
    
    import pickle
    with open(filename, 'wb') as handle:
        pickle.dump(info, handle, protocol=pickle.HIGHEST_PROTOCOL)

    # Then export that single csv file to blob storage.
    # The new result will overwrite result from the previous run.
    for name in glob.iglob(filename):
        print(os.path.abspath(name))
        blobService.create_blob_from_path(container, filename, name)

def read_blob(localFilename,blobName,container, account, key):
    from azure.storage.blob import BlockBlobService
    import glob
    import os
    
    result = None
    # Define your blob service
    blobService = BlockBlobService(account_name=account, account_key=key)
    # Load blob
    blobService.get_blob_to_path(container, blobName, localFilename)
    import pickle
    with open(localFilename, 'rb') as handle:
        result = pickle.load(handle)
    return result

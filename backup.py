import os, uuid, sys, time
from azure.storage.blob import BlobServiceClient, BlobPrefix, BlobClient, ContainerClient, __version__

# Global vars
data_dir = "/home/wk/Desktop/Backup/"


# Authenticate
connect_str = "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"

# Create the BlobServiceClient object which will be used to create a container client
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

## BACKUP ##
for filename in os.listdir(data_dir):
    print("\n" + str(filename))
    
    blobconn = BlobClient.from_connection_string(conn_str=connect_str, container_name="cbackup", blob_name=str(filename), include=['metadata','versions'])
    path = data_dir + str(filename)
    
    if not blobconn.exists():         
        with open(path, "rb") as data:
            blobconn.upload_blob(data,overwrite=True)
        blobconn.set_blob_metadata( metadata={'LastModified': str(time.ctime(os.path.getmtime(data_dir + str(filename))))})
    else:
        # Check if ModifiedData is different
        lastModifiedOnline = blobconn.get_blob_properties().metadata["LastModified"]
        lastModifiedOffline = time.ctime(os.path.getmtime(data_dir + str(filename)))
        
        print("- Online:", lastModifiedOnline)
        print("- Lokaal:", lastModifiedOffline)
        
        if lastModifiedOnline != lastModifiedOffline:
            # upload
            print("- Change detected: uploading file")
            with open(path, "rb") as data:
                blobconn.upload_blob(data,overwrite=True)
            blobconn.set_blob_metadata( metadata={'LastModified': str(time.ctime(os.path.getmtime(data_dir + str(filename))))})
        else:
            print("- No change detected")

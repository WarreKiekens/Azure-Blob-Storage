import os, uuid, sys, time
from azure.storage.blob import BlobServiceClient, BlobPrefix, BlobClient, ContainerClient, __version__

print("Azure Blob Storage v" + __version__ + " - Basic Interface")

# Global vars
data_dir = "/home/wk/Desktop/Backup/"

option = ""
while option not in ["r","restore","b","backup","l","list","t","test"]:
    print("Restore (R/r), Backup(B/b), List(L/l)")
    option = str(input("> ")).lower()

try:
    # Authenticate
    connect_str = "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
    
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    ## RESTORE ##
    if option in ["r", "restore"]:
        subOption = ""
        while subOption not in ["1","2"]:
            print("1. Restore all files \n2. Restore 1 file")
            subOption = str(input("> "))
            
        if subOption == "1":
            container_client = blob_service_client.get_container_client("cbackup")
            blob_list = container_client.list_blobs()
            for blob in blob_list:
                blobconn = BlobClient.from_connection_string(conn_str=connect_str, container_name="cbackup", blob_name=str(blob.name))
                filename = data_dir + str(blob.name)
                with open(filename, "wb") as curr_blob:
                    blob_data = blobconn.download_blob()
                    blob_data.readinto(curr_blob)
                    
        elif subOption == "2":
            container_client = blob_service_client.get_container_client("cbackup")
            blob_list = container_client.list_blobs(include=['metadata','versions'])
            blobDict = {}
            for blob in blob_list:
                
                if blob["name"] not in blobDict:
                    blobDict[blob["name"]] = {"Versions": 0, "LastModified": {}}
                else:
                    blobDict[blob["name"]]["Versions"] = int(blobDict[blob["name"]]["Versions"]) + 1
                    
                    try: 
                        date = blob["metadata"]["LastModified"]                        
                        blobDict[blob["name"]]["LastModified"][date] = blob["version_id"]
                    except:
                        date = "None"               
                    
            print("Select a file to restore. (ex. file.txt 2)")
            
            for item in blobDict:
                print("-", item, '(Versions:', str(len(blobDict[item]["LastModified"])) + ")")
                i = 1
                for version in blobDict[item]["LastModified"]:
                    print("   ", str(i) + ":", blobDict[item]["LastModified"][version],"(" + version + ")")
                    i = i + 1
                    
            validOption = False
            while not validOption:
                restoreOption = str(input("> ")).lower()
                
                if len(restoreOption.rsplit(" ", 1)) == 2:
                    def RepresentsInt(s):
                        try: 
                            int(s)
                            return True
                        except ValueError:
                            return False
        
                    if RepresentsInt(restoreOption.rsplit(" ", 1)[-1]):
                        fileName = str(restoreOption.rsplit(" ", 1)[0])
                        versionNumber = abs(int(restoreOption.rsplit(" ", 1)[-1]))
                        validOption = True
            try: 
                blobconn = BlobClient.from_connection_string(conn_str=connect_str, container_name="cbackup", blob_name=str(fileName), include=['metadata','versions'])
                path = data_dir + str(fileName)
                
                versions = []
                for version in blobDict[item]["LastModified"]:
                    versions.append(blobDict[item]["LastModified"][str(version)])
                versionId = versions[versionNumber - 1]
                
                if blobconn.exists:
                        
                    with open(path, "wb") as curr_blob:
                        blob_data = blobconn.download_blob(version_id=str(versionId))
                        print("Downloading...")
                        blob_data.readinto(curr_blob)
                        
                    print("Restore succesfully")
                
            except Exception as ex:
                print('Exception:')
                print(ex)

	  ## BACKUP ##

    elif option in ["b", "backup"]:
    
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
        
        
	  ## LIST ##

    elif option in ["l", "list"]:
    
        def walk_container(client, container):
            container_client = client.get_container_client(container.name)
            print('C: {}'.format(container.name))
            depth = 1
            separator = '   '
            
            def walk_blob_hierarchy(prefix=""):
                
                nonlocal depth
                for item in container_client.walk_blobs(name_starts_with=prefix):
                    
                    short_name = item.name[len(prefix):]
                    if isinstance(item, BlobPrefix):
                        print('F: ' + separator * depth + short_name)
                        depth += 1
                        walk_blob_hierarchy(prefix=item.name)
                        depth -= 1
                    else:
                        message = 'B: ' + separator * depth + short_name
                        results = list(container_client.list_blobs(name_starts_with=item.name, include=['versions']))
                        num_snapshots = len(results) - 1
                        if num_snapshots:
                            message += " ({} versions)".format(num_snapshots)
                        print(message)
            walk_blob_hierarchy()

        containers = blob_service_client.list_containers()

        for container in containers:
            walk_container(blob_service_client, container)           

except Exception as ex:
    print('Exception:')
    print(ex)

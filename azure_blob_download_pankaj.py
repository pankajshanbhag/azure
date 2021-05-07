from urllib.parse import urlparse
from azure.storage.blob import BlockBlobService
import constants as c
import os

def azure_blob_download():

    sas_url=""
    url = urlparse(sas_url)
    account = url.hostname[0:url.hostname.find(".")]
    container = url.path[url.path.find("/") + 1:]
    token = url.query

    #prefix="87005/2021/01/25/20210427052048/tgt_cross_check/assets_summary"
    prefix=c.BLOB_SOURCE_PATH
    #directory_path="C://Users//d6650819//Desktop"
    directory_path=c.BLOB_TARGET_PATH
    blob_service = BlockBlobService(account_name=account,sas_token=token)


    # To list out files
    list_files=blob_service.list_blobs(container_name=container,prefix=prefix)
    for fl in list_files:
        print(fl + '\n')


    for blob_file in blob_service.list_blobs(container, prefix):
            local_file_path = os.path.join(directory_path, blob_file.name)
            local_dir_path = os.path.join(directory_path, os.path.dirname(blob_file.name))
            os.makedirs(local_dir_path, mode=0o777, exist_ok=True)
            blob_service.get_blob_to_path(container, blob_file.name, local_file_path)

    print("Files are copied into dir ...{}",c.BLOB_TARGET_PATH)

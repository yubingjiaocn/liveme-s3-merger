import json
import boto3
import botocore
import os
import logging
import threading
from natsort import natsorted

logging.basicConfig(format='%(asctime)s => %(message)s')

BUCKET = ''
MIN_S3_SIZE = 6000000
parts_mapping = []


def new_s3_client():
    # initialize an S3 client with a private session so that multithreading
    # doesn't cause issues with the client's internal state
    client_config = botocore.config.Config(
        max_pool_connections=25,
    )

    session = boto3.session.Session()
    return session.client('s3', config=client_config)

def collect_parts(s3, folder, prefix):
    return list(filter(lambda x: x[0].startswith(folder+"/"+prefix), _list_all_objects_with_size(s3, folder)))   

def _list_all_objects_with_size(s3, folder):

    def resp_to_filelist(resp):
        return [(x['Key'], x['Size']) for x in resp['Contents']]

    objects_list = []
    resp = s3.list_objects(Bucket=BUCKET, Prefix=folder)
    objects_list.extend(resp_to_filelist(resp))
    while resp['IsTruncated']:
        # if there are more entries than can be returned in one request, the key
        # of the last entry returned acts as a pagination value for the next request
        logging.warning("Found {} objects so far".format(len(objects_list)))
        last_key = objects_list[-1][0]
        resp = s3.list_objects(Bucket=BUCKET, Prefix=folder, Marker=last_key)
        objects_list.extend(resp_to_filelist(resp))
    
    logging.warning("Found {} objects total".format(len(objects_list)))
    objects_list = natsorted(objects_list)
    logging.warning("Objects: ".format(print(objects_list)))
    return objects_list

def chunk_by_size(parts_list, max_filesize):
    grouped_list = []
    current_list = []
    current_size = 0
    for p in parts_list:
        current_size += p[1]
        current_list.append(p)
        if current_size > max_filesize:
            grouped_list.append(current_list)
            current_list = []
            current_size = 0
    if current_list != []:
        grouped_list.append(current_list) 
    print(grouped_list)    
    return grouped_list

def run_concatenation(folder_to_concatenate, result_filepath, file_prefix):
    s3 = new_s3_client()
    parts_list = collect_parts(s3, folder_to_concatenate, file_prefix)
    logging.warning("Found {} parts to concatenate in {}/{}".format(len(parts_list), BUCKET, folder_to_concatenate))
    if len(parts_list) > 1:
        # perform multi-part upload
        upload_id = initiate_concatenation(s3, result_filepath)
        parts_mapping = assemble_parts_to_concatenate(s3, result_filepath, upload_id, parts_list)
        result = complete_concatenation(s3, result_filepath, upload_id, parts_mapping)
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": print("Merged file to {}".format(result)),
                "path": result
            }),
        }
    elif len(parts_list) == 1:
        # can perform a simple S3 copy since there is just a single file
        resp = s3.copy_object(Bucket=BUCKET, CopySource="{}/{}".format(BUCKET, parts_list[0][0]), Key=result_filepath)
        logging.warning("Copied single file to {} and got response {}".format(result_filepath, resp))
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": print("Copied single file to {}".format(result_filepath)),
                "path": result_filepath
            }),
        }
    else:
        logging.warning("No files to concatenate for {}".format(result_filepath))
        return {
            "statusCode": 400,
            "body": json.dumps({
                "message": print("No files to concatenate")
            }),
        }

def initiate_concatenation(s3, result_filename):
    # performing the concatenation in S3 requires creating a multi-part upload
    # and then referencing the S3 files we wish to concatenate as "parts" of that upload
    resp = s3.create_multipart_upload(Bucket=BUCKET, Key=result_filename)
    logging.warning("Initiated concatenation attempt for {}, and got response: {}".format(result_filename, resp))
    return resp['UploadId']

def assemble_parts_to_concatenate(s3, result_filename, upload_id, parts_list):
    global parts_mapping 
    parts_mapping = []
    part_num = 0
    grouped_list = chunk_by_size(parts_list, MIN_S3_SIZE)
    threads = []
    for group in grouped_list:
        part_num = part_num + 1
        t = threading.Thread(target=_thread_concatenate,
                                 args=(group, part_num, s3, result_filename, upload_id))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()    
    return parts_mapping

def _thread_concatenate(group, part_num, s3, result_filename, upload_id):
    global parts_mapping
    if len(group) == 1:
        resp = s3.upload_part_copy(Bucket=BUCKET,
                           Key=result_filename,
                           PartNumber=part_num,
                           UploadId=upload_id,
                           CopySource=BUCKET+"/"+group[0][0])
        logging.warning("Setup S3 part #{}, with path: {}, and got response: {}".format(part_num, group[0][0], resp))
        parts_mapping.append({'ETag': resp['CopyPartResult']['ETag'][1:-1], 'PartNumber': part_num})    
    else: 
        small_parts = []
        for source_part in group:
            temp_filename = "/tmp/{}".format(source_part[0].replace("/","_"))
            s3.download_file(Bucket=BUCKET, Key=source_part[0], Filename=temp_filename)

            with open(temp_filename, 'rb') as f:
                small_parts.append(f.read())
            os.remove(temp_filename)
            logging.warning("Downloaded and copied small part with path: {}".format(source_part[0]))

        if len(small_parts) > 0:
            part = bytes().join(small_parts)
            resp = s3.upload_part(Bucket=BUCKET, Key=result_filename, PartNumber=part_num, UploadId=upload_id, Body=part)
            logging.warning("Setup local part #{} from {} small files, and got response: {}".format(part_num, len(small_parts), resp))
            parts_mapping.append({'ETag': resp['ETag'][1:-1], 'PartNumber': part_num})

def complete_concatenation(s3, result_filename, upload_id, parts_mapping):
    if len(parts_mapping) == 0:
        resp = s3.abort_multipart_upload(Bucket=BUCKET, Key=result_filename, UploadId=upload_id)
        logging.warning("Aborted concatenation for file {}, with upload id #{} due to empty parts mapping".format(result_filename, upload_id))
        raise
    else:
        print(format(parts_mapping))
        resp = s3.complete_multipart_upload(Bucket=BUCKET, Key=result_filename, UploadId=upload_id, MultipartUpload={'Parts': parts_mapping})
        logging.warning("Finished concatenation for file {}, with upload id #{}, and parts mapping: {}".format(result_filename, upload_id, parts_mapping))
        return result_filename

def lambda_handler(event, context):
    print(format(event))
    global BUCKET
    BUCKET = event["bucket"]
    result = run_concatenation(event["folder"], event["output"], event["prefix"])
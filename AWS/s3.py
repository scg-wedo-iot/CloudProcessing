import os
from os.path import join as pjoin
import logging
import glob

# import boto3
import cv2
# from botocore.exceptions import ClientError
import numpy as np

from .session import *

class AwsS3Bucket(AwsSession):
    def __init__(self, authen_by='production', credentials=None, bucket_name=None):
        AwsSession.__init__(self, authen_by, credentials)
        self.connect_bucket(bucket_name)

    def connect_bucket(self, bucket_name):
        self.bucket_name = bucket_name

        s3 = self.session.resource('s3')
        self.bucket = s3.Bucket(self.bucket_name)

    def list_objects_in_folder(self, key_folder):
        list_objects = self.bucket.objects.all()
        list_objects = filter_list_objects(list_objects, prefix=key_folder)
        list_objects = object2list(list_objects)

        return list_objects

    def show_objects_in_folder(self, key_folder):
        list_objects = self.list_objects_in_folder(key_folder)
        for object in list_objects:
            print(object)

    def download(self, key_file, folder_save=None, path_save=None):
        if path_save is None:
            if folder_save is None:
                raise ValueError("you must set one of folder_save or path_save")

            filename = key_file.split('/')[-1]
            folder_s3, filename = os.path.split(filename)
            # FOLDER_DATA_CLOUD = pjoin(FOLDER_PROJECT, 'data', 's3', 'wedoflytnow')
            if not os.path.exists(folder_save):
                os.makedirs(folder_save)
            path_save = pjoin(folder_save, filename)

        self.bucket.download_file(key_file, path_save)

    def upload(self, path_file, key_file=None, key_folder=None):
        folder_file_local, filename_local = os.path.split(path_file)
        if key_file is None:
            if key_folder is None:
                raise ValueError("you must set one of folder_file_s3 or path_file_s3")

            filename_s3 = filename_local
            key_file = f'{key_folder}/{filename_s3}'

        # Upload the file
        try:
            # response = self.bucket.upload_file(filename=path_file_local, bucket=self.bucket_name, key=path_file_s3)
            response = self.bucket.upload_file(path_file, key_file)
        except ClientError as e:
            logging.error(e)

    def upload_folder(self, folder_file, key_folder, debug=False):
        pathSearchExtend = os.path.join(folder_file, '**/{}')
        stringSearch = '*.JPG'
        listFileAll = glob.glob(pathSearchExtend.format(stringSearch), recursive=True)

        folder_file_name = folder_file.split('/')[-1]
        for path_file in listFileAll:
            key_folder2 = f'{key_folder}/{folder_file_name}'
            if debug:
                print(f'uploading {path_file} to {key_folder2} ...')
            self.upload(path_file=path_file, key_folder=key_folder2)

    def copy(self, key_file_src, key_file_dst):
        copy_source = {"Bucket": self.bucket_name,
                       "Key": key_file_src}

        self.bucket.copy(copy_source, key_file_dst)

    # def copy_list(self, list_key_file_src, key_folder_dst, filter_name=None):
    #     list_copy_source = []
    #     for key_file_src in list_key_file_src:
    #         is_file_pass = filer_filename_from_path(key_file_src, filter_name)
    #         if not is_file_pass:
    #             continue
    #
    #         copy_source = {"Bucket": self.bucket_name,
    #                        "Key": key_file_src}
    #         list_copy_source.append(copy_source)
    #
    #     destination_bucket = f'{self.bucket_name}/copy_multiples'
    #
    #     copy_sources = {
    #         'Objects': list_copy_source,
    #         'Quiet': True
    #     }
    #
    #     s3 = self.session.resource('s3')
    #     response = s3.copy_objects(
    #         CopySource=copy_sources,
    #         Bucket=destination_bucket
    #     )

    def copy_objects2folder(self, list_key_src, key_folder_dst, filter_name=None):
        for key_path_src in list_key_src:
            key_folder_src, filename_src = os.path.split(key_path_src)
            is_file_pass = filer_filename_from_path(key_path_src, filter_name)
            if not is_file_pass:
                continue
            key_path_dst = f'{key_folder_dst}/{filename_src}'

            self.copy(key_path_src, key_path_dst)

    def get_file(self, key_file):
        s3 = self.session.resource('s3')
        response = s3.Bucket(self.bucket_name).Object(key_file).get()
        # Read the content of the object
        data = response['Body'].read()

        return data

    def get_image(self, key_file_img, color_format='color'):
        data = self.get_file(key_file_img)
        np_array = np.frombuffer(data, np.uint8)
        if color_format == 'color':
            index_decode = cv2.IMREAD_COLOR
        else:
            raise ValueError(f'list support = color, ')

        img = cv2.imdecode(np_array, index_decode)

        return img

    def put(self, data, key_file):
        s3 = self.session.resource('s3')
        response = s3.Bucket(self.bucket_name).Object(key_file).put(Body=data)

    def delete(self, list_key_file):
        if isinstance(list_key_file, list):
            if len(list_key_file) == 0:
                return

            list_objects_delete = []
            for key_file in list_key_file:
                dict_object_delete = {"Key": key_file}
                list_objects_delete.append(dict_object_delete)

        elif isinstance(list_key_file, str):
            list_objects_delete = list_key_file

        dict_delete = {
            "Objects": list_objects_delete
        }
        respones = self.bucket.delete_objects(Delete=dict_delete)

    def delete_folder(self, key_folder):
        list_key_file = self.list_objects_in_folder(key_folder)

        if len(list_key_file) > 0:
            self.delete(list_key_file)

    # def move(self):


def isFolderEmpty(list_object):
    is_empty = False
    if len(list_object) == 1:
        if not '.' in list_object[0]:
            is_empty = True

    return is_empty

def filter_list_objects(list_object, prefix):
    if not prefix is None:
        prefix = prefix + "/"
        list_object = list_object.filter(Prefix=prefix)

    return list_object

def object2list(list_object):
    list_file_all = []

    for my_bucket_object in list_object:
        object_name = my_bucket_object.key
        list_file_all.append(object_name)

    return list_file_all

def filer_filename_from_path(path_file, filter_name=None):
    key_folder, filename = os.path.split(path_file)
    is_file_pass = True
    if filter_name is not None:
        is_file_pass = (filter_name in filename)

    return is_file_pass

import os
import boto3

class AwsSession:
    def __init__(self, authen_by, credentials, region_name='ap-southeast-1'):
        self.create_session(authen_by, credentials, region_name)

    def create_session(self, authen_by='production', credentials=None, region_name='ap-southeast-1'):
        if authen_by == 'profile':
            self.session = boto3.Session(profile_name=credentials, region_name=region_name)  # has permission only write

        elif authen_by == '.env':
            from dotenv import load_dotenv
            load_dotenv()

            self.session = boto3.Session(
                region_name=os.getenv('region_name'),
                aws_access_key_id=os.getenv('aws_access_key_id'),
                aws_secret_access_key=os.getenv('aws_secret_access_key')
            )

        elif authen_by == 'role':
            self.session = boto3.Session(region_name=region_name)
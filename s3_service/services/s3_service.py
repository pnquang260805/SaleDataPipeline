import os
from typing import List

import boto3
from dotenv import load_dotenv

load_dotenv()


class S3Service:
    def __init__(self):
        self.s3 = boto3.resource("s3",
                                 aws_access_key_id=os.getenv("ACCESS_KEY"),
                                 aws_secret_access_key=os.getenv("SECRET_KEY"),
                                 endpoint_url="http://minio:9000"
                                 )

    def get_objects(self, bucket_name: str, prefix: str) -> List[str]:
        bucket = self.s3.Bucket(bucket_name)
        return [obj.key for obj in bucket.objects.filter(Prefix=prefix)]

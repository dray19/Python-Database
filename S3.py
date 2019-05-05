import boto3 as boto
import botocore as botoc
class S3_CLASS:
    bucket_name = ''
    def __init__(self, bucket_name):
        client = boto.client('s3')
        self.client = client
        self.bucket_name = bucket_name
    def create_bucket(self, bucket_name):
        client = self.client
        try:
            response = client.head_bucket(
                Bucket=bucket_name)
        except botoc.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                response = client.list_buckets()
                buckets = response['Buckets']
                response = client.create_bucket(
                    Bucket=self.bucket_name)
            if response['Location']:
                return True
            else:
                return False
    def delete_bucket(self, bucket_name=''):
        client = self.client
        if bucket_name == '':
            bucket_name = self.bucket_name
        client.delete_bucket(
            Bucket=bucket_name)
    def put_object(self, file, key):
        client = self.client
        client.upload_file(key,self.bucket_name,file)
    def delete_object(self, key):
        client = self.client
        client.delete_object(
            Key=key,
            Bucket=self.bucket_name
        )


bucket_1 = S3_CLASS('drazenzack-bucket')
bucket_1.create_bucket('drazenzack-bucket')
files = ['product.csv', 'customer.csv', 'category.csv', 'region.csv', 'salestransaction.csv', 'soldvia.csv','store.csv', 'vendor.csv']
for i in files:
    bucket_1.put_object(file=i, key=i )

#for i in files:
#   bucket_1.delete_object(key=i)

#bucket_1.delete_bucket('drazenzack-bucket')


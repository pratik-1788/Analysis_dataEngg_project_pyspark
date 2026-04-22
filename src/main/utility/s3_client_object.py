import boto3


class S3ClientProvider:
    def __init__(self,access_key=None,secret_key=None):
        self.access_key=access_key
        self.secret_key=secret_key
        self.session=boto3.Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )
        self.s3_client=self.session.client('s3')

    def get_s3_client(self):
        return self.s3_client
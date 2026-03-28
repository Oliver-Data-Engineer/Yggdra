import boto3
from botocore.exceptions import BotoCoreError, ClientError


aws_access_key_id="AKIA5ER5WRKSIBOEKW77"
aws_secret_access_key="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"


class AWSClient:
    def __init__(self, service_name, region_name="us-east-2"):
        self.session = boto3.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        self.region = region_name
        self.client = self.session.client(service_name, region_name=region_name )
        self._account_id = None

    @property
    def account_id(self):
        """Permite acessar como self.account_id em vez de self.get_account_id()"""
        if self._account_id is None:
            sts = self.session.client("sts")
            self._account_id = sts.get_caller_identity()["Account"]
        return self._account_id
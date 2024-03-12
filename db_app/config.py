import os

import dotenv
import boto3

dotenv.load_dotenv()


db_config = {
    "connections": {
        "default": (
            f"postgres://"
            f"{os.getenv('pg_user')}:{os.getenv('pg_user_password')}"
            f"@{os.getenv('pg_db_host')}:{os.getenv('pg_host_port')}"
            f"/{os.getenv('pg_db_name')}"
        ),
    },
    "apps": {
        "models": {
            "models": ["db_app.models"],
            "default_connection": "default",
        }
    },
}

accounts_config = [
    #first_account
    {
        "aws_access_key_id": os.getenv("aws_access_key_id1"),
        "aws_secret_access_key": os.getenv("aws_secret_access_key1"),
        "region_name": os.getenv("aws_region_name1")
    },
    #second_account
    {
        "aws_access_key_id": os.getenv("aws_access_key_id2"),
        "aws_secret_access_key": os.getenv("aws_secret_access_key2"),
        "region_name": os.getenv("aws_region_name5")
    },
    {
        "aws_access_key_id": os.getenv("aws_access_key_id2"),
        "aws_secret_access_key": os.getenv("aws_secret_access_key2"),
        "region_name": os.getenv("aws_region_name6")
    },
    {
        "aws_access_key_id": os.getenv("aws_access_key_id2"),
        "aws_secret_access_key": os.getenv("aws_secret_access_key2"),
        "region_name": os.getenv("aws_region_name4")
    },
    {
        "aws_access_key_id": os.getenv("aws_access_key_id2"),
        "aws_secret_access_key": os.getenv("aws_secret_access_key2"),
        "region_name": os.getenv("aws_region_name7")
    },
]


aws_clients_ec2 = [
    boto3.client(
        'ec2',
        aws_access_key_id=account.get("aws_access_key_id"),
        aws_secret_access_key=account.get("aws_secret_access_key"),
        region_name=account.get("region_name")
    ) for account in accounts_config
]

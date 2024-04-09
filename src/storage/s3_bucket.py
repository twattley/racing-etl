from io import BytesIO

import boto3
import pandas as pd

from src.utils.logging_config import I


class DigitalOceanSpacesHandler:
    def __init__(
        self,
        access_key_id,
        secret_access_key,
        region_name="fra1",
        endpoint_url="https://fra1.digitaloceanspaces.com",
        bucket_name="racehorse-database-backup",
    ):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self.bucket_name = bucket_name
        self.session = boto3.session.Session()
        self.client = self.session.client(
            "s3",
            region_name=self.region_name,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
        )

    def upload_df_as_parquet(self, df, object_path):
        """
        Upload a pandas DataFrame to DigitalOcean Spaces as a Parquet file.
        """
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        try:
            self.client.put_object(
                Bucket=self.bucket_name, Key=object_path, Body=parquet_buffer.getvalue()
            )
            I(f"DataFrame uploaded to {self.bucket_name}/{object_path}.")
            return True
        except Exception as e:
            I(
                f"Failed to upload DataFrame to {self.bucket_name}/{object_path}. Error: {e}"
            )
            return False

    def upload_sql_file(self, file_path, object_path):
        """
        Upload a SQL file to DigitalOcean Spaces.
        """
        try:
            with open(file_path, "rb") as file:
                self.client.put_object(
                    Bucket=self.bucket_name, Key=object_path, Body=file
                )
            print(f"SQL file uploaded to {self.bucket_name}/{object_path}.")
            return True
        except Exception as e:
            print(
                f"Failed to upload SQL file to {self.bucket_name}/{object_path}. Error: {e}"
            )
            return False

    def download_df_from_parquet(self, object_path):
        """
        Download a Parquet file from DigitalOcean Spaces and load it into a pandas DataFrame.
        """
        try:
            parquet_object = self.client.get_object(
                Bucket=self.bucket_name, Key=object_path
            )
            parquet_content = parquet_object["Body"].read()

            df = pd.read_parquet(BytesIO(parquet_content))
            I(f"DataFrame loaded from {self.bucket_name}/{object_path}.")
            return df
        except Exception as e:
            I(
                f"Failed to download DataFrame from {self.bucket_name}/{object_path}. Error: {e}"
            )
            return None

    def download_folder(self, prefix):
        concatenated_df = pd.DataFrame()

        continuation_token = None
        while True:
            list_kwargs = {
                "Bucket": self.bucket_name,
                "Prefix": prefix,
            }
            if continuation_token:
                list_kwargs["ContinuationToken"] = continuation_token

            response = self.client.list_objects_v2(**list_kwargs)
            files = [
                obj["Key"]
                for obj in response.get("Contents", [])
                if obj["Key"].endswith("parquet")
            ]

            for file_key in files:
                file_obj = self.client.get_object(Bucket=self.bucket_name, Key=file_key)
                file_content = file_obj["Body"].read()
                df = pd.read_parquet(BytesIO(file_content))

                concatenated_df = (
                    df
                    if concatenated_df.empty
                    else pd.concat([concatenated_df, df], ignore_index=True)
                )

            if not response.get("IsTruncated"):
                break
            continuation_token = response.get("NextContinuationToken")

        return concatenated_df

    def delete_files(self, prefix):
        """
        Delete all files under a specific prefix in the given bucket.
        """
        response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        for obj in response.get("Contents", []):
            self.client.delete_object(Bucket=self.bucket_name, Key=obj["Key"])
            I(f"Deleted {obj['Key']}")

    def process_folder(self, prefix, upload_path):
        """
        Download all data under a specified prefix, concatenate, delete the files,
        and then upload the concatenated DataFrame.
        """

        concatenated_df = self.download_folder(prefix)
        if concatenated_df.empty:
            I("No data found to process.")
            return
        concatenated_df = concatenated_df.sort_values(
            "created_at", ascending=False
        ).drop_duplicates(subset=["unique_id"])

        self.delete_files(prefix)

        if self.upload_df_as_parquet(concatenated_df, upload_path):
            I(f"Uploaded concatenated DataFrame to {upload_path}")
        else:
            I("Failed to upload concatenated DataFrame.")

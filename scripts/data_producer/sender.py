import boto3
import time
from botocore.exceptions import ClientError, EndpointConnectionError
from scripts.data_producer.config import STREAM_NAME, AWS_REGION, LOCALSTACK_ENDPOINT,AWS_ACCES_KEY_ID,AWS_SECRET_ACCESS_KEY


def get_kinesis_client():
    try:
        client = boto3.client(
            "kinesis",
            region_name=AWS_REGION,
            endpoint_url=LOCALSTACK_ENDPOINT,
            aws_access_key_id=AWS_ACCES_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            verify=False
        )
        print(f"[INFO] [INIT] Kinesis client initialized successfully at {LOCALSTACK_ENDPOINT}")
        return client 
    except Exception as e:
        print(f"[ERROR] [INIT] Failed to initialize Kinesis client: {e}")
        return None

def send_records(kinesis_client, records, max_retries=3):

    attempt = 0
    records_to_send = records

    while attempt <= max_retries and len(records_to_send) > 0:
        try:
            response = kinesis_client.put_records(
                StreamName=STREAM_NAME,
                Records=records_to_send
            )
            
            failed_count = response["FailedRecordCount"]

            if failed_count == 0:
                print(f"[INFO] [WRITE] Batch sent successfully: {len(records_to_send)} records OK.")
                return True
            
            print(f"[WARN] [RETRY] {failed_count} records failed. Retrying (Attempt {attempt+1}/{max_retries})")

            next_retry_list = []
            for i, result in enumerate(response["Records"]):
                if "ErrorCode" in result:
                    next_retry_list.append(records_to_send[i])
            
            records_to_send = next_retry_list
            attempt += 1
            sleep_time = 0.5 * (2 ** (attempt - 1))
            time.sleep(sleep_time)

        except (ClientError, EndpointConnectionError) as e:
            print(f"[ERROR] [CONNECTION] Error on attempt {attempt}: {e}")
            attempt += 1
            time.sleep(1)

    if len(records_to_send) > 0:
        print(f"[CRIT] [LOST] {len(records_to_send)} records could not be sent after {max_retries} retries.")
        return False
        
    return True
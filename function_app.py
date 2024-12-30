import logging
from azure.storage.blob import BlobClient
import pandas as pd
import numpy as np
from io import BytesIO
import snowflake.connector
import azure.functions as func
import os
import json

app = func.FunctionApp()

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Content-Type": "application/json"
}

SNOWFLAKE_CREDS = {
    "user": os.getenv("SNOWFLAKE_USER", "Raghu"),
    "password": os.getenv("SNOWFLAKE_PASSWORD", "Katalyst@123"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT", "zxmrpxw-ed90668"),
    "insecure_mode": False,
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
}

@app.function_name(name="HttpTrigger")
@app.route(route="upload", methods=["GET", "POST", "OPTIONS"])
def main(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return func.HttpResponse(json.dumps({"message": "OK"}), status_code=200, headers=CORS_HEADERS)

    try:
        request_body = req.get_json()
        blob_name = request_body.get('blobName')
        database = request_body.get('databaseName')
        table = request_body.get('tableName')
        schema = 'PUBLIC'

        if not all([blob_name, database, table]):
            return func.HttpResponse(
                json.dumps({"error": "Missing required parameters"}),
                status_code=400,
                headers=CORS_HEADERS
            )

        blob_url = f"https://mifiddatainjection.blob.core.windows.net/datainjection/datainjection/{blob_name}?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-12-31T00:41:21Z&st=2024-12-30T16:41:21Z&spr=https,http&sig=RIu1XPXEl%2B3PikN1ZAWEEhhgXYMerfGTx5EN1SkcE5o%3D"
        blob_client = BlobClient.from_blob_url(blob_url)
        blob_data = blob_client.download_blob().readall()

        if blob_name.endswith('.csv'):
            data = pd.read_csv(BytesIO(blob_data))
        elif blob_name.endswith(('.xlsx', '.xls')):
            data = pd.read_excel(BytesIO(blob_data))
        else:
            return func.HttpResponse(
                json.dumps({"error": "Unsupported file format"}),
                status_code=400,
                headers=CORS_HEADERS
            )

        # Handle NaN values
        data = data.replace({np.nan: None})
        data.columns = [str(col).strip().upper().replace(' ', '_') for col in data.columns]
        
        conn = snowflake.connector.connect(**SNOWFLAKE_CREDS)
        cursor = conn.cursor()
        
        cursor.execute(f'USE WAREHOUSE "{SNOWFLAKE_CREDS["warehouse"]}"')
        cursor.execute(f'CREATE DATABASE IF NOT EXISTS "{database}"')
        cursor.execute(f'USE DATABASE "{database}"')
        cursor.execute(f'USE SCHEMA "{schema}"')

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS "{table}" (
            {', '.join([f'"{col}" VARCHAR' for col in data.columns])}
        )
        """
        cursor.execute(create_table_query)

        insert_query = f"""
        INSERT INTO "{table}" (
            {', '.join([f'"{col}"' for col in data.columns])}
        ) VALUES ({', '.join(['%s'] * len(data.columns))})
        """

        # Convert DataFrame to list of tuples for insertion
        rows = data.replace({np.nan: None}).values.tolist()
        for row in rows:
            cursor.execute(insert_query, row)

        conn.commit()
        cursor.close()
        conn.close()

        return func.HttpResponse(
            json.dumps({"message": "Data uploaded successfully"}),
            status_code=200,
            headers=CORS_HEADERS
        )

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            headers=CORS_HEADERS
        )
from google.cloud import bigquery

client = bigquery.Client()
table_id = "coffee-shop-project-483310.coffee_shop.transactions_raw"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
)

with open(r"C:\Users\kenne\Documents\Business Stuffs\Coffee stuffs - for data engineering - postman method\docs\response2.ndjson", "rb") as f:
    job = client.load_table_from_file(f, table_id, job_config=job_config)

job.result()
print("Data loaded successfully!")

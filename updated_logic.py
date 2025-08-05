from google.cloud import bigquery, storage
import pandas as pd
import numpy as np
import uuid
import json
from datetime import datetime
from io import StringIO

# Constants
SOURCE_CSV_GCS_PATH = "gs://duohealth_2025/source_data/lastSuspects.csv"
MEMBER_INFO_TABLE = "customerpov-defe0.DuoHealth.member_info"
DEST_TABLE = "customerpov-defe0.DuoHealth.OpenMemberGaps"
BASE_CHART_PATH = "gs://duohealth_2025/results/tocChartsNewAcute/"

# Clients
bq_client = bigquery.Client()
gcs_client = storage.Client()

def extract_chart_ids(chartToPages_raw):
    chart_ids = []
    if chartToPages_raw is not None:
        try:
            if isinstance(chartToPages_raw, (np.ndarray, list)):
                for item in chartToPages_raw:
                    if isinstance(item, np.ndarray):
                        for subitem in item:
                            filename = subitem.get("filename")
                            if filename:
                                chart_ids.append(filename)
                    elif isinstance(item, dict):
                        filename = item.get("filename")
                        if filename:
                            chart_ids.append(filename)
            elif isinstance(chartToPages_raw, str):
                chart_data = json.loads(chartToPages_raw)
                for chart in chart_data.get("chartToPages", []):
                    filename = chart.get("filename")
                    if filename:
                        chart_ids.append(filename)
            elif isinstance(chartToPages_raw, dict):
                for chart in chartToPages_raw.get("chartToPages", []):
                    filename = chart.get("filename")
                    if filename:
                        chart_ids.append(filename)
        except Exception as e:
            print(f"Warning: error parsing chartToPages: {e}")
    return ",".join(chart_ids)

def flatten_rows(member_id, group_df, now):
    rows = []
    timestamp_val = now.strftime("%Y-%m-%d %H:%M:%S")
    batchid_val = f"duohealth.allymar_{now.strftime('%m%d%Y%H%M')}"

    for (gap_id, model_code), hcc_group in group_df.groupby(["gapId", "modelCode"]):
        if pd.isna(model_code):
            continue

        for _, row in hcc_group.iterrows():
            try:
                dos = pd.to_datetime(row.get("lastRecorded")).strftime("%Y-%m-%d")
            except Exception:
                dos = row.get("lastRecorded")

            rows.append({
                "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, member_id)),
                "memberId": member_id,
                "timestamp": timestamp_val,
                "batchid": batchid_val,
                "source": "Allymar",
                "gapId": gap_id,
                "gapType": row.get("gapType"),
                "hccCode": int(model_code),
                "hccDescription": row.get("modelDescription"),
                "hccModel": "CMS-HCC",
                "hccModelVersion": int(row["modelVersion"]) if pd.notna(row.get("modelVersion")) else None,
                "yos": int(now.year),
                "icdCode": row.get("diagnosisCode"),
                "icdDescription": row.get("diagnosisDescription"),
                "lastRecordedDos": dos,
                "lastRecordedNpi": row.get("lastRecordedNPI"),
                "lastRecordedProviderName": row.get("lastRecordedProviderName"),
                "notes": row.get("notes"),
                "rafScore": float(row["rafScore"]) if pd.notna(row.get("rafScore")) else None,
                "chartIds": extract_chart_ids(row.get("chartToPages"))
            })

    return rows

def convert_numpy_types(data):
    def convert(obj):
        if isinstance(obj, np.generic):
            return obj.item()
        return obj
    return [{k: convert(v) for k, v in row.items()} for row in data]

def main():
    now = datetime.now()

    print("Reading source CSV from GCS...")
    bucket_name = SOURCE_CSV_GCS_PATH.split("/")[2]
    path_inside_bucket = "/".join(SOURCE_CSV_GCS_PATH.split("/")[3:])
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(path_inside_bucket)
    csv_data = blob.download_as_text()
    df_suspects = pd.read_csv(StringIO(csv_data))

    print("Reading member_info table from BigQuery...")
    df_members = bq_client.query("SELECT memberId FROM `{}`".format(MEMBER_INFO_TABLE)).to_dataframe()

    df_suspects["memberId"] = df_suspects["memberId"].astype(str)
    df_members["memberId"] = df_members["memberId"].astype(str)

    print("Merging on memberId...")
    df_joined = df_suspects.merge(df_members, on="memberId", how="left")

    print("Filtering and deduplicating...")
    df_joined = df_joined[df_joined["memberId"].notnull()]
    df_joined = df_joined.drop_duplicates(subset=["memberId", "gapId", "diagnosisCode", "lastRecorded"])

    print("Grouping and transforming...")
    flat_rows = []
    for member_id, group in df_joined.groupby("memberId"):
        flat_rows.extend(flatten_rows(member_id, group, now))

    if not flat_rows:
        print("No data to load.")
        return

    print("Converting NumPy types to native Python types...")
    flat_rows = convert_numpy_types(flat_rows)

    print(f"Loading {len(flat_rows)} rows into BigQuery table: {DEST_TABLE}")
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    job = bq_client.load_table_from_json(flat_rows, DEST_TABLE, job_config=job_config)
    job.result()

    print("Done. Data loaded to BigQuery.")

if __name__ == "__main__":
    main()

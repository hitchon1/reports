import requests
import json
import pandas as pd
from io import BytesIO
from time import sleep
#from creds import credentials
from get_access_key import get_access_toke

def create_report(access_token, endpoint, marketplace_id, report_type):
    create_report_endpoint = f"{endpoint}/reports/2021-06-30/reports"
    report_details = {
        "reportType": report_type,
        "marketplaceIds": [marketplace_id],
    }
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json",
    }
    response = requests.post(create_report_endpoint, headers=headers, data=json.dumps(report_details))
    if response.status_code in [200, 202]:
        return response.json()['reportId']
    else:
        raise Exception(f"Failed to create report. Status code: {response.status_code}, Response: {response.text}")

def check_report_status(access_token, endpoint, report_id):
    sleep(10)
    get_report_endpoint = f"{endpoint}/reports/2021-06-30/reports/{report_id}"
    headers = {"x-amz-access-token": access_token}
    response = requests.get(get_report_endpoint, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to retrieve report details. Status code: {response.status_code}, Response: {response.text}")

def get_report_document(access_token, endpoint, report_document_id):
    get_report_document_endpoint = f"{endpoint}/reports/2021-06-30/documents/{report_document_id}"
    response = requests.get(get_report_document_endpoint, headers={"x-amz-access-token": access_token})
    if response.status_code == 200:
        report_document_details = response.json()
        return report_document_details['url']  # URL to download the report
    else:
        raise Exception(f"Failed to retrieve report document details. Status code: {response.status_code}, Response: {response.text}")

def download_report(download_url):
    report_response = requests.get(download_url)
    if report_response.status_code == 200:
        try:
            df = pd.read_csv(BytesIO(report_response.content), encoding='utf-8', sep='\t')
        except UnicodeDecodeError:
            df = pd.read_csv(BytesIO(report_response.content), encoding='ISO-8859-1', sep='\t')
        return df
    else:
        raise Exception(f"Failed to download the report. Status code: {report_response.status_code}")

# Main workflow
    

def make_report(report_type_, brand_id):
    sleep(300)

    access_token = get_access_toke(brand_id)
    endpoint = "https://sellingpartnerapi-na.amazon.com"
    marketplace_id = "ATVPDKIKX0DER"
    report_type = report_type_

    try:
        report_id = create_report(access_token, endpoint, marketplace_id, report_type)
        print(f"Report ID: {report_id}")
        
        # Wait and check the status of the report
        while True:
            sleep(10)
            report_details_response = check_report_status(access_token, endpoint, report_id)
            if report_details_response.get('processingStatus') in ['DONE', 'CANCELLED', 'FATAL']:
                break
            print("Waiting for report to be ready...")
            sleep(180)  # Check every 60 seconds
        
        if report_details_response.get('processingStatus') == 'DONE':
            report_document_id = report_details_response['reportDocumentId']
            download_url = get_report_document(access_token, endpoint, report_document_id)
            df = download_report(download_url)
            df['brandid'] = brand_id
            df['marketplace_id'] = marketplace_id
            print("Report loaded into DataFrame successfully.")
            return df
        else:
            print("Report processing was not successful.")
    except Exception as e:
        print(e)

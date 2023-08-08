import sys
import time
import requests
import json
import boto3
# import pandas as pd
from datetime import datetime, timedelta
from . import exception_handling
# from source_dsp_campaign_report import dsp_configfile


# s3 = boto3.client('s3')
# s3_client = boto3.client("s3")



class Report:

    def __init__(self, report_type, format, time_unit, dimensions, metrics,report_start_date,report_end_date):

        self.report_type = report_type
        self.format = format
        self.time_unit = time_unit
        self.dimensions = dimensions
        self.metrics = metrics
        self.report_start_date=report_start_date
        self.report_end_date = report_end_date


    def __str__(self):

        return (
            f"\nReport Type: {self.report_type}\n"
            f"Format: {self.format}\n"
            f"Time Unit: {self.time_unit}\n"
            f"Dimensions: {self.dimensions}\n"
            f"Metrics: {self.metrics}\n"
        )


class Credentials:

    def __init__(self, refresh_token, client_id, client_secret, access_token=""):

        self.refresh_token = refresh_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token

    def __str__(self):

        return (
            f"Refresh Token: {self.refresh_token}\n"
            f"\nClient ID: {self.client_id}\n"
            f"\nClient Secret: {self.client_secret}\n"
            f"\nAccess Token: {self.access_token}\n"
        )


class Advertiser:

    def __init__(self, advertiser_name, advertiser_id, region, country, aws_region, base_url):

        self.advertiser_name = advertiser_name
        self.advertiser_id = advertiser_id
        self.region = region
        self.country = country
        self.aws_region = aws_region
        self.base_url = base_url

    def __str__(self):

        return (
            f"\nCustomer ID: {self.advertiser_name}\n"
            f"Account Info ID (Entity ID): {self.advertiser_id}\n"
            f"Region: {self.region}\n"
            f"Country: {self.country}\n"
            f"AWS Region: {self.aws_region}\n"
            f"Base URL: {self.base_url}\n"
        )

    def __repr__(self):
        return str(self)



def get_access_token(credentials):
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    data = {
        "grant_type": "refresh_token",
        "client_id": credentials.client_id,
        "refresh_token": credentials.refresh_token,
        "client_secret": credentials.client_secret
    }

    response = requests.post(
        "https://api.amazon.com/auth/o2/token", headers=headers, data=data)
    r_json = response.json()
    json_object = json.dumps(r_json, indent=4)
    print(1)
    # write_authtokenfile(json_object)
    # print(r_json["access_token"])
    return r_json["access_token"]

# status code handler function

# def write_authtokenfile(r):
#     with open("source_dsp_campaign_report/dspauth.json", "w") as f:
#         f.write(r)
# def write_reportidfile(k):
#     with open("source_dsp_campaign_report/dspreportid.json", "w") as f:
#         f.write(k)
# def write_finalfile(k):
#     # with open("source_dsp_campaign_report/dspfinalfile.json", "w") as f:
#     #     f.write(k)

def handle_status_codes(response,advertiser,campaign_num,report_config):
    try:
        if response.status_code == 429:
            print(
                f"Error {response.status_code}:\nToo many requests... Retrying...\n")
            return -1
        elif response.status_code == 500 or response.status_code == 504:
            print(
                f"Error {response.status_code}:\nError occured due to possible intermittent issue... Retrying...\n")
            return -1
        elif response.status_code // 100 == 2:
            return 1
        else:
            response.raise_for_status()
    except requests.HTTPError as e:
        exception_handling.logger.info(f"Error {response.status_code}:{e}")
        exception_handling.log_file_creation()
        # session = boto3.session.Session(profile_name="ann04-sandbox-poweruser")
        session = boto3.session.Session()
        # glue_client = session.client('glue')
        # response = glue_client.get_job_runs(JobName=jobname)
        dynamodb = session.resource('dynamodb',region_name='us-east-1')
        table = dynamodb.Table('dsp_status_manager')
        job_attribute={
            "Report_type":"Campaign",
            "Date":datetime.strftime(datetime.now(),"%Y-%m-%d"),
            "reportId": None,
            "advertiser": {'advertiser_id': advertiser.advertiser_id, 'base_url':advertiser.base_url},
            "status":"FAILED",
            "Report_num":advertiser.advertiser_id+f"{'_'}"+f"{campaign_num}",
            "start_date":report_config.report_start_date,
            "end_date":report_config.report_end_date
        }
        table.put_item(Item=job_attribute)


        sys.exit(f"ERROR: {e}\n")

# Create a DSP report and recieve report generation status


def create_report_and_get_reportid(credentials, advertiser, report_config,campaign_num):
    print(advertiser)
    headers = {
        "Amazon-Advertising-API-ClientId": credentials.client_id,
        "Authorization": "Bearer " + credentials.access_token,
        "Content-Type": "application/json",
        "Accept": "application/vnd.dspcreatereports.v3+json"
    }
    # print(headers)

    data = {
        "startDate": report_config.report_start_date,
        "endDate": report_config.report_end_date,
        "format": report_config.format,
        "timeUnit": report_config.time_unit,
        "type": report_config.report_type,
        "dimensions": report_config.dimensions,
        "metrics": report_config.metrics,
    }
    print(data)
    url = (
        f"{advertiser.base_url}/accounts/{advertiser.advertiser_id}/dsp/reports")

    while True:
        response = requests.post(url, headers=headers, json=data)
        status = handle_status_codes(response,advertiser,campaign_num,report_config)
        if status == -1:
            time.sleep(3)
        else:
            r_json = response.json()
            json_object = json.dumps(r_json, indent=4)
            # write_reportidfile(json_object)

            print("Requesting DSP Report...\n"
                  f"Status Code: {response.status_code}\n"
                  f"Report Status: {r_json['status']}\n"
                  f"Report ID: {r_json['reportId']}\n")
            return r_json["reportId"]


def download_report(credentials, advertiser, report_id,campaign_num,report_config):

    headers = {
        "Amazon-Advertising-API-ClientId": credentials.client_id,
        "Authorization": "Bearer " + credentials.access_token,
        "Content-Type": "application/json",
        "Accept": "application/vnd.dspgetreports.v3+json",
        "Accept-Encoding": "gzip, deflate, br"
    }

    api_url = (
        f"{advertiser.base_url}/accounts/{advertiser.advertiser_id}/dsp/reports/{report_id}")
    start_time = time.time()
    while True:
        response = requests.get(api_url, headers=headers)
        status = handle_status_codes(response,advertiser,campaign_num,report_config)
        if status == -1:
            time.sleep(4)
        else:
            r_json = response.json()
            if r_json["status"] == "IN_PROGRESS":
                print("DSP Report Generation In Progress...\n"
                      f"Status Code: {response.status_code}\n"
                      f"Report Status: {r_json['status']}\n"
                      f"Report ID: {r_json['reportId']}\n")
                time.sleep(4)
            elif r_json["status"] == "SUCCESS":
                print("DSP Report Generation Complete.\n")
                print(f"{r_json}\n")
                # json_object = json.dumps(r_json, indent=4)
                # write_finalfile(json_object)
                return r_json
            if time.time() - start_time >300:  # 300 seconds = 5 minutes
                print("5 minutes have passed. Exiting.")
                execute().logging_job_status(r_json,credentials, advertiser,campaign_num,report_config)
                exception_handling.logger.info("5 minutes have passed. Exiting.")
                exception_handling.log_file_creation()
                sys.exit("5 minutes have passed. Exiting.")
def download_report_2(credentials, advertiser, report_id,campaign_num,report_config):

    headers = {
        "Amazon-Advertising-API-ClientId": credentials.client_id,
        "Authorization": "Bearer " + credentials.access_token,
        "Content-Type": "application/json",
        "Accept": "application/vnd.dspgetreports.v3+json",
        "Accept-Encoding": "gzip, deflate, br"
    }

    api_url = (
        f"{advertiser['base_url']}/accounts/{advertiser['advertiser_id']}/dsp/reports/{report_id}")
    start_time = time.time()
    while True:
        response = requests.get(api_url, headers=headers)
        status = handle_status_codes(response,advertiser,campaign_num,report_config)
        if status == -1:
            time.sleep(4)
        else:
            r_json = response.json()
            if r_json["status"] == "IN_PROGRESS":
                print("DSP Report Generation In Progress...\n"
                      f"Status Code: {response.status_code}\n"
                      f"Report Status: {r_json['status']}\n"
                      f"Report ID: {r_json['reportId']}\n")
                time.sleep(4)
            elif r_json["status"] == "SUCCESS":
                print("DSP Report Generation Complete.\n")
                print(f"{r_json}\n")
                # json_object = json.dumps(r_json, indent=4)
                # write_finalfile(json_object)
                return r_json
            if time.time() - start_time > 300:  # 300 seconds = 5 minutes
                print("5 minutes have passed. Exiting.")
                # execute().logging_job_status(r_json,credentials, advertiser,campaign_num)
                exception_handling.logger.info("5 minutes have passed. Exiting.")
                exception_handling.log_file_creation()
                sys.exit("5 minutes have passed. Exiting.")


# Method to log the configuration file


def log_config_file(print_list):
    """
        Method to log DSP configuration
    """
    string_list = ["REPORT CONFIGURATION", "CREDENTIALS", "ADVERTISERS"]

    for i, item in enumerate(print_list):
        print(
            f"-------------------------------{string_list[i]}-------------------------------\n"
            f"{item}\n"
            # cust=json.dumps()
        )
    print(f"-----------------------------------------------------------------------\n")

# Main


def generate(advertiser, credentials, report_config,campaign_num):
    # print(report_config.report_date)
    print("this method is running")
    report_id = create_report_and_get_reportid(
        credentials, advertiser, report_config,campaign_num)

    return download_report(credentials, advertiser, report_id,campaign_num,report_config)




class execute():
    @staticmethod
    def logging_job_status(k,credentials,advertiser,campaign_num,report_config):
        # session = boto3.session.Session(profile_name="ann04-sandbox-poweruser")
        session = boto3.session.Session()
        # glue_client = session.client('glue')
        # response = glue_client.get_job_runs(JobName=jobname)
        dynamodb = session.resource('dynamodb',region_name='us-east-1')
        table = dynamodb.Table('dsp_status_manager')
        job_attribute={
            "Report_type":"Campaign",
            "Date":datetime.strftime(datetime.now(),"%Y-%m-%d"),
            "reportId": k['reportId'],
            "advertiser": {'advertiser_id': advertiser.advertiser_id, 'base_url':advertiser.base_url},
            "status":k['status'],
            "Report_num":advertiser.advertiser_id+f"{'_'}"+f"{campaign_num}",
            "start_date":report_config.report_start_date,
            "end_date":report_config.report_end_date
        }
        table.put_item(Item=job_attribute)
    @staticmethod
    def runfile(start_date,end_date,config,campaign_num,**kwargs):
        report_type=config['report_types']['report_type']
        # report_type = "campaign / inventory / audience / products / technology / geography / conversion_source"
        # if report_type == "campaign / inventory / audience / products / technology / geography / conversion_source":
        # if report_type == "campaign":
        report_config = Report(
            report_type.upper(),
            config["format"] if "format" in config else "CSV",
            config["time_unit"] if "time_unit" in config else "DAILY",
            config["report_types"]["dimensions"].split(","),
            config["report_types"]["reqbody"]["metrics"].split(","),
            start_date,end_date

        )
        # session = boto3.Session(profile_name="ann04-sandbox-poweruser")
        session = boto3.session.Session()
        dynamodb = session.resource('dynamodb',region_name='us-east-1')
        table = dynamodb.Table("dsp_status_manager")
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('Report_type').eq('Campaign') & boto3.dynamodb.conditions.Key('Report_num').eq(f"{config['advertisers']['advertiser_id']}{'_'}{campaign_num}")
        )
        # response = table.query(
        #     KeyConditionExpression=Key('YourPartitionKey').eq('YourValue') & Key('YourSortKey').eq('YourSortKeyValue')
        # )
        if (not response['Items']) or ((response['Items'][0]["status"] in ["SUCCESS","FAILED"]) or (response['Items'][0]["Date"]!=datetime.strftime(datetime.now(),"%Y-%m-%d"))):
            # with open('config.json', "r") as cf:
            #     config = json.load(cf)
            credentials = Credentials(
                config["refresh_token"],
                config["client_id"],
                config["client_secret"]
            )
            # print("sidddone************************************")
            credentials.access_token = get_access_token(credentials)
            print("done****************************")
            report_type=config['report_types']['report_type']
            # report_type = "campaign / inventory / audience / products / technology / geography / conversion_source"
            # if report_type == "campaign / inventory / audience / products / technology / geography / conversion_source":
            # if report_type == "campaign":
            report_config = Report(
                report_type.upper(),
                config["format"] if "format" in config else "CSV",
                config["time_unit"] if "time_unit" in config else "DAILY",
                config["report_types"]["dimensions"].split(","),
                config["report_types"]["reqbody"]["metrics"].split(","),
                start_date,end_date

            )

            advertisers = []
            # for customer in config["customers"]:
            if (config["advertisers"]["active"] == True):
                advertisers.append(Advertiser(
                    config["advertisers"]["advertiser_name"],
                    config["advertisers"]["advertiser_id"],
                    config["advertisers"]["region"],
                    config["advertisers"]["country"],
                    config["advertisers"]["aws_region"],
                    config["advertisers"]["url"]
                ))
                # print(report_config)
                # print(customers)

                # json_object = json.dumps(customers, indent=4)
                # print("**************************************************************")
                # print(json_object)
                # with open ("./source_dsp_report_downloader/dsp_customer.json", "w") as f:
                #   f.write(json_object)

                log_config_file([report_config, credentials, advertisers])


                date_format = '%Y-%m-%d'
                date='YYYY-MM-DD'
                start_date = 'YYYY-MM-DD'
                end_date = 'YYYY-MM-DD'

                if start_date != 'YYYY-MM-DD' and end_date != 'YYYY-MM-DD':

                    dsp_report_start_date = start_date
                    dsp_report_end_date = end_date
                    date_range = pd.date_range(dsp_report_start_date, dsp_report_end_date)

                    # TODO SUDO FOR NOW
                    # if len(date_range) > 2 months from todays date:
                    #     print(f'There is a limit of 20 days ({len(date_range)} days provided). We are trying to save you from a typo.')
                    #     sys.exit()0
                    # request amazon to create day report
                    for advertiser in advertisers:
                        for _report_date in date_range:
                            report_config.report_date = datetime.strftime(
                                _report_date, date_format)
                            generate(advertiser, credentials, report_config)
                # elif date != 'YYYY-MM-DD':
                #   generate(customer, credentials, report_config)
                else:
                    # for i in range(2):
                    #     s="2023-03-16"
                    #     d=datetime.strptime(s,date_format)
                    #     report_config.report_date = datetime.strftime(
                    #         d - timedelta(days=i + 1), date_format)
                    for advertiser in advertisers:
                        # report_config.report_start_date=report_start_date,
                        # Report.report_end_date = report_end_date
                        # report_config.report_date = datetime.strftime(
                        #     datetime.now() - timedelta(1), date_format)
                        gen= generate(advertiser, credentials, report_config,campaign_num)

        else:
            # response = table.query(
            #     KeyConditionExpression=boto3.dynamodb.conditions.Key('Report_type').eq('Campaign')
            # )
            # print(response['Items'][0]["Date"]==datetime.strftime(datetime.now(),"%Y-%m-%d"))
            client_id=config['client_id']
            report_id=response['Items'][0]['reportId']
            refresh_token=config['refresh_token']
            client_secret=config['client_secret']
            # advertiser_id=response['Items']['advertiser']['advertiser_id']
            # advertiser_url=response['Items']['advertiser']['advertiser_url']
            advertiser=response['Items'][0]['advertiser']
            credentials = Credentials(
                refresh_token,
                client_id,
                client_secret)

            access_token=get_access_token(credentials)
            credentials.access_token=access_token
            k=download_report_2(credentials, advertiser, report_id,campaign_num,report_config)
            # execute().logging_job_status(k,credentials,advertiser,campaign_num)
            # response = table.update_item(
            #     Key={
            #         "Report_type":"Campaign",
            #         "Report_num":advertiser['advertiser_id']+f"{'_'}"+f"{campaign_num}"  # If your table has a sort key
            #     },
            #     UpdateExpression='SET #s = :val1',
            #     ExpressionAttributeNames={
            #         '#s': 'status'
            #     },
            #
            #     ExpressionAttributeValues={
            #         ':val1': 'SUCCESS'
            #     }
            # )

            return k

        execute().logging_job_status(gen,credentials,advertiser,campaign_num,report_config)
        return gen

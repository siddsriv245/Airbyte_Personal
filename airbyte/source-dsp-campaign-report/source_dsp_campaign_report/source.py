#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
# import smart_open
import os
import sys
import logging
import boto3
from abc import ABC
import calendar
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.core import Stream, StreamData
import requests

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from . import dsp_downloader,valid_list,exception_handling
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator, NoAuth

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


# Basic full refresh stream
class DspCampaignReportStream(HttpStream, ABC):
    """
    TODO remove this comment

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class DspCampaignReportStream(HttpStream, ABC)` which is the current class
    `class Customers(DspCampaignReportStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(DspCampaignReportStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalDspCampaignReportStream((DspCampaignReportStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """

    # TODO: Fill in the url base. Required.
    url_base = ""

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        return None

    @exception_handling.exception(exception_handling.logger)
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        return {}
    @exception_handling.exception(exception_handling.logger)
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        yield from response.json()



class Campaigns_1(DspCampaignReportStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = None
    @exception_handling.exception(exception_handling.logger)
    def __init__(self, start_date,end_date,config: Mapping[str, Any], connector_num, **kwargs):
    # if config['report_types']['report_type'] == "campaign":
        super().__init__()
        self.config = config
        self.connector_num=connector_num
        self.response,self.table=dynamodbsetup_check(self.config,self.connector_num)
        self.start_date=start_date if (not self.response['Items']) or (self.response['Items'][0]["status"] in ["SUCCESS","FAILED"]) or (self.response['Items'][0]["Date"]!=datetime.strftime(datetime.now(),"%Y-%m-%d")) else self.response['Items'][0]['start_date']
        self.end_date=end_date if (not self.response['Items']) or (self.response['Items'][0]["status"]in ["SUCCESS","FAILED"]) or (self.response['Items'][0]["Date"]!=datetime.strftime(datetime.now(),"%Y-%m-%d")) else self.response['Items'][0]['end_date']
        print(self.config)



    @exception_handling.exception(exception_handling.logger)
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """

        self.k = dsp_downloader.execute.runfile(self.start_date, self.end_date, self.config,self.connector_num)
        return self.k['location']

    @exception_handling.exception(exception_handling.logger)
    def read_records(
            self,
            sync_mode: SyncMode,
            cursor_field: List[str] = None,
            stream_slice: Mapping[str, Any] = None,
            stream_state: Mapping[str, Any] = None,
    ) -> Iterable[StreamData]:
        yield from self._read_pages(
        lambda req, res, state, _slice: self.parse_response(res, stream_slice=_slice, stream_state=state), stream_slice, stream_state
        )

        self.response,self.table=dynamodbsetup_check(self.config,self.connector_num)
        if (not self.response['Items']) or ((self.response['Items'][0]["status"]!="SUCCESS")):
               dynamodbstatus_update(self.config,self.connector_num,self.response,self.table,"SUCCESS")
        #       response = self.table.update_item(
        #       Key={
        #         "Report_type":"Campaign",
        #         "Report_num":self.config['advertisers']['advertiser_id']+f"{'_'}"+f"{self.connector_num}"  # If your table has a sort key
        #     },
        #       UpdateExpression='SET #s = :val1',
        #       ExpressionAttributeNames={
        #         '#s': 'status'
        #     },
        #
        #       ExpressionAttributeValues={
        #         ':val1': 'SUCCESS'
        #     }
        # )
        #       print("Status done")
        #     else:
        #       pass






class Campaigns_2(DspCampaignReportStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = None

    @exception_handling.exception(exception_handling.logger)
    def __init__(self, start_date,end_date,config: Mapping[str, Any],connector_num, **kwargs):
    # if config['report_types']['report_type'] == "campaign":
        super().__init__()
        self.config = config
        self.connector_num=connector_num
        self.response,self.table=dynamodbsetup_check(self.config,self.connector_num)
        self.start_date=start_date if (not self.response['Items']) or (self.response['Items'][0]["status"] in ["SUCCESS","FAILED"]) else self.response['Items'][0]['start_date']
        self.end_date=end_date if (not self.response['Items']) or (self.response['Items'][0]["status"] in ["SUCCESS","FAILED"]) else self.response['Items'][0]['end_date']
        print(self.config)




    @exception_handling.exception(exception_handling.logger)
    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        self.k = dsp_downloader.execute.runfile(self.start_date, self.end_date, self.config,self.connector_num)
        return self.k['location']

    @exception_handling.exception(exception_handling.logger)
    def read_records(
            self,
            sync_mode: SyncMode,
            cursor_field: List[str] = None,
            stream_slice: Mapping[str, Any] = None,
            stream_state: Mapping[str, Any] = None,
    ) -> Iterable[StreamData]:
        yield from self._read_pages(
        lambda req, res, state, _slice: self.parse_response(res, stream_slice=_slice, stream_state=state), stream_slice, stream_state
        )
        self.response,self.table=dynamodbsetup_check(self.config,self.connector_num)
        if (not self.response['Items']) or ((self.response['Items'][0]["status"]!="SUCCESS")):
             dynamodbstatus_update(self.config,self.connector_num,self.response,self.table,"SUCCESS")
        else:
             pass





class SourceDspCampaignReport(AbstractSource):
    @exception_handling.exception(exception_handling.logger)
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        client_id = config['client_id']
        client_secret = config['client_secret']
        refresh_token = config['refresh_token']
        if (client_id not in valid_list.l['client_id']) and (client_secret not in valid_list.l['client_secret']) and (
            refresh_token not in valid_list.l['refresh_token']):
         return False, f"Input values entered are  invalid. Please input correct value"
        return True, None

    @exception_handling.exception(exception_handling.logger)
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.

        @exception_handling.exception(exception_handling.logger)
        def get_date_range(date_str):
          date_1 = datetime.strptime(date_str, "%Y-%m-%d")
          previous_month_start = (date_1.replace(day=1) - timedelta(days=1)).replace(day=1)
          year, month, _ = map(int, previous_month_start.strftime("%Y-%m-%d").split('-'))
          days_in_month = calendar.monthrange(year, month)[1]
          previous_month_end = previous_month_start.replace(day=15) + timedelta(days=(16 if days_in_month == 31 else 15))
          previous_month_last_day = previous_month_start.replace(day=1) - timedelta(days=1)

          if date_1.day > 16:
           start_date_1 = date_1.replace(day=1)
           end_date_1 = date_1.replace(day=15)
          elif date_1.day < 16 and ((date_1 - timedelta(days=1)).day == previous_month_end.day):
            start_date_1 = previous_month_start
            end_date_1 = previous_month_start.replace(day=15)
          else:
            start_date_1 = previous_month_start.replace(day=16)
            end_date_1 = date_1.replace(day=1) - timedelta(days=1)

          date_2 = datetime.strptime(date_str, "%Y-%m-%d")

          if date_2.day > 16:
            start_date_2 = date_2.replace(day=16)
            end_date_2 = date_2 - timedelta(days=1)

          elif date_2.day < 16 and ((date_2 - timedelta(days=1)).day == previous_month_end.day):
            start_date_2 = previous_month_start.replace(day=16)
            end_date_2 = date_2.replace(day=1) - timedelta(days=1)
          else:
            start_date_2 = date_2.replace(day=1)
            end_date_2 = date_2 - timedelta(days=1)

          return start_date_1, end_date_1, start_date_2, end_date_2
        start_date_1, end_date_1, start_date_2, end_date_2 = get_date_range(config['date'])

        connector_num=[1,2]
        auth = NoAuth()  # Oauth2Authenticator is also available if you need oauth support
        return [Campaigns_1(authenticator=auth,config=config,start_date=start_date_1.strftime("%Y-%m-%d"),end_date=end_date_1.strftime("%Y-%m-%d"),connector_num=connector_num[0]),
                Campaigns_2(authenticator=auth,config=config,start_date=start_date_2.strftime("%Y-%m-%d"),end_date=end_date_2.strftime("%Y-%m-%d"),connector_num=connector_num[1])]



@exception_handling.exception(exception_handling.logger)
def dynamodbsetup_check(config,connector_num):
    # session = boto3.Session(profile_name="ann04-sandbox-poweruser")
    session = boto3.session.Session()
    dynamodb = session.resource('dynamodb',region_name='us-east-1')
    table = dynamodb.Table("dsp_status_manager")
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('Report_type').eq('Campaign') & boto3.dynamodb.conditions.Key('Report_num').eq(f"{config['advertisers']['advertiser_id']}{'_'}{connector_num}")
    )
    return response,table


@exception_handling.exception(exception_handling.logger)
def dynamodbstatus_update(config,connector_num,response,table,value):
        response = table.update_item(
            Key={
                "Report_type":"Campaign",
                "Report_num":config['advertisers']['advertiser_id']+f"{'_'}"+f"{connector_num}"  # If your table has a sort key
            },
            UpdateExpression='SET #s = :val1',
            ExpressionAttributeNames={
                '#s': 'status'
            },

            ExpressionAttributeValues={
                ':val1': value
            }
        )


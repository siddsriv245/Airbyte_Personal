#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import os
import sys
from abc import ABC
import calendar
from datetime import datetime, timedelta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.utils.schema_helpers import expand_refs
from .schemas import campaigns
import requests

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from . import dsp_downloader,valid_list
from airbyte_cdk.sources.streams.http.auth import NoAuth


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
class CampaignSecondRunStream(HttpStream, ABC):
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
    `class CampaignSecondRunStream(HttpStream, ABC)` which is the current class
    `class Customers(CampaignSecondRunStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(CampaignSecondRunStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalCampaignSecondRunStream((CampaignSecondRunStream), ABC)` then have concrete stream implementations extend it. An example
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

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        yield from response.json()


class Campaigns_1(CampaignSecondRunStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = None
    def __init__(self, start_date,end_date,config: Mapping[str, Any],connector_num, **kwargs):
        # if config['report_types']['report_type'] == "campaign":
        super().__init__()
        self.config = config
        self.connector_num=connector_num
        # self.response,self.table=dynamodbsetup_check(self.config,self.connector_num)
        self.start_date=start_date
        self.end_date=end_date
        print(self.config)


    @property
    def model(self) ->campaigns.BaseSchemaModel:
        """
        Pydantic model to represent json schema
        """
        return campaigns.campaigns

    @model.setter
    def model(self, value):
        self._model = value


    def get_json_schema(self):

        schema = self.model.schema()
        expand_refs(schema)
        return schema


    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        self.k = dsp_downloader.execute.runfile(self.start_date, self.end_date, self.config, self.connector_num)
        return self.k['location']


class Campaigns_2(CampaignSecondRunStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = None
    def __init__(self, start_date,end_date,config: Mapping[str, Any],connector_num, **kwargs):
        # if config['report_types']['report_type'] == "campaign":
        super().__init__()
        self.config = config
        self.connector_num=connector_num
        # self.response,self.table=dynamodbsetup_check(self.config,self.connector_num)
        self.start_date=start_date
        self.end_date=end_date
        print(self.config)


    @property
    def model(self) ->campaigns.BaseSchemaModel:
        """
        Pydantic model to represent json schema
        """
        return campaigns.campaigns


    def get_json_schema(self):

        schema = self.model.schema()
        expand_refs(schema)
        return schema


    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """

        self.k = dsp_downloader.execute.runfile(self.start_date, self.end_date, self.config, self.connector_num)
        return self.k['location']


# Source
class SourceCampaignSecondRun(AbstractSource):
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

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.
        auth = NoAuth()  # Oauth2Authenticator is also available if you need oauth support
        connector_num=[1,2]
        start_date_1, end_date_1, start_date_2, end_date_2 = SourceCampaignSecondRun.get_date_range(config['date'])
        return [Campaigns_1(authenticator=auth,config=config,start_date=start_date_1.strftime("%Y-%m-%d"),end_date=end_date_1.strftime("%Y-%m-%d"),connector_num=connector_num[0]),
                Campaigns_2(authenticator=auth,config=config,start_date=start_date_2.strftime("%Y-%m-%d"),end_date=end_date_2.strftime("%Y-%m-%d"),connector_num=connector_num[1])]

    @classmethod
    def get_date_range(cls,date_str):
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

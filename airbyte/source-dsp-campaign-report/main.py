#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_dsp_campaign_report import SourceDspCampaignReport

if __name__ == "__main__":
    source = SourceDspCampaignReport()
    launch(source, sys.argv[1:])

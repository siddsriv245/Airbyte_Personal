#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_campaign_second_run import SourceCampaignSecondRun

if __name__ == "__main__":
    source = SourceCampaignSecondRun()
    launch(source, sys.argv[1:])

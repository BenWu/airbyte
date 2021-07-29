#
# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
import re
from base64 import b64decode
from unittest import mock

import responses
from airbyte_cdk.models import SyncMode
from source_amazon_ads.common import Config, SourceContext
from source_amazon_ads.report_streams import DisplayReportStream
from source_amazon_ads.schemas.profile import AccountInfo, Profile, TimeZones

"""
METRIC_RESPONSE is gzip compressed binary representing this string:
[
  {
    "campaignId": 214078428,
    "campaignName": "sample-campaign-name-214078428"
  },
  {
    "campaignId": 44504582,
    "campaignName": "sample-campaign-name-44504582"
  },
  {
    "campaignId": 509144838,
    "campaignName": "sample-campaign-name-509144838"
  },
  {
    "campaignId": 231712082,
    "campaignName": "sample-campaign-name-231712082"
  },
  {
    "campaignId": 895306040,
    "campaignName": "sample-campaign-name-895306040"
  }
]
"""
METRIC_RESPONSE = b64decode(
    """
H4sIAAAAAAAAAIvmUlCoBmIFBaXkxNyCxMz0PM8UJSsFI0MTA3MLEyMLHVRJv8TcVKC0UjGQn5Oq
CxPWzQOK68I1KQE11ergMNrExNTAxNTCiBSTYXrwGmxqYGloYmJhTJKb4ZrwGm1kbGhuaGRAmqPh
mvAabWFpamxgZmBiQIrRcE1go7liAYX9dsTHAQAA
"""
)
METRICS_COUNT = 5


def setup_responses(init_response=None, status_response=None, metric_response=None):
    if init_response:
        responses.add(responses.POST, re.compile(r"https://advertising-api.amazon.com/sd/[a-zA-Z]+/report"), body=init_response, status=202)
    if status_response:
        responses.add(
            responses.GET,
            re.compile(r"https://advertising-api.amazon.com/v2/reports/[^/]+$"),
            body=status_response,
        )

    if metric_response:
        responses.add(
            responses.GET,
            "https://advertising-api-test.amazon.com/v1/reports/amzn1.sdAPI.v1.m1.61022EEC.2ac27e60-665c-46b4-b5a9-d72f216cc8ca/download",
            body=metric_response,
        )


@responses.activate
def test_display_report_stream(test_config):
    config = Config(**test_config)
    ctx = SourceContext()
    ctx.profiles.append(
        Profile(
            profileId=1,
            timezone=TimeZones.AMERICA_LOS_ANGELES,
            accountInfo=AccountInfo(marketplaceStringId="", id="", type="seller"),
        )
    )
    setup_responses(
        init_response="""{"reportId":"amzn1.sdAPI.v1.m1.61022EEC.2ac27e60-665c-46b4-b5a9-d72f216cc8ca","recordType":"campaigns","status":"IN_PROGRESS","statusDetails":"Generating report"}""",
        status_response="""{"reportId":"amzn1.sdAPI.v1.m1.61022EEC.2ac27e60-665c-46b4-b5a9-d72f216cc8ca","status":"SUCCESS","statusDetails":"Report successfully generated","location":"https://advertising-api-test.amazon.com/v1/reports/amzn1.sdAPI.v1.m1.61022EEC.2ac27e60-665c-46b4-b5a9-d72f216cc8ca/download","fileSize":144}""",
        metric_response=METRIC_RESPONSE,
    )

    stream = DisplayReportStream(config, ctx, authenticator=mock.MagicMock())
    metrics = [m for m in stream.read_records(SyncMode.full_refresh)]
    assert len(metrics) == METRICS_COUNT * len(stream.metrics_map)

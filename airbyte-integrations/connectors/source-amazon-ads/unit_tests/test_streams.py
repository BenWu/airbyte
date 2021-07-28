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

from json import dumps, loads
from urllib.parse import parse_qs, urlparse

import pytest
import responses
from airbyte_cdk.models import SyncMode
from jsonschema import validate
from source_amazon_ads import SourceAmazonAds


def setup_responses(
    profiles_response=None,
    campaigns_response=None,
    adgroups_response=None,
    creatives_response=None,
    targeting_response=None,
    product_ads_response=None,
):
    responses.add(
        responses.POST,
        "https://api.amazon.com/auth/o2/token",
        json={"access_token": "alala", "expires_in": 10},
    )
    if profiles_response:
        responses.add(
            responses.GET,
            "https://advertising-api.amazon.com/v2/profiles",
            body=profiles_response,
        )
    if campaigns_response:
        responses.add(
            responses.GET,
            "https://advertising-api.amazon.com/sd/campaigns",
            body=campaigns_response,
        )
    if adgroups_response:
        responses.add(
            responses.GET,
            "https://advertising-api.amazon.com/sd/adGroups",
            body=adgroups_response,
        )
    if creatives_response:
        responses.add(
            responses.GET,
            "https://advertising-api.amazon.com/sd/creatives",
            body=creatives_response,
        )
    if targeting_response:
        responses.add(
            responses.GET,
            "https://advertising-api.amazon.com/sd/targets",
            body=targeting_response,
        )
    if product_ads_response:
        responses.add(
            responses.GET,
            "https://advertising-api.amazon.com/sd/productAds",
            body=product_ads_response,
        )


@responses.activate
def test_streams_profile(test_config, profiles_response):
    source = SourceAmazonAds()
    streams = source.streams(test_config)
    assert len(streams) == 6
    profile_stream = streams[0]
    assert profile_stream.name == "profiles"
    schema = profile_stream.get_json_schema()
    setup_responses(profiles_response=profiles_response)

    records = profile_stream.read_records(SyncMode.full_refresh)
    records = [r for r in records]
    assert len(responses.calls) == 2
    assert len(profile_stream.ctx.profiles) == 4
    assert len(records) == 4
    expected_records = loads(profiles_response)
    for record, expected_record in zip(records, expected_records):
        validate(schema=schema, instance=record)
        assert record == expected_record


@responses.activate
def test_streams_campaigns_one_vendor(test_config, profiles_response, campaigns_response):
    source = SourceAmazonAds()
    streams = source.streams(test_config)
    profile_stream = streams[0]
    assert profile_stream.name == "profiles"
    campaigns_stream = streams[1]
    assert campaigns_stream.name == "sponsored_display_campaigns"
    setup_responses(profiles_response=profiles_response)

    responses.add(
        responses.GET,
        "https://advertising-api.amazon.com/sd/campaigns",
        body=campaigns_response,
    )
    records = profile_stream.read_records(SyncMode.full_refresh)
    _ = [r for r in records]

    records = campaigns_stream.read_records(SyncMode.full_refresh)
    campaigns_records = [r for r in records]
    assert len(campaigns_records) == 4  # we have only one vendor account


@responses.activate
def test_streams_campaigns_4_vendors(test_config, profiles_response, campaigns_response):
    source = SourceAmazonAds()
    streams = source.streams(test_config)
    profile_stream = streams[0]
    assert profile_stream.name == "profiles"
    campaigns_stream = streams[1]
    profiles_response = loads(profiles_response)
    for profile in profiles_response:
        profile["accountInfo"]["type"] = "vendor"
    profiles_response = dumps(profiles_response)
    assert campaigns_stream.name == "sponsored_display_campaigns"
    setup_responses(profiles_response=profiles_response, campaigns_response=campaigns_response)
    records = profile_stream.read_records(SyncMode.full_refresh)
    profile_records = [r for r in records]

    records = campaigns_stream.read_records(SyncMode.full_refresh)
    campaigns_records = [r for r in records]
    assert len(campaigns_records) == len(profile_records) * len(loads(campaigns_response))


@pytest.mark.parametrize(
    ("page_size"),
    [1, 2, 5, 100],
)
@responses.activate
def test_streams_campaigns_pagination(mocker, test_config, profiles_response, campaigns_response, page_size):
    mocker.patch("source_amazon_ads.streams.PaginationStream.PAGE_SIZE", page_size)
    source = SourceAmazonAds()
    streams = source.streams(test_config)
    campaigns_stream = streams[1]
    profile_stream = streams[0]
    assert profile_stream.name == "profiles"

    profiles_response = loads(profiles_response)
    for profile in profiles_response:
        profile["accountInfo"]["type"] = "vendor"
    profiles_response = dumps(profiles_response)

    setup_responses(profiles_response=profiles_response)
    campaigns = loads(campaigns_response)

    def campaigns_paginated_response_cb(request):
        query = urlparse(request.url).query
        query = parse_qs(query)
        start_index, count = (int(query[f][0]) for f in ["startIndex", "count"])
        response_body = campaigns[start_index : start_index + count]
        return (200, {}, dumps(response_body))

    responses.add_callback(
        responses.GET,
        "https://advertising-api.amazon.com/sd/campaigns",
        content_type="application/json",
        callback=campaigns_paginated_response_cb,
    )
    records = profile_stream.read_records(SyncMode.full_refresh)
    _ = [r for r in records]

    records = campaigns_stream.read_records(SyncMode.full_refresh)
    campaigns_records = [r for r in records]
    assert len(campaigns_records) == 4 * 4  # we have only one vendor account


@pytest.mark.parametrize(
    ("stream_name", "endpoint"),
    [
        ("sponsored_display_ad_groups", "sd/adGroups"),
        ("sponsored_display_product_ads", "sd/productAds"),
        ("sponsored_display_targetings", "sd/targets"),
        ("sponsored_display_creatives", "sd/creatives"),
    ],
)
@responses.activate
def test_streams_adgroup(
    test_config, stream_name, endpoint, profiles_response, adgroups_response, creatives_response, targeting_response, product_ads_response
):
    source = SourceAmazonAds()
    streams = source.streams(test_config)
    profile_stream = streams[0]
    assert profile_stream.name == "profiles"
    test_stream = [stream for stream in streams if stream.name == stream_name][0]
    setup_responses(
        profiles_response=profiles_response,
        adgroups_response=adgroups_response,
        creatives_response=creatives_response,
        targeting_response=targeting_response,
        product_ads_response=product_ads_response,
    )

    _ = [r for r in profile_stream.read_records(SyncMode.full_refresh)]

    records = test_stream.read_records(SyncMode.full_refresh)
    records = [r for r in records]
    assert len(records) == 1
    schema = test_stream.get_json_schema()
    for r in records:
        validate(schema=schema, instance=r)
    assert any([endpoint in call.request.url for call in responses.calls])

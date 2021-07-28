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

from abc import ABC
from json import dumps, loads
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.sources.streams.http import HttpStream

from .common import PageToken, SourceContext
from .schemas import DisplayAdGroup, DisplayCampaign, DisplayCreatives, DisplayProductAds, DisplayTargeting, Profile, Types


# Basic full refresh stream
class AmazonAdsStream(HttpStream, ABC):
    def __init__(self, config, *args, context: SourceContext = SourceContext(), **kwargs):
        self.ctx = context
        self._config = config
        super().__init__(*args, **kwargs)

    url_base = "https://advertising-api.amazon.com/"
    # Pydantic model to represent json schema
    model = None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
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

    def request_headers(self, *args, **kvargs) -> MutableMapping[str, Any]:
        return {"Amazon-Advertising-API-ClientId": self._config.client_id}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an object representing single record in the response
        """
        resp = loads(response.text)
        for r in resp:
            yield r

    def get_json_schema(self):
        return self.model.schema()


class Profiles(AmazonAdsStream):
    """
    This stream corresponds to Amazon Advertising API - Profiles
    https://advertising.amazon.com/API/docs/en-us/reference/2/profiles#/Profiles
    """

    primary_key = "profileId"
    model = Profile

    def path(self, **kvargs) -> str:
        return "v2/profiles"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for r in super().parse_response(response, **kwargs):
            profile_id_obj = self.model.parse_obj(r)
            self.ctx.profiles.append(profile_id_obj)
            yield r


class ContextStream(AmazonAdsStream):
    """
    Stream for getting resources based on context set by previous stream.
    """

    flattern_properties = []

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for r in super().parse_response(response, **kwargs):
            for prop in self.flattern_properties:
                if prop in r:
                    r[prop] = dumps(r[prop])
            yield r

    def read_records(self, *args, **kvargs) -> Iterable[Mapping[str, Any]]:
        for profile in self.ctx.profiles:
            if profile.accountInfo.type == Types.VENDOR:
                self.ctx.current_profile_id = profile.profileId
                for record in super().read_records(*args, **kvargs):
                    yield record

    def request_headers(self, *args, **kvargs) -> MutableMapping[str, Any]:
        hdr = super().request_headers(*args, **kvargs)
        hdr["Amazon-Advertising-API-Scope"] = str(self.ctx.current_profile_id)
        return hdr


class PaginationStream(ContextStream):
    """
    Stream for getting resources with pagination support
    """

    PAGE_SIZE = 100

    def next_page_token(self, response: requests.Response) -> Optional[PageToken]:
        responses = loads(response.text)
        if len(responses) < PaginationStream.PAGE_SIZE:
            self.ctx.current_token = PageToken()
            return None
        else:
            next_token = PageToken(self.ctx.current_token.offset + PaginationStream.PAGE_SIZE)
            self.ctx.current_token = next_token
            return next_token

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: PageToken = None,
    ) -> MutableMapping[str, Any]:
        return {
            "startIndex": next_page_token.offset if next_page_token else 0,
            "count": PaginationStream.PAGE_SIZE,
        }


class SponsoredDisplayCampaigns(PaginationStream):
    """
    This stream corresponds to Amazon Advertising API - Sponsored Displays Campaigns
    https://advertising.amazon.com/API/docs/en-us/sponsored-display/3-0/openapi#/Campaigns
    """

    primary_key = "campaignId"
    model = DisplayCampaign

    def path(self, **kvargs) -> str:
        return "sd/campaigns"


class SponsoredDisplayAdGroups(PaginationStream):
    """
    This stream corresponds to Amazon Advertising API - Sponsored Displays Ad groups
    https://advertising.amazon.com/API/docs/en-us/sponsored-display/3-0/openapi#/Ad%20groups
    """

    primary_key = "adGroupId"
    model = DisplayAdGroup

    def path(self, **kvargs) -> str:
        return "sd/adGroups"


class SponsoredDisplayProductAds(PaginationStream):
    """
    This stream corresponds to Amazon Advertising API - Sponsored Displays Product Ads
    https://advertising.amazon.com/API/docs/en-us/sponsored-display/3-0/openapi#/Product%20ads
    """

    primary_key = "adId"
    model = DisplayProductAds

    def path(self, **kvargs) -> str:
        return "sd/productAds"


class SponsoredDisplayTargetings(PaginationStream):
    """
    This stream corresponds to Amazon Advertising API - Sponsored Displays Targetings
    https://advertising.amazon.com/API/docs/en-us/sponsored-display/3-0/openapi#/Targeting
    """

    primary_key = "targetId"
    model = DisplayTargeting
    flattern_properties = ["expression", "resolvedExpression"]

    def path(self, **kvargs) -> str:
        return "sd/targets"


class SponsoredDisplayCreatives(PaginationStream):
    """
    This stream corresponds to Amazon Advertising API - Sponsored Displays Creatives
    https://advertising.amazon.com/API/docs/en-us/sponsored-display/3-0/openapi#/Creatives
    """

    primary_key = "creativeId"
    model = DisplayCreatives

    flattern_properties = ["properties"]

    def path(self, **kvargs) -> str:
        return "sd/creatives"

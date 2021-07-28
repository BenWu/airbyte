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


from typing import Any, List, Mapping, Tuple

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.auth import Oauth2Authenticator

from .common import Config, SourceContext
from .streams import (
    Profiles,
    SponsoredDisplayAdGroups,
    SponsoredDisplayCampaigns,
    SponsoredDisplayCreatives,
    SponsoredDisplayProductAds,
    SponsoredDisplayTargetings,
)

TOKEN_URL = "https://api.amazon.com/auth/o2/token"


# Source
class SourceAmazonAds(AbstractSource):
    def __init__(self):
        super().__init__()
        self.ctx = SourceContext()

    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        """
        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        config = Config(**config)
        next(Profiles(config, authenticator=self._make_authenticator(config)).read_records(sync_mode=SyncMode.full_refresh))
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # import http.client as http_client
        # http_client.HTTPConnection.debuglevel = 1

        config = Config(**config)
        auth = self._make_authenticator(config)
        return [
            Profiles(config, context=self.ctx, authenticator=auth),
            SponsoredDisplayCampaigns(config, context=self.ctx, authenticator=auth),
            SponsoredDisplayAdGroups(config, context=self.ctx, authenticator=auth),
            SponsoredDisplayProductAds(config, context=self.ctx, authenticator=auth),
            SponsoredDisplayTargetings(config, context=self.ctx, authenticator=auth),
            SponsoredDisplayCreatives(config, context=self.ctx, authenticator=auth),
        ]

    @staticmethod
    def _make_authenticator(config: Config):
        return Oauth2Authenticator(
            TOKEN_URL,
            config.client_id,
            config.client_secret,
            config.refresh_token,
            [config.scope],
        )

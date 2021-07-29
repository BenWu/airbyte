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
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from gzip import decompress
from http import HTTPStatus
from json import loads
from time import sleep
from typing import Any, Dict, Iterable, List, Mapping, Optional
from urllib.parse import urljoin

import backoff
import requests
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.core import Stream
from airbyte_cdk.sources.streams.http.auth import Oauth2Authenticator
from pydantic import BaseModel
from source_amazon_ads.common import URL_BASE, Config, SourceContext
from source_amazon_ads.schemas import MetricsReport
from source_amazon_ads.schemas.profile import Types

logger = AirbyteLogger()


class RecordType(str, Enum):
    CAMPAIGNS = "campaigns"
    ADGROUPS = "adGroups"
    PRODUCTADS = "productAds"
    TARGETS = "targets"
    ASINS = "asins"


class Status(str, Enum):
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class Tactics(str, Enum):
    T00001 = "T00001"
    T00020 = "T00020"
    T00030 = "T00030"
    REMARKETING = "remarketing"


class ReportInitResponse(BaseModel):
    reportId: str
    status: str


class ReportStatus(BaseModel):
    status: str
    location: Optional[str]


@dataclass
class ReportInfo:
    report_id: str
    profile_id: int
    record_type: str


class TooManyRequests(Exception):
    """
    Custom exception occured when response with 429 status code received
    """


class ReportStream(Stream, ABC):
    """
    Common base class for report streams
    """

    primary_key = None
    CHECK_INTERVAL_SECONDS = 30
    REPORT_WAIT_TIMEOUT = timedelta(hours=1)
    REPORT_DATE_FORMAT = "%Y%m%d"
    cursor_field = "reportDate"

    def __init__(self, config: Config, context: SourceContext, authenticator: Oauth2Authenticator):
        self._authenticator = authenticator
        self._ctx = context
        self._config = config
        self._session = requests.Session()
        self._last_slice = None

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:

        report_date = stream_slice[self.cursor_field]
        report_infos = self._init_reports(report_date)
        logger.info(f"Waiting for {len(report_infos)} report(s) to be generated")
        start_time_point = datetime.now()
        while report_infos or datetime.now() >= start_time_point + self.REPORT_WAIT_TIMEOUT:
            success_reports = []
            logger.info(f"Checking report status, {len(report_infos)} report(s) remained")
            for report_info in report_infos:
                download_url = self._check_status(report_info)
                if download_url:
                    metric_objects = self._download_report(report_info, download_url)
                    for metric_object in metric_objects:
                        yield MetricsReport(
                            profileId=report_info.profile_id,
                            recordType=report_info.record_type,
                            reportDate=report_date,
                            metric=metric_object,
                        ).dict()
                    success_reports.append(report_info)
            for success_report in success_reports:
                report_infos.remove(success_report)
            if report_infos:
                logger.info(f"{len(report_infos)} report(s) remained, taking {self.CHECK_INTERVAL_SECONDS} seconds timeout")
                sleep(self.CHECK_INTERVAL_SECONDS)
        if not report_infos:
            logger.info("All reports have been processed")
        else:
            logger.error("Not all reports has been processed due to timeout")

        self._last_slice = stream_slice

        yield from []

    def get_json_schema(self):
        return MetricsReport.schema()

    def _get_auth_headers(self, profile_id: int):
        return {
            "Amazon-Advertising-API-ClientId": self._config.client_id,
            "Amazon-Advertising-API-Scope": str(profile_id),
            **self._authenticator.get_auth_header(),
        }

    @abstractmethod
    def report_init_endpoint(self, record_type: str) -> str:
        """
        :return: endpoint to initial report generating process.
        """

    @property
    @abstractmethod
    def metrics_map(self) -> Dict[str, List]:
        """
        :return: Map record type to list of available metrics
        """

    def _check_status(self, report_info: ReportInfo) -> Optional[str]:
        """
        Check report status and return download link if report generated successfuly
        """
        check_endpoint = f"/v2/reports/{report_info.report_id}"
        resp = self._send_http_request(urljoin(URL_BASE, check_endpoint), report_info.profile_id)
        resp = ReportStatus.parse_raw(resp.text)
        if resp.location and resp.status == Status.SUCCESS:
            return resp.location

    @backoff.on_exception(
        backoff.expo,
        (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            TooManyRequests,
        ),
        max_tries=5,
    )
    def _send_http_request(self, url: str, profile_id: int, json: dict = None):
        headers = self._get_auth_headers(profile_id)
        if json:
            response = self._session.post(
                url,
                headers=headers,
                json=json,
            )
        else:
            response = self._session.get(
                url,
                headers=headers,
            )
        if response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            raise TooManyRequests()
        return response

    @staticmethod
    def get_report_date_ranges(start_report_date: datetime) -> List[str]:
        """
        Generates dates in YYYYMMDD format for each day started from start_report_date
        """
        now = datetime.now()
        if not start_report_date:
            start_report_date = now

        for days in range(0, (now - start_report_date).days + 1):
            next_date = start_report_date + timedelta(days=days)
            next_date = next_date.strftime(ReportStream.REPORT_DATE_FORMAT)
            yield next_date

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        stream_state = stream_state or {}

        start_date = stream_state.get(self.cursor_field)
        if start_date:
            start_date = datetime.strptime(start_date, ReportStream.REPORT_DATE_FORMAT)

        return [{self.cursor_field: date} for date in ReportStream.get_report_date_ranges(start_date)]

    def get_updated_state(self, *args) -> Mapping[str, Any]:
        return self._last_slice

    def _init_reports(self, report_date: str) -> List[ReportInfo]:
        """
        Initiate report creating
        """
        report_infos = []
        for profile in self._ctx.profiles:
            for record_type, metrics in self.metrics_map.items():
                if record_type == RecordType.ASINS and profile.accountInfo.type == Types.VENDOR:
                    # Asins reports not available to Vendors
                    continue
                report_init_body = {
                    "reportDate": report_date,
                    # Only for most common T00020 tactic for now
                    "tactic": Tactics.T00020,
                    "metrics": ",".join(metrics),
                }
                logger.info(f"Initiating report generation for {profile.profileId} profile with {record_type} type for {report_date} data")
                response = self._send_http_request(
                    urljoin(URL_BASE, self.report_init_endpoint(record_type)),
                    profile.profileId,
                    report_init_body,
                )
                if response.status_code != HTTPStatus.ACCEPTED:
                    logger.warn(f"Unexpected error when registering {record_type} for {profile.profileId} profile: {response.text}")
                    continue

                response = ReportInitResponse.parse_raw(response.text)
                report_infos.append(
                    ReportInfo(
                        report_id=response.reportId,
                        record_type=record_type,
                        profile_id=profile.profileId,
                    )
                )
                logger.info("Initiated successfully")

        return report_infos

    def _download_report(self, report_info: ReportInfo, url: str) -> List[dict]:
        """
        Download and parse report result
        """
        response = self._send_http_request(url, report_info.profile_id)
        raw_string = decompress(response.content).decode("utf")
        return loads(raw_string)

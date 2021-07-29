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
from .report_streams import ReportStream

"""
Those metrics are mentioned on reference
https://advertising.amazon.com/API/docs/en-us/sponsored-display/3-0/openapi#/Reports/requestReport
but it generates an error "Unsupported metric" so they are commented out
"""
METRICS_MAP = {
    "campaigns": [
        "campaignName",
        "campaignId",
        "impressions",
        "clicks",
        "cost",
        "currency",
        # "attributedConversions1d",
        # "attributedConversions7d",
        # "attributedConversions14d",
        # "attributedConversions30d",
        # "attributedConversions1dSameSKU",
        # "attributedConversions7dSameSKU",
        # "attributedConversions14dSameSKU",
        # "attributedConversions30dSameSKU",
        # "attributedUnitsOrdered1d",
        # "attributedUnitsOrdered7d",
        # "attributedUnitsOrdered14d",
        # "attributedUnitsOrdered30d",
        # "attributedSales1d",
        # "attributedSales7d",
        # "attributedSales14d",
        # "attributedSales30d",
        # "attributedSales1dSameSKU",
        # "attributedSales7dSameSKU",
        # "attributedSales14dSameSKU",
        # "attributedSales30dSameSKU",
        # "attributedOrdersNewToBrand14d",
        # "attributedSalesNewToBrand14d",
        # "attributedUnitsOrderedNewToBrand14d",
    ],
    "adGroups": [
        "campaignName",
        "campaignId",
        "adGroupName",
        "adGroupId",
        "impressions",
        "clicks",
        "cost",
        "currency",
        "attributedConversions1d",
        "attributedConversions7d",
        "attributedConversions14d",
        "attributedConversions30d",
        "attributedConversions1dSameSKU",
        "attributedConversions7dSameSKU",
        "attributedConversions14dSameSKU",
        "attributedConversions30dSameSKU",
        "attributedUnitsOrdered1d",
        "attributedUnitsOrdered7d",
        "attributedUnitsOrdered14d",
        "attributedUnitsOrdered30d",
        "attributedSales1d",
        "attributedSales7d",
        "attributedSales14d",
        "attributedSales30d",
        "attributedSales1dSameSKU",
        "attributedSales7dSameSKU",
        "attributedSales14dSameSKU",
        "attributedSales30dSameSKU",
        "attributedOrdersNewToBrand14d",
        "attributedSalesNewToBrand14d",
        "attributedUnitsOrderedNewToBrand14d",
    ],
    "productAds": [
        "campaignName",
        "campaignId",
        "adGroupName",
        "adGroupId",
        "asin",
        "sku",
        "adId",
        "impressions",
        "clicks",
        "cost",
        "currency",
        "attributedConversions1d",
        "attributedConversions7d",
        "attributedConversions14d",
        "attributedConversions30d",
        "attributedConversions1dSameSKU",
        "attributedConversions7dSameSKU",
        "attributedConversions14dSameSKU",
        "attributedConversions30dSameSKU",
        "attributedUnitsOrdered1d",
        "attributedUnitsOrdered7d",
        "attributedUnitsOrdered14d",
        "attributedUnitsOrdered30d",
        "attributedSales1d",
        "attributedSales7d",
        "attributedSales14d",
        "attributedSales30d",
        "attributedSales1dSameSKU",
        "attributedSales7dSameSKU",
        "attributedSales14dSameSKU",
        "attributedSales30dSameSKU",
        "attributedOrdersNewToBrand14d",
        "attributedSalesNewToBrand14d",
        "attributedUnitsOrderedNewToBrand14d",
    ],
    "targets": [
        "campaignName",
        "campaignId",
        "adGroupName",
        "adGroupId",
        "targetId",
        "targetingExpression",
        "targetingText",
        "targetingType",
        "impressions",
        "clicks",
        "cost",
        "currency",
        "attributedConversions1d",
        "attributedConversions7d",
        "attributedConversions14d",
        "attributedConversions30d",
        "attributedConversions1dSameSKU",
        "attributedConversions7dSameSKU",
        "attributedConversions14dSameSKU",
        "attributedConversions30dSameSKU",
        "attributedUnitsOrdered1d",
        "attributedUnitsOrdered7d",
        "attributedUnitsOrdered14d",
        "attributedUnitsOrdered30d",
        "attributedSales1d",
        "attributedSales7d",
        "attributedSales14d",
        "attributedSales30d",
        "attributedSales1dSameSKU",
        "attributedSales7dSameSKU",
        "attributedSales14dSameSKU",
        "attributedSales30dSameSKU",
        "attributedOrdersNewToBrand14d",
        "attributedSalesNewToBrand14d",
        "attributedUnitsOrderedNewToBrand14d",
    ],
    "asins": [
        "campaignName",
        "campaignId",
        "adGroupName",
        "adGroupId",
        "asin",
        "otherAsin",
        "sku",
        "currency",
        "attributedUnitsOrdered1dOtherSKU",
        "attributedUnitsOrdered7dOtherSKU",
        "attributedUnitsOrdered14dOtherSKU",
        "attributedUnitsOrdered30dOtherSKU",
        "attributedSales1dOtherSKU",
        "attributedSales7dOtherSKU",
        "attributedSales14dOtherSKU",
        "attributedSales30dOtherSKU",
    ],
}


class DisplayReportStream(ReportStream):
    """
    https://advertising.amazon.com/API/docs/en-us/sponsored-display/3-0/openapi#/Reports
    """

    def report_init_endpoint(self, record_type: str) -> str:
        return f"/sd/{record_type}/report"

    metrics_map = METRICS_MAP

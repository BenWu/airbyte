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

from decimal import Decimal

from .common import JSEnum, JSModel


class CostType(JSEnum):
    CPC = "cpc"
    VCPM = "vcpm"


class State(JSEnum):
    ENABLED = "enabled"
    PAUSED = "paused"
    ARCHIVED = "archived"


class Tactic(JSEnum):
    T00020 = "T00020"
    T00030 = "T00030"


class DeliveryProfile(JSEnum):
    AS_SOON_AS_POSSIBLE = "as_soon_as_possible"


class DisplayCampaign(JSModel):
    campaignId: Decimal
    name: str
    budgetType: str
    budget: Decimal
    startDate: str
    endDate: str = None
    costType: CostType
    state: State
    portfolioId: str = None
    tactic: Tactic
    deliveryProfile: DeliveryProfile


class BidOptimization(JSEnum):
    CLICKS = "clicks"
    CONVERSIONS = "conversions"
    REACH = "reach"


class ExpressionType(JSEnum):
    MANUAL = "manual"
    AUTO = "auto"


class ModerationStatus(JSEnum):
    APPROVED = "APPROVED"
    PENDING_REVIEW = "PENDING_REVIEW"
    REJECTED = "REJECTED"


class DisplayAdGroup(JSModel):
    name: str
    campaignId: Decimal
    adGroupId: Decimal
    defaultBid: Decimal
    bidOptimization: BidOptimization
    state: State
    tactic: Tactic


class DisplayProductAds(JSModel):
    state: State
    adId: Decimal
    campaignId: Decimal
    adGroupId: Decimal
    asin: str
    sku: str


class DisplayTargeting(JSModel):
    state: State
    bid: Decimal
    targetId: Decimal
    adGroupId: Decimal
    expressionType: ExpressionType
    expression: str
    resolvedExpression: str


class DisplayCreatives(JSModel):
    creativeId: Decimal
    moderationStatus: ModerationStatus
    properties: str = None

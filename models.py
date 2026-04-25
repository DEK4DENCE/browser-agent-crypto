from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class TraderPosition(BaseModel):
    address: str
    asset: str
    side: str  # LONG / SHORT
    size_usd: float = 0.0
    entry_price: float = 0.0
    liquidation_price: Optional[float] = None
    unrealized_pnl: float = 0.0
    leverage: int = 1
    roi_pct: Optional[float] = None


class TraderStats(BaseModel):
    address: str
    rank: int
    pnl_all_time: float = 0.0
    win_rate: Optional[float] = None
    total_volume: Optional[float] = None
    days_trading: Optional[int] = None
    avg_position_size: Optional[float] = None
    positions: list[TraderPosition] = []


class FundingRate(BaseModel):
    exchange: str
    asset: str
    rate_8h: float
    annualized: float


class LiquidationZone(BaseModel):
    price: float
    liquidation_usd: float
    side: str  # LONG / SHORT


class WhaleWallet(BaseModel):
    address: str
    asset: str
    action: str  # ACCUMULATING / DISTRIBUTING / UNKNOWN
    size_usd: float = 0.0
    timestamp: Optional[str] = None
    known_name: Optional[str] = None


class KOLSentiment(BaseModel):
    author: str
    text: str
    timestamp: str
    bullish: bool
    url: Optional[str] = None
    likes: int = 0


class TokenMetrics(BaseModel):
    price_usd: Optional[float] = None
    market_cap: Optional[float] = None
    volume_24h: Optional[float] = None
    ath: Optional[float] = None
    atl: Optional[float] = None
    tvl: Optional[float] = None
    fees_24h: Optional[float] = None
    revenue_24h: Optional[float] = None
    open_interest: Optional[float] = None
    long_oi_pct: Optional[float] = None
    funding_rate_hl: Optional[float] = None


class ResearchResult(BaseModel):
    asset: str
    query: str = ""
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    token_metrics: TokenMetrics = Field(default_factory=TokenMetrics)
    top_traders: list[TraderStats] = []
    funding_rates: list[FundingRate] = []
    liquidation_zones: list[LiquidationZone] = []
    whale_wallets: list[WhaleWallet] = []
    kol_sentiment: list[KOLSentiment] = []
    raw_notes: list[str] = []

    @property
    def long_bias_pct(self) -> float:
        positions = [p for t in self.top_traders for p in t.positions]
        if not positions:
            return 50.0
        longs = sum(1 for p in positions if p.side == "LONG")
        return round(longs / len(positions) * 100, 1)

    @property
    def avg_funding_8h(self) -> float:
        if not self.funding_rates:
            return 0.0
        return round(sum(r.rate_8h for r in self.funding_rates) / len(self.funding_rates), 4)

    @property
    def whale_signal(self) -> str:
        if not self.whale_wallets:
            return "UNKNOWN"
        accum = sum(1 for w in self.whale_wallets if w.action == "ACCUMULATING")
        dist = sum(1 for w in self.whale_wallets if w.action == "DISTRIBUTING")
        if accum > dist * 1.5:
            return "ACCUMULATING"
        if dist > accum * 1.5:
            return "DISTRIBUTING"
        return "MIXED"

    @property
    def kol_bullish_pct(self) -> float:
        if not self.kol_sentiment:
            return 50.0
        bullish = sum(1 for k in self.kol_sentiment if k.bullish)
        return round(bullish / len(self.kol_sentiment) * 100, 1)

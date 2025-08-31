from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime

class BaseResponse(BaseModel):
    status: str = Field(..., description="Response status")
    message: str = Field(..., description="Response message") 
    timestamp: datetime = Field(..., description="Response timestamp")

class HealthResponse(BaseResponse):
    services: Dict[str, str] = Field(..., description="Service status information")

class PriceUpdateResponse(BaseResponse):
    prices_updated: int = Field(..., description="Number of prices successfully updated")
    tokens_processed: int = Field(..., description="Total number of tokens processed")
    failed_tokens: List[str] = Field(..., description="List of tokens that failed to update")

class ScoreCalculationResponse(BaseResponse):
    matched_trades: int = Field(..., description="Number of trade pairs matched")
    score_adjustments: int = Field(..., description="Number of score adjustments calculated")
    wallet_updates: int = Field(..., description="Number of wallet updates generated")
    recommendations_generated: int = Field(..., description="Number of recommendations created")

class RecommendationItem(BaseModel):
    wallet_address: str
    current_score: int
    suggested_new_score: int
    total_score_change: int
    trades_analyzed: int
    win_rate: float
    avg_profit_pct: float
    recommendation_message: str
    priority: str
    update_calculated_at: datetime

class RecommendationsResponse(BaseResponse):
    recommendations: List[RecommendationItem] = Field(..., description="List of score recommendations")
    count: int = Field(..., description="Number of recommendations returned")
    filters: Dict[str, Any] = Field(..., description="Applied filters")

class TransactionProcessingResponse(BaseResponse):
    transactions_processed: int = Field(..., description="Number of transactions processed")
    unique_wallets: int = Field(..., description="Number of unique wallets involved")
    unique_tokens: int = Field(..., description="Number of unique tokens involved") 
    price_update_triggered: bool = Field(..., description="Whether price update was triggered")

class WalletAnalysisResponse(BaseResponse):
    wallet_address: str = Field(..., description="Analyzed wallet address")
    analysis: Dict[str, Any] = Field(..., description="Wallet analysis results")
    days_analyzed: Optional[int] = Field(None, description="Number of days analyzed")
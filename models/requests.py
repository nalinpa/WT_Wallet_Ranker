from pydantic import BaseModel, Field, validator
from typing import List, Optional
from datetime import datetime

class Transaction(BaseModel):
    transaction_hash: str = Field(..., description="Unique transaction hash")
    block_number: int = Field(..., description="Blockchain block number")
    timestamp: datetime = Field(..., description="Transaction timestamp")
    wallet_address: str = Field(..., description="Wallet address (0x...)")
    token_address: str = Field(..., description="Token contract address")
    token_symbol: str = Field(..., description="Token symbol")
    token_amount: float = Field(..., description="Amount of tokens")
    transfer_type: str = Field(..., description="buy or sell")
    cost_in_eth: float = Field(..., description="Gas cost in ETH")
    platform: str = Field(..., description="Trading platform")
    network: str = Field(default="ethereum", description="Blockchain network")
    wallet_sophistication_score: int = Field(..., description="Current wallet score")
    
    @validator('wallet_address', 'token_address')
    def validate_address(cls, v):
        if not v.startswith('0x') or len(v) != 42:
            raise ValueError('Invalid Ethereum address format')
        return v.lower()
    
    @validator('transfer_type')
    def validate_transfer_type(cls, v):
        if v.lower() not in ['buy', 'sell']:
            raise ValueError('transfer_type must be "buy" or "sell"')
        return v.lower()

class TransactionBatch(BaseModel):
    transactions: List[Transaction] = Field(..., description="List of transactions to process")
    
    @validator('transactions')
    def validate_not_empty(cls, v):
        if not v:
            raise ValueError('transactions list cannot be empty')
        if len(v) > 1000:
            raise ValueError('Maximum 1000 transactions per batch')
        return v

class ScoreCalculationRequest(BaseModel):
    days_back: int = Field(default=7, ge=1, le=30, description="Days to look back for trades")
    min_trades: int = Field(default=3, ge=1, le=100, description="Minimum trades for score update")

class PriceUpdateRequest(BaseModel):
    hours_back: int = Field(default=24, ge=1, le=168, description="Hours to look back for active tokens")
    min_trades: int = Field(default=3, ge=1, le=100, description="Minimum trades for price update")
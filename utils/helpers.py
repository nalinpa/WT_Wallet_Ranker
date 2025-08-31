import os
import re
import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta, timezone
import json

logger = logging.getLogger(__name__)

class ResponseFormatter:
    """Standardized response formatting for the API"""
    
    @staticmethod
    def success(
        message: str, 
        data: Optional[Dict[str, Any]] = None, 
        timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Format a successful response"""
        response = {
            "status": "success",
            "message": message,
            "timestamp": timestamp or datetime.utcnow()
        }
        if data:
            response.update(data)
        return response
    
    @staticmethod
    def error(
        message: str, 
        error_code: Optional[str] = None,
        timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Format an error response"""
        response = {
            "status": "error",
            "message": message,
            "timestamp": timestamp or datetime.utcnow()
        }
        if error_code:
            response["error_code"] = error_code
        return response

class AddressValidator:
    """Ethereum address validation utilities"""
    
    @staticmethod
    def is_valid_address(address: str) -> bool:
        """Check if an address is a valid Ethereum address format"""
        if not address or not isinstance(address, str):
            return False
        
        address = address.strip().lower()
        pattern = r'^0x[a-f0-9]{40}$'
        return bool(re.match(pattern, address))
    
    @staticmethod
    def normalize_address(address: str) -> str:
        """Normalize an Ethereum address to lowercase"""
        if not AddressValidator.is_valid_address(address):
            raise ValueError(f"Invalid Ethereum address: {address}")
        return address.strip().lower()
    
    @staticmethod
    def is_null_address(address: str) -> bool:
        """Check if address is the null/zero address"""
        null_address = "0x0000000000000000000000000000000000000000"
        return AddressValidator.normalize_address(address) == null_address

class DateTimeHelper:
    """DateTime utilities for the application"""
    
    @staticmethod
    def utc_now() -> datetime:
        """Get current UTC datetime"""
        return datetime.now(timezone.utc)
    
    @staticmethod
    def from_timestamp(timestamp: Union[int, float, str]) -> datetime:
        """Convert various timestamp formats to datetime"""
        try:
            if isinstance(timestamp, str):
                try:
                    return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except ValueError:
                    timestamp = float(timestamp)
            
            if isinstance(timestamp, (int, float)):
                if timestamp > 1e12:  # Milliseconds
                    timestamp = timestamp / 1000
                return datetime.fromtimestamp(timestamp, tz=timezone.utc)
                
        except (ValueError, TypeError, OSError) as e:
            raise ValueError(f"Invalid timestamp format: {timestamp}") from e
    
    @staticmethod
    def format_duration(hours: float) -> str:
        """Format duration in hours to human-readable string"""
        if hours < 1:
            minutes = int(hours * 60)
            return f"{minutes}m"
        elif hours < 24:
            return f"{hours:.1f}h"
        else:
            days = int(hours / 24)
            remaining_hours = int(hours % 24)
            if remaining_hours > 0:
                return f"{days}d {remaining_hours}h"
            return f"{days}d"

class PriceCalculator:
    """Price calculation utilities"""
    
    @staticmethod
    def calculate_price_change(buy_price: float, sell_price: float) -> float:
        """Calculate percentage price change"""
        if buy_price <= 0:
            raise ValueError("Buy price must be positive")
        return ((sell_price - buy_price) / buy_price) * 100
    
    @staticmethod
    def calculate_profit_usd(
        buy_amount: float,
        sell_amount: float,
        buy_price: float,
        sell_price: float,
        gas_cost_eth: float,
        eth_price_usd: float = 2500
    ) -> float:
        """Calculate profit in USD including gas costs"""
        if buy_price <= 0 or sell_price <= 0:
            return 0.0
        
        traded_amount = min(buy_amount, sell_amount)
        buy_value = traded_amount * buy_price
        sell_value = traded_amount * sell_price
        gas_cost_usd = gas_cost_eth * eth_price_usd
        
        return sell_value - buy_value - gas_cost_usd
    
    @staticmethod
    def is_price_reasonable(price: float, symbol: str) -> bool:
        """Basic price reasonableness check"""
        if price <= 0:
            return False
        
        if price > 1_000_000:  # $1M per token is extreme
            logger.warning(f"Unusually high price for {symbol}: ${price}")
            return False
        
        if price < 0.000001:  # Less than $0.000001 is suspicious
            logger.warning(f"Unusually low price for {symbol}: ${price}")
            return False
        
        return True

class MetricsCalculator:
    """Calculate various trading and wallet metrics"""
    
    @staticmethod
    def calculate_win_rate(profitable_trades: int, total_trades: int) -> float:
        """Calculate win rate percentage"""
        if total_trades <= 0:
            return 0.0
        return (profitable_trades / total_trades) * 100
    
    @staticmethod
    def calculate_diversification_score(unique_assets: int, total_trades: int) -> float:
        """Calculate diversification score (0-1)"""
        if total_trades <= 0:
            return 0.0
        
        max_reasonable_assets = min(20, total_trades)
        normalized_assets = min(unique_assets, max_reasonable_assets)
        
        return normalized_assets / max_reasonable_assets

class DataValidator:
    """Data validation utilities"""
    
    @staticmethod
    def validate_transaction_data(transaction: Dict[str, Any]) -> tuple[bool, List[str]]:
        """Validate transaction data completeness and format"""
        errors = []
        
        # Required fields
        required_fields = [
            'transaction_hash', 'wallet_address', 'token_address',
            'token_symbol', 'transfer_type', 'wallet_sophistication_score'
        ]
        
        for field in required_fields:
            if field not in transaction or transaction[field] is None:
                errors.append(f"Missing required field: {field}")
        
        # Validate address formats
        if 'wallet_address' in transaction:
            if not AddressValidator.is_valid_address(transaction['wallet_address']):
                errors.append("Invalid wallet_address format")
        
        if 'token_address' in transaction:
            if not AddressValidator.is_valid_address(transaction['token_address']):
                errors.append("Invalid token_address format")
        
        # Validate transfer type
        if 'transfer_type' in transaction:
            if transaction['transfer_type'].lower() not in ['buy', 'sell']:
                errors.append("transfer_type must be 'buy' or 'sell'")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def validate_score_range(score: int, min_score: int = 25, max_score: int = 1000) -> bool:
        """Validate wallet sophistication score is in reasonable range"""
        return min_score <= score <= max_score

class CacheManager:
    """Simple in-memory cache with TTL support"""
    
    def __init__(self, default_ttl_seconds: int = 300):
        self.cache = {}
        self.default_ttl = default_ttl_seconds
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired"""
        if key in self.cache:
            value, expiry = self.cache[key]
            if datetime.utcnow() < expiry:
                return value
            else:
                del self.cache[key]
        return None
    
    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """Set value in cache with TTL"""
        ttl = ttl_seconds or self.default_ttl
        expiry = datetime.utcnow() + timedelta(seconds=ttl)
        self.cache[key] = (value, expiry)
    
    def clear(self) -> int:
        """Clear all cache entries and return count cleared"""
        count = len(self.cache)
        self.cache.clear()
        return count

class ConfigHelper:
    """Configuration and environment variable helpers"""
    
    @staticmethod
    def get_env_var(key: str, default: Optional[str] = None, required: bool = False) -> str:
        """Get environment variable with validation"""
        value = os.environ.get(key, default)
        
        if required and not value:
            raise ValueError(f"Required environment variable '{key}' not set")
        
        return value
    
    @staticmethod
    def get_env_int(key: str, default: int, min_value: Optional[int] = None, max_value: Optional[int] = None) -> int:
        """Get integer environment variable with validation"""
        try:
            value = int(os.environ.get(key, str(default)))
            
            if min_value is not None and value < min_value:
                logger.warning(f"Environment variable {key}={value} below minimum {min_value}, using minimum")
                return min_value
            
            if max_value is not None and value > max_value:
                logger.warning(f"Environment variable {key}={value} above maximum {max_value}, using maximum")
                return max_value
            
            return value
        except ValueError:
            logger.warning(f"Invalid integer value for {key}, using default: {default}")
            return default
    
    @staticmethod
    def get_env_bool(key: str, default: bool = False) -> bool:
        """Get boolean environment variable"""
        value = os.environ.get(key, '').lower()
        return value in ('true', '1', 'yes', 'on', 'enabled')

# Utility functions
def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safe division that handles zero denominator"""
    try:
        if denominator == 0:
            return default
        return numerator / denominator
    except (TypeError, ValueError):
        return default

def safe_percentage(part: float, total: float, decimals: int = 2) -> float:
    """Safely calculate percentage"""
    result = safe_divide(part, total, 0.0) * 100
    return round(result, decimals)

def truncate_address(address: str, start_chars: int = 6, end_chars: int = 4) -> str:
    """Truncate Ethereum address for display"""
    if not AddressValidator.is_valid_address(address):
        return address
    
    if len(address) <= start_chars + end_chars + 2:
        return address
    
    return f"{address[:start_chars]}...{address[-end_chars:]}"

def format_large_number(value: float, decimals: int = 2) -> str:
    """Format large numbers with appropriate suffixes"""
    if abs(value) >= 1e12:
        return f"{value / 1e12:.{decimals}f}T"
    elif abs(value) >= 1e9:
        return f"{value / 1e9:.{decimals}f}B"
    elif abs(value) >= 1e6:
        return f"{value / 1e6:.{decimals}f}M"
    elif abs(value) >= 1e3:
        return f"{value / 1e3:.{decimals}f}K"
    else:
        return f"{value:.{decimals}f}"

class WalletTierClassifier:
    """Classify wallets into tiers based on sophistication scores"""
    
    TIER_THRESHOLDS = {
        'Elite': (0, 179),
        'High': (180, 219),
        'Medium': (220, 259),
        'Low': (260, 1000)
    }
    
    @classmethod
    def get_tier(cls, sophistication_score: int) -> str:
        """Get wallet tier from sophistication score"""
        for tier, (min_score, max_score) in cls.TIER_THRESHOLDS.items():
            if min_score <= sophistication_score <= max_score:
                return tier
        return 'Unknown'
    
    @classmethod
    def get_all_tiers(cls) -> List[str]:
        """Get list of all tier names"""
        return list(cls.TIER_THRESHOLDS.keys())

class TradeTypeClassifier:
    """Classify trades based on holding period"""
    
    @staticmethod
    def classify_by_hold_time(hold_hours: float) -> str:
        """Classify trade type by holding period"""
        if hold_hours < 1:
            return 'Scalp'
        elif hold_hours < 24:
            return 'Day Trade'
        elif hold_hours < 168:  # 1 week
            return 'Swing Trade'
        else:
            return 'Position Trade'
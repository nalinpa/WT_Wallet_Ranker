import os
import re
import hashlib
import logging
from typing import Dict, Any, List, Optional, Union, Tuple
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
from dataclasses import dataclass, asdict
from enum import Enum
import asyncio
from functools import wraps
import time

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
        details: Optional[Dict[str, Any]] = None,
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
        if details:
            response["details"] = details
        return response
    
    @staticmethod
    def partial(
        message: str,
        success_count: int,
        total_count: int,
        failures: Optional[List[str]] = None,
        timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Format a partial success response"""
        response = {
            "status": "partial",
            "message": message,
            "success_count": success_count,
            "total_count": total_count,
            "success_rate": (success_count / total_count * 100) if total_count > 0 else 0,
            "timestamp": timestamp or datetime.utcnow()
        }
        if failures:
            response["failures"] = failures
        return response
    
    @staticmethod
    def paginated(
        data: List[Any],
        total_count: int,
        page: int = 1,
        page_size: int = 50,
        message: Optional[str] = None
    ) -> Dict[str, Any]:
        """Format a paginated response"""
        total_pages = (total_count + page_size - 1) // page_size
        
        return {
            "status": "success",
            "message": message or f"Retrieved {len(data)} records",
            "data": data,
            "pagination": {
                "current_page": page,
                "page_size": page_size,
                "total_count": total_count,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            },
            "timestamp": datetime.utcnow()
        }


class AddressValidator:
    """Ethereum address validation utilities"""
    
    @staticmethod
    def is_valid_address(address: str) -> bool:
        """Check if an address is a valid Ethereum address format"""
        if not address or not isinstance(address, str):
            return False
        
        # Remove whitespace and convert to lowercase
        address = address.strip().lower()
        
        # Check format: 0x followed by 40 hexadecimal characters
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
    

class StatisticsHelper:
    """Statistical calculation utilities"""
    
    @staticmethod
    def calculate_percentiles(values: List[float], percentiles: List[int] = [25, 50, 75, 90, 95]) -> Dict[int, float]:
        """Calculate percentiles for a list of values"""
        if not values:
            return {p: 0.0 for p in percentiles}
        
        sorted_values = sorted(values)
        n = len(sorted_values)
        result = {}
        
        for p in percentiles:
            if p < 0 or p > 100:
                continue
            
            if p == 0:
                result[p] = sorted_values[0]
            elif p == 100:
                result[p] = sorted_values[-1]
            else:
                # Linear interpolation method
                index = (p / 100) * (n - 1)
                lower_index = int(index)
                upper_index = min(lower_index + 1, n - 1)
                
                if lower_index == upper_index:
                    result[p] = sorted_values[lower_index]
                else:
                    weight = index - lower_index
                    result[p] = (sorted_values[lower_index] * (1 - weight) + 
                               sorted_values[upper_index] * weight)
        
        return result
    
    @staticmethod
    def calculate_moving_average(values: List[float], window: int) -> List[float]:
        """Calculate moving average with specified window"""
        if window <= 0 or window > len(values):
            return values.copy()
        
        moving_averages = []
        for i in range(len(values)):
            start_idx = max(0, i - window + 1)
            window_values = values[start_idx:i + 1]
            moving_averages.append(sum(window_values) / len(window_values))
        
        return moving_averages
    
    @staticmethod
    def calculate_correlation(x_values: List[float], y_values: List[float]) -> Optional[float]:
        """Calculate Pearson correlation coefficient"""
        if len(x_values) != len(y_values) or len(x_values) < 2:
            return None
        
        n = len(x_values)
        sum_x = sum(x_values)
        sum_y = sum(y_values)
        sum_xx = sum(x * x for x in x_values)
        sum_yy = sum(y * y for y in y_values)
        sum_xy = sum(x * y for x, y in zip(x_values, y_values))
        
        # Calculate correlation
        numerator = n * sum_xy - sum_x * sum_y
        denominator = ((n * sum_xx - sum_x * sum_x) * (n * sum_yy - sum_y * sum_y)) ** 0.5
        
        if denominator == 0:
            return 0.0
        
        return numerator / denominator
    
    @staticmethod
    def detect_outliers(values: List[float], method: str = 'iqr', factor: float = 1.5) -> Tuple[List[int], Dict[str, float]]:
        """Detect outliers using IQR or Z-score method"""
        if len(values) < 4:
            return [], {}
        
        outlier_indices = []
        stats = {}
        
        if method == 'iqr':
            percentiles = StatisticsHelper.calculate_percentiles(values, [25, 50, 75])
            q1, median, q3 = percentiles[25], percentiles[50], percentiles[75]
            iqr = q3 - q1
            
            lower_bound = q1 - factor * iqr
            upper_bound = q3 + factor * iqr
            
            outlier_indices = [
                i for i, val in enumerate(values)
                if val < lower_bound or val > upper_bound
            ]
            
            stats = {
                'q1': q1, 'median': median, 'q3': q3, 'iqr': iqr,
                'lower_bound': lower_bound, 'upper_bound': upper_bound
            }
        
        elif method == 'zscore':
            mean_val = sum(values) / len(values)
            variance = sum((x - mean_val) ** 2 for x in values) / (len(values) - 1)
            std_dev = variance ** 0.5
            
            outlier_indices = [
                i for i, val in enumerate(values)
                if abs((val - mean_val) / std_dev) > factor
            ]
            
            stats = {
                'mean': mean_val, 'std_dev': std_dev,
                'threshold_z_score': factor
            }
        
        return outlier_indices, stats


class HashHelper:
    """Hashing and encoding utilities"""
    
    @staticmethod
    def hash_string(text: str, algorithm: str = 'md5') -> str:
        """Generate hash of string"""
        if algorithm == 'md5':
            return hashlib.md5(text.encode()).hexdigest()
        elif algorithm == 'sha256':
            return hashlib.sha256(text.encode()).hexdigest()
        else:
            raise ValueError(f"Unsupported hash algorithm: {algorithm}")
    
    @staticmethod
    def create_cache_key(*args, separator: str = ':') -> str:
        """Create cache key from multiple arguments"""
        key_parts = [str(arg) for arg in args if arg is not None]
        combined = separator.join(key_parts)
        return HashHelper.hash_string(combined)
    
    @staticmethod
    def create_transaction_id(wallet_address: str, token_address: str, timestamp: datetime) -> str:
        """Create unique transaction identifier"""
        timestamp_str = str(int(timestamp.timestamp()))
        combined = f"{wallet_address}:{token_address}:{timestamp_str}"
        return HashHelper.hash_string(combined, 'sha256')[:16]


class PerformanceMonitor:
    """Monitor and track performance metrics"""
    
    def __init__(self):
        self.metrics = {}
        self.start_times = {}
    
    def start_timer(self, operation: str) -> None:
        """Start timing an operation"""
        self.start_times[operation] = time.time()
    
    def end_timer(self, operation: str) -> Optional[float]:
        """End timing and record duration"""
        if operation not in self.start_times:
            return None
        
        duration = time.time() - self.start_times[operation]
        del self.start_times[operation]
        
        if operation not in self.metrics:
            self.metrics[operation] = {
                'count': 0,
                'total_time': 0.0,
                'min_time': float('inf'),
                'max_time': 0.0,
                'avg_time': 0.0
            }
        
        metric = self.metrics[operation]
        metric['count'] += 1
        metric['total_time'] += duration
        metric['min_time'] = min(metric['min_time'], duration)
        metric['max_time'] = max(metric['max_time'], duration)
        metric['avg_time'] = metric['total_time'] / metric['count']
        
        return duration
    
    def get_metrics(self) -> Dict[str, Dict[str, float]]:
        """Get all recorded metrics"""
        return self.metrics.copy()
    
    def reset_metrics(self) -> None:
        """Reset all metrics"""
        self.metrics.clear()
        self.start_times.clear()


# Global performance monitor instance
performance_monitor = PerformanceMonitor()


class BatchProcessor:
    """Utilities for batch processing operations"""
    
    @staticmethod
    async def process_in_batches(
        items: List[Any],
        batch_size: int,
        processor_func,
        max_concurrent: int = 5,
        delay_between_batches: float = 0.1
    ) -> List[Any]:
        """Process items in batches with concurrency control"""
        
        if batch_size <= 0:
            raise ValueError("Batch size must be positive")
        
        if max_concurrent <= 0:
            raise ValueError("Max concurrent must be positive")
        
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_batch(batch: List[Any]) -> Any:
            async with semaphore:
                return await processor_func(batch)
        
        # Create batches
        batches = [
            items[i:i + batch_size]
            for i in range(0, len(items), batch_size)
        ]
        
        results = []
        for i, batch in enumerate(batches):
            try:
                result = await process_batch(batch)
                results.append(result)
                
                # Add delay between batches (except for the last one)
                if i < len(batches) - 1 and delay_between_batches > 0:
                    await asyncio.sleep(delay_between_batches)
                    
            except Exception as e:
                logger.error(f"Error processing batch {i + 1}/{len(batches)}: {e}")
                results.append(None)
        
        return results
    
    @staticmethod
    def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
        """Split list into chunks of specified size"""
        return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


class ErrorAnalyzer:
    """Analyze and categorize errors for better debugging"""
    
    ERROR_CATEGORIES = {
        'network': [
            'connection', 'timeout', 'unreachable', 'dns', 'ssl',
            'certificate', 'proxy', 'gateway'
        ],
        'authentication': [
            'unauthorized', 'forbidden', 'auth', 'permission', 'credential',
            'token', 'key', 'secret'
        ],
        'data': [
            'validation', 'format', 'parse', 'decode', 'encode',
            'schema', 'missing', 'invalid', 'corrupt'
        ],
        'rate_limit': [
            'rate', 'limit', 'throttle', 'quota', 'too many requests'
        ],
        'resource': [
            'memory', 'disk', 'cpu', 'storage', 'capacity', 'resource'
        ],
        'bigquery': [
            'bigquery', 'query', 'dataset', 'table', 'sql', 'syntax'
        ]
    }
    
    @classmethod
    def categorize_error(cls, error_message: str) -> str:
        """Categorize error based on message content"""
        error_lower = error_message.lower()
        
        for category, keywords in cls.ERROR_CATEGORIES.items():
            if any(keyword in error_lower for keyword in keywords):
                return category
        
        return 'unknown'
    
    @classmethod
    def suggest_resolution(cls, error_message: str, error_category: str = None) -> List[str]:
        """Suggest resolution steps based on error category"""
        if not error_category:
            error_category = cls.categorize_error(error_message)
        
        suggestions = {
            'network': [
                "Check internet connectivity",
                "Verify API endpoint URLs",
                "Check firewall and proxy settings",
                "Retry with exponential backoff"
            ],
            'authentication': [
                "Verify API keys and credentials",
                "Check authentication token expiry",
                "Confirm service account permissions",
                "Review access control settings"
            ],
            'data': [
                "Validate input data format",
                "Check required fields are present",
                "Verify data types and ranges",
                "Review data schema requirements"
            ],
            'rate_limit': [
                "Reduce request frequency",
                "Implement request queuing",
                "Use batch operations where possible",
                "Consider upgrading API plan"
            ],
            'resource': [
                "Monitor system resources",
                "Optimize memory usage",
                "Check available disk space",
                "Scale up infrastructure if needed"
            ],
            'bigquery': [
                "Check SQL query syntax",
                "Verify table and dataset names",
                "Review BigQuery quotas",
                "Check dataset permissions"
            ],
            'unknown': [
                "Review error logs for patterns",
                "Check recent code changes",
                "Verify system configuration",
                "Contact support if issue persists"
            ]
        }
        
        return suggestions.get(error_category, suggestions['unknown'])


class DataSanitizer:
    """Sanitize and clean data for safe processing"""
    
    @staticmethod
    def sanitize_string(value: Any, max_length: int = 1000, allow_none: bool = True) -> Optional[str]:
        """Sanitize string value"""
        if value is None:
            return None if allow_none else ""
        
        if not isinstance(value, str):
            value = str(value)
        
        # Remove control characters and excessive whitespace
        value = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', value)
        value = re.sub(r'\s+', ' ', value).strip()
        
        # Truncate if too long
        if len(value) > max_length:
            value = value[:max_length].rstrip()
        
        return value
    
    @staticmethod
    def sanitize_numeric(value: Any, allow_none: bool = True) -> Optional[float]:
        """Sanitize numeric value"""
        if value is None:
            return None if allow_none else 0.0
        
        try:
            # Handle various numeric types
            if isinstance(value, (int, float)):
                result = float(value)
            elif isinstance(value, str):
                # Remove common non-numeric characters
                cleaned = re.sub(r'[,$%\s]', '', value)
                result = float(cleaned)
            else:
                raise ValueError(f"Cannot convert {type(value)} to numeric")
            
            # Check for infinity and NaN
            if not (result == result):  # NaN check
                return None if allow_none else 0.0
            if abs(result) == float('inf'):
                return None if allow_none else 0.0
            
            return result
            
        except (ValueError, TypeError):
            logger.warning(f"Could not sanitize numeric value: {value}")
            return None if allow_none else 0.0
    
    @staticmethod
    def sanitize_dict(data: Dict[str, Any], schema: Optional[Dict[str, type]] = None) -> Dict[str, Any]:
        """Sanitize dictionary data"""
        sanitized = {}
        
        for key, value in data.items():
            # Sanitize key
            clean_key = DataSanitizer.sanitize_string(key, max_length=100, allow_none=False)
            if not clean_key:
                continue
            
            # Sanitize value based on schema if provided
            if schema and clean_key in schema:
                expected_type = schema[clean_key]
                
                if expected_type == str:
                    sanitized[clean_key] = DataSanitizer.sanitize_string(value)
                elif expected_type in (int, float):
                    sanitized[clean_key] = DataSanitizer.sanitize_numeric(value)
                elif expected_type == bool:
                    sanitized[clean_key] = bool(value) if value is not None else None
                else:
                    sanitized[clean_key] = value
            else:
                # Auto-detect and sanitize
                if isinstance(value, str):
                    sanitized[clean_key] = DataSanitizer.sanitize_string(value)
                elif isinstance(value, (int, float)):
                    sanitized[clean_key] = DataSanitizer.sanitize_numeric(value)
                else:
                    sanitized[clean_key] = value
        
        return sanitized


class QueryBuilder:
    """Helper for building BigQuery SQL queries"""
    
    @staticmethod
    def build_where_clause(filters: Dict[str, Any]) -> Tuple[str, List[str]]:
        """Build WHERE clause from filters dictionary"""
        conditions = []
        parameters = []
        
        for field, value in filters.items():
            if value is None:
                continue
            
            field = DataSanitizer.sanitize_string(field, max_length=50, allow_none=False)
            if not field:
                continue
            
            if isinstance(value, list):
                if value:  # Non-empty list
                    placeholders = ', '.join(['?' for _ in value])
                    conditions.append(f"{field} IN ({placeholders})")
                    parameters.extend(value)
            
            elif isinstance(value, dict):
                # Handle range queries like {'min': 100, 'max': 500}
                if 'min' in value and value['min'] is not None:
                    conditions.append(f"{field} >= ?")
                    parameters.append(value['min'])
                
                if 'max' in value and value['max'] is not None:
                    conditions.append(f"{field} <= ?")
                    parameters.append(value['max'])
            
            elif isinstance(value, str) and value.startswith('%') and value.endswith('%'):
                # LIKE query
                conditions.append(f"{field} LIKE ?")
                parameters.append(value)
            
            else:
                conditions.append(f"{field} = ?")
                parameters.append(value)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        return where_clause, parameters
    
    @staticmethod
    def build_order_clause(sort_by: Optional[str] = None, sort_direction: str = 'ASC') -> str:
        """Build ORDER BY clause"""
        if not sort_by:
            return ""
        
        sort_by = DataSanitizer.sanitize_string(sort_by, max_length=50, allow_none=False)
        direction = 'DESC' if sort_direction.upper() == 'DESC' else 'ASC'
        
        return f"ORDER BY {sort_by} {direction}"
    
    @staticmethod
    def build_limit_clause(limit: Optional[int] = None, offset: Optional[int] = None) -> str:
        """Build LIMIT and OFFSET clause"""
        clause_parts = []
        
        if limit is not None and limit > 0:
            clause_parts.append(f"LIMIT {limit}")
        
        if offset is not None and offset > 0:
            clause_parts.append(f"OFFSET {offset}")
        
        return " ".join(clause_parts)


class WalletAnalyzer:
    """Advanced wallet analysis utilities"""
    
    @staticmethod
    def analyze_trading_patterns(transactions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze trading patterns from transaction history"""
        if not transactions:
            return {}
        
        # Sort by timestamp
        sorted_txns = sorted(transactions, key=lambda x: x.get('timestamp', datetime.min))
        
        # Calculate time patterns
        hours = [DateTimeHelper.from_timestamp(tx['timestamp']).hour for tx in sorted_txns]
        days_of_week = [DateTimeHelper.from_timestamp(tx['timestamp']).weekday() for tx in sorted_txns]
        
        # Activity patterns
        hourly_distribution = {}
        for hour in hours:
            hourly_distribution[hour] = hourly_distribution.get(hour, 0) + 1
        
        daily_distribution = {}
        for day in days_of_week:
            daily_distribution[day] = daily_distribution.get(day, 0) + 1
        
        # Transaction intervals
        intervals = []
        for i in range(1, len(sorted_txns)):
            prev_time = DateTimeHelper.from_timestamp(sorted_txns[i-1]['timestamp'])
            curr_time = DateTimeHelper.from_timestamp(sorted_txns[i]['timestamp'])
            interval_hours = (curr_time - prev_time).total_seconds() / 3600
            intervals.append(interval_hours)
        
        # Calculate metrics
        avg_interval = sum(intervals) / len(intervals) if intervals else 0
        
        # Find most active hours and days
        peak_hour = max(hourly_distribution.items(), key=lambda x: x[1])[0] if hourly_distribution else 0
        peak_day = max(daily_distribution.items(), key=lambda x: x[1])[0] if daily_distribution else 0
        
        # Trading session detection (transactions within 1 hour = same session)
        sessions = 0
        session_transactions = []
        current_session_size = 1
        
        for interval in intervals:
            if interval <= 1:  # Within 1 hour
                current_session_size += 1
            else:
                sessions += 1
                session_transactions.append(current_session_size)
                current_session_size = 1
        
        if current_session_size > 0:
            sessions += 1
            session_transactions.append(current_session_size)
        
        avg_session_size = sum(session_transactions) / len(session_transactions) if session_transactions else 1
        
        return {
            'total_transactions': len(sorted_txns),
            'trading_sessions': sessions,
            'avg_session_size': round(avg_session_size, 1),
            'avg_interval_hours': round(avg_interval, 2),
            'peak_trading_hour': peak_hour,
            'peak_trading_day': peak_day,
            'hourly_distribution': hourly_distribution,
            'daily_distribution': daily_distribution,
            'weekend_activity_pct': (
                (daily_distribution.get(5, 0) + daily_distribution.get(6, 0)) / 
                len(sorted_txns) * 100
            ),
            'business_hours_activity_pct': sum(
                count for hour, count in hourly_distribution.items()
                if 9 <= hour <= 17
            ) / len(sorted_txns) * 100
        }
    
    @staticmethod
    def detect_trading_style(
        avg_hold_hours: float,
        win_rate: float,
        avg_trade_size: float,
        diversification_score: float,
        gas_efficiency: float
    ) -> Dict[str, Any]:
        """Detect wallet trading style based on metrics"""
        
        style_indicators = {
            'scalper': (
                avg_hold_hours < 2 and
                win_rate > 55 and
                gas_efficiency > 0.8
            ),
            'day_trader': (
                2 <= avg_hold_hours < 48 and
                win_rate > 50 and
                diversification_score > 0.3
            ),
            'swing_trader': (
                48 <= avg_hold_hours < 336 and  # 2 days to 2 weeks
                diversification_score > 0.4
            ),
            'position_trader': (
                avg_hold_hours >= 336 and
                diversification_score > 0.2
            ),
            'arbitrageur': (
                avg_hold_hours < 1 and
                win_rate > 70 and
                gas_efficiency > 0.9
            ),
            'whale': (
                avg_trade_size > 50000
            ),
            'gas_inefficient': (
                gas_efficiency < 0.3
            ),
            'high_frequency': (
                avg_hold_hours < 0.5 and
                diversification_score < 0.2
            )
        }
        
        detected_styles = [
            style for style, condition in style_indicators.items()
            if condition
        ]
        
        # Determine primary style
        primary_style = 'mixed'
        if len(detected_styles) == 1:
            primary_style = detected_styles[0]
        elif 'whale' in detected_styles:
            primary_style = 'whale'
        elif 'arbitrageur' in detected_styles:
            primary_style = 'arbitrageur'
        elif 'scalper' in detected_styles:
            primary_style = 'scalper'
        elif 'day_trader' in detected_styles:
            primary_style = 'day_trader'
        elif 'swing_trader' in detected_styles:
            primary_style = 'swing_trader'
        elif 'position_trader' in detected_styles:
            primary_style = 'position_trader'
        
        return {
            'primary_style': primary_style,
            'detected_styles': detected_styles,
            'style_confidence': len([s for s in detected_styles if s not in ['gas_inefficient']]) / 6,
            'characteristics': {
                'avg_hold_hours': avg_hold_hours,
                'win_rate': win_rate,
                'diversification': diversification_score,
                'gas_efficiency': gas_efficiency,
                'avg_trade_size': avg_trade_size
            }
        }


class TokenAnalyzer:
    """Token analysis utilities"""
    
    @staticmethod
    def calculate_smart_money_metrics(
        total_holders: int,
        elite_holders: int,
        total_volume: float,
        elite_volume: float,
        recent_elite_activity: int
    ) -> Dict[str, float]:
        """Calculate smart money interest metrics"""
        
        # Basic ratios
        elite_holder_ratio = elite_holders / max(1, total_holders)
        elite_volume_ratio = elite_volume / max(1, total_volume)
        
        # Activity score
        activity_score = min(1.0, recent_elite_activity / 10)  # Normalize to 0-1
        
        # Overall smart money score (weighted combination)
        smart_money_score = (
            elite_holder_ratio * 0.4 +      # 40% holder ratio
            elite_volume_ratio * 0.4 +      # 40% volume ratio
            activity_score * 0.2            # 20% recent activity
        )
        
        return {
            'elite_holder_ratio': round(elite_holder_ratio, 4),
            'elite_volume_ratio': round(elite_volume_ratio, 4),
            'activity_score': round(activity_score, 4),
            'smart_money_score': round(smart_money_score, 4),
            'smart_money_grade': (
                'A+' if smart_money_score >= 0.9 else
                'A' if smart_money_score >= 0.8 else
                'B+' if smart_money_score >= 0.7 else
                'B' if smart_money_score >= 0.6 else
                'C+' if smart_money_score >= 0.5 else
                'C' if smart_money_score >= 0.4 else
                'D' if smart_money_score >= 0.3 else 'F'
            )
        }
    
    @staticmethod
    def analyze_accumulation_pattern(
        buy_volume: float,
        sell_volume: float,
        buy_count: int,
        sell_count: int
    ) -> Dict[str, Any]:
        """Analyze accumulation vs distribution patterns"""
        
        total_volume = buy_volume + sell_volume
        total_count = buy_count + sell_count
        
        if total_volume == 0 or total_count == 0:
            return {
                'pattern': 'insufficient_data',
                'accumulation_score': 0,
                'confidence': 0
            }
        
        # Volume-based accumulation score (-1 to 1)
        volume_accumulation = (buy_volume - sell_volume) / total_volume
        
        # Count-based accumulation score
        count_accumulation = (buy_count - sell_count) / total_count
        
        # Combined score (weighted towards volume)
        accumulation_score = volume_accumulation * 0.7 + count_accumulation * 0.3
        
        # Determine pattern
        if accumulation_score > 0.3:
            pattern = 'strong_accumulation'
        elif accumulation_score > 0.1:
            pattern = 'accumulation'
        elif accumulation_score > -0.1:
            pattern = 'balanced'
        elif accumulation_score > -0.3:
            pattern = 'distribution'
        else:
            pattern = 'strong_distribution'
        
        # Confidence based on volume and transaction count
        confidence = min(1.0, (total_count / 10) * (total_volume / 10000))
        
        return {
            'pattern': pattern,
            'accumulation_score': round(accumulation_score, 3),
            'volume_accumulation': round(volume_accumulation, 3),
            'count_accumulation': round(count_accumulation, 3),
            'confidence': round(confidence, 3),
            'buy_sell_ratio': round(buy_volume / max(1, sell_volume), 2),
            'transaction_ratio': round(buy_count / max(1, sell_count), 2)
        }


class RiskCalculator:
    """Risk assessment utilities"""
    
    @staticmethod
    def calculate_position_risk(
        position_size_usd: float,
        wallet_total_value_usd: float,
        token_volatility: float = 0.1
    ) -> Dict[str, float]:
        """Calculate position risk metrics"""
        
        if wallet_total_value_usd <= 0:
            return {'position_risk': 0, 'risk_level': 'unknown'}
        
        # Position size as percentage of wallet
        position_percentage = (position_size_usd / wallet_total_value_usd) * 100
        
        # Risk-adjusted position size
        risk_adjusted_exposure = position_percentage * (1 + token_volatility)
        
        # Risk level classification
        if risk_adjusted_exposure > 50:
            risk_level = 'extreme'
        elif risk_adjusted_exposure > 25:
            risk_level = 'high'
        elif risk_adjusted_exposure > 10:
            risk_level = 'medium'
        elif risk_adjusted_exposure > 5:
            risk_level = 'low'
        else:
            risk_level = 'minimal'
        
        return {
            'position_percentage': round(position_percentage, 2),
            'risk_adjusted_exposure': round(risk_adjusted_exposure, 2),
            'risk_level': risk_level,
            'recommended_max_position': min(20, max(5, 100 / (1 + token_volatility * 10)))
        }
    
    @staticmethod
    def calculate_portfolio_risk(positions: List[Dict[str, float]]) -> Dict[str, float]:
        """Calculate portfolio-level risk metrics"""
        if not positions:
            return {}
        
        # Calculate concentration risk
        total_value = sum(pos.get('value_usd', 0) for pos in positions)
        if total_value <= 0:
            return {}
        
        # Position sizes as percentages
        position_percentages = [
            (pos.get('value_usd', 0) / total_value) * 100 
            for pos in positions
        ]
        
        # Herfindahl-Hirschman Index (concentration measure)
        hhi = sum(pct ** 2 for pct in position_percentages)
        
        # Diversification score (inverse of concentration)
        diversification = max(0, min(1, (10000 - hhi) / 10000))
        
        # Largest position percentage
        max_position = max(position_percentages) if position_percentages else 0
        
        # Risk classification
        if hhi > 5000:  # Highly concentrated
            concentration_risk = 'high'
        elif hhi > 2500:
            concentration_risk = 'medium'
        else:
            concentration_risk = 'low'
        
        return {
            'herfindahl_index': round(hhi, 2),
            'diversification_score': round(diversification, 3),
            'largest_position_pct': round(max_position, 2),
            'concentration_risk': concentration_risk,
            'total_positions': len(positions),
            'effective_positions': round(10000 / hhi, 1) if hhi > 0 else len(positions)
        }


class JSONHelper:
    """JSON serialization utilities with datetime support"""
    
    @staticmethod
    def serialize_datetime(obj: Any) -> Any:
        """Custom JSON serializer for datetime objects"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    @staticmethod
    def safe_json_dumps(data: Any, **kwargs) -> str:
        """Safe JSON dumps with datetime handling"""
        return json.dumps(data, default=JSONHelper.serialize_datetime, **kwargs)
    
    @staticmethod
    def safe_json_loads(json_str: str) -> Optional[Any]:
        """Safe JSON loads with error handling"""
        try:
            return json.loads(json_str)
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(f"JSON decode error: {e}")
            return None
    
    @staticmethod
    def flatten_dict(data: Dict[str, Any], separator: str = '.', max_depth: int = 5) -> Dict[str, Any]:
        """Flatten nested dictionary"""
        def _flatten(obj, parent_key='', depth=0):
            if depth >= max_depth:
                return {parent_key: obj}
            
            items = []
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_key = f"{parent_key}{separator}{key}" if parent_key else key
                    items.extend(_flatten(value, new_key, depth + 1).items())
            else:
                items.append((parent_key, obj))
            return dict(items)
        
        return _flatten(data)


class AlertManager:
    """Manage alerts and notifications for the system"""
    
    @dataclass
    class Alert:
        level: str  # 'info', 'warning', 'error', 'critical'
        message: str
        component: str
        timestamp: datetime
        data: Optional[Dict[str, Any]] = None
        
        def to_dict(self) -> Dict[str, Any]:
            return {
                'level': self.level,
                'message': self.message,
                'component': self.component,
                'timestamp': self.timestamp.isoformat(),
                'data': self.data
            }
    
    def __init__(self, max_alerts: int = 1000):
        self.alerts: List[AlertManager.Alert] = []
        self.max_alerts = max_alerts
    
    def add_alert(
        self,
        level: str,
        message: str,
        component: str,
        data: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add an alert to the system"""
        alert = self.Alert(
            level=level,
            message=message,
            component=component,
            timestamp=DateTimeHelper.utc_now(),
            data=data
        )
        
        self.alerts.append(alert)
        
        # Keep only the most recent alerts
        if len(self.alerts) > self.max_alerts:
            self.alerts = self.alerts[-self.max_alerts:]
        
        # Log the alert
        log_method = getattr(logger, level.lower(), logger.info)
        log_method(f"[{component}] {message}")
    
    def get_recent_alerts(
        self,
        hours_back: int = 24,
        levels: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get recent alerts with optional filtering"""
        cutoff_time = DateTimeHelper.utc_now() - timedelta(hours=hours_back)
        
        filtered_alerts = [
            alert for alert in self.alerts
            if alert.timestamp >= cutoff_time
        ]
        
        if levels:
            levels_lower = [level.lower() for level in levels]
            filtered_alerts = [
                alert for alert in filtered_alerts
                if alert.level.lower() in levels_lower
            ]
        
        return [alert.to_dict() for alert in filtered_alerts]
    
    def get_alert_summary(self, hours_back: int = 24) -> Dict[str, int]:
        """Get summary of alerts by level"""
        recent_alerts = self.get_recent_alerts(hours_back)
        
        summary = {'info': 0, 'warning': 0, 'error': 0, 'critical': 0}
        
        for alert in recent_alerts:
            level = alert['level'].lower()
            if level in summary:
                summary[level] += 1
        
        return summary
    
    def clear_old_alerts(self, hours_back: int = 168) -> int:
        """Clear alerts older than specified hours"""
        cutoff_time = DateTimeHelper.utc_now() - timedelta(hours=hours_back)
        initial_count = len(self.alerts)
        
        self.alerts = [
            alert for alert in self.alerts
            if alert.timestamp >= cutoff_time
        ]
        
        return initial_count - len(self.alerts)


# Global alert manager instance
alert_manager = AlertManager()


class DatabaseHelper:
    """Database utility functions"""
    
    @staticmethod
    def escape_sql_identifier(identifier: str) -> str:
        """Escape SQL identifier (table/column names)"""
        # Remove dangerous characters and limit length
        clean_id = re.sub(r'[^a-zA-Z0-9_]', '', identifier)
        return clean_id[:63]  # BigQuery identifier limit
    
    @staticmethod
    def build_insert_query(
        table_name: str,
        data: List[Dict[str, Any]],
        dataset_ref: str
    ) -> Tuple[str, List[tuple]]:
        """Build parameterized INSERT query"""
        if not data:
            return "", []
        
        # Get all unique columns
        all_columns = set()
        for row in data:
            all_columns.update(row.keys())
        
        columns = sorted([DatabaseHelper.escape_sql_identifier(col) for col in all_columns])
        
        # Build query
        table_ref = f"`{dataset_ref}.{DatabaseHelper.escape_sql_identifier(table_name)}`"
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["?" for _ in columns])
        
        query = f"INSERT INTO {table_ref} ({columns_str}) VALUES ({placeholders})"
        
        # Prepare values
        values = []
        for row in data:
            row_values = tuple(row.get(col, None) for col in columns)
            values.append(row_values)
        
        return query, values
    
    @staticmethod
    def parse_bigquery_error(error_message: str) -> Dict[str, str]:
        """Parse BigQuery error message for better debugging"""
        error_info = {
            'type': 'unknown',
            'description': error_message,
            'suggestion': 'Check query syntax and data format'
        }
        
        error_lower = error_message.lower()
        
        if 'syntax error' in error_lower or 'invalid query' in error_lower:
            error_info.update({
                'type': 'syntax_error',
                'suggestion': 'Review SQL syntax and table/column names'
            })
        elif 'not found' in error_lower:
            error_info.update({
                'type': 'resource_not_found',
                'suggestion': 'Verify table/dataset exists and check permissions'
            })
        elif 'quota' in error_lower or 'limit' in error_lower:
            error_info.update({
                'type': 'quota_exceeded',
                'suggestion': 'Check BigQuery quotas and optimize queries'
            })
        elif 'permission' in error_lower or 'access denied' in error_lower:
            error_info.update({
                'type': 'permission_error',
                'suggestion': 'Check BigQuery IAM permissions'
            })
        elif 'timeout' in error_lower:
            error_info.update({
                'type': 'timeout',
                'suggestion': 'Optimize query performance or increase timeout'
            })
        
        return error_info


class SystemMonitor:
    """System health monitoring utilities"""
    
    def __init__(self):
        self.health_checks = {}
        self.last_check_time = {}
    
    def register_health_check(self, component: str, check_func, interval_seconds: int = 300):
        """Register a health check function for a component"""
        self.health_checks[component] = {
            'check_func': check_func,
            'interval_seconds': interval_seconds,
            'last_result': None,
            'last_success': None,
            'consecutive_failures': 0
        }
    
    async def run_health_checks(self, force: bool = False) -> Dict[str, Dict[str, Any]]:
        """Run all registered health checks"""
        results = {}
        current_time = time.time()
        
        for component, check_info in self.health_checks.items():
            last_check = self.last_check_time.get(component, 0)
            
            # Skip if not enough time has passed (unless forced)
            if not force and (current_time - last_check) < check_info['interval_seconds']:
                results[component] = check_info['last_result'] or {'status': 'pending'}
                continue
            
            try:
                # Run the health check
                check_result = await check_info['check_func']()
                
                check_info['last_result'] = {
                    'status': 'healthy',
                    'timestamp': datetime.utcnow().isoformat(),
                    'details': check_result,
                    'response_time_ms': (time.time() - current_time) * 1000
                }
                check_info['last_success'] = current_time
                check_info['consecutive_failures'] = 0
                
            except Exception as e:
                check_info['consecutive_failures'] += 1
                check_info['last_result'] = {
                    'status': 'unhealthy',
                    'timestamp': datetime.utcnow().isoformat(),
                    'error': str(e),
                    'consecutive_failures': check_info['consecutive_failures'],
                    'response_time_ms': (time.time() - current_time) * 1000
                }
                
                # Add alert for failures
                alert_manager.add_alert(
                    'error' if check_info['consecutive_failures'] < 3 else 'critical',
                    f"Health check failed for {component}: {str(e)}",
                    'system_monitor',
                    {'consecutive_failures': check_info['consecutive_failures']}
                )
            
            self.last_check_time[component] = current_time
            results[component] = check_info['last_result']
        
        return results
    
    def get_system_health_summary(self) -> Dict[str, Any]:
        """Get overall system health summary"""
        healthy_components = 0
        total_components = len(self.health_checks)
        
        for component, check_info in self.health_checks.items():
            if (check_info['last_result'] and 
                check_info['last_result'].get('status') == 'healthy'):
                healthy_components += 1
        
        health_percentage = (healthy_components / total_components * 100) if total_components > 0 else 0
        
        overall_status = 'healthy'
        if health_percentage < 100:
            overall_status = 'degraded'
        if health_percentage < 50:
            overall_status = 'unhealthy'
        
        return {
            'overall_status': overall_status,
            'health_percentage': round(health_percentage, 1),
            'healthy_components': healthy_components,
            'total_components': total_components,
            'last_check': max(self.last_check_time.values()) if self.last_check_time else 0
        }


# Global system monitor instance
system_monitor = SystemMonitor()


class LoggingHelper:
    """Enhanced logging utilities"""
    
    @staticmethod
    def setup_structured_logging(
        service_name: str,
        log_level: str = 'INFO',
        include_trace_id: bool = True
    ) -> None:
        """Setup structured logging for the service"""
        
        class StructuredFormatter(logging.Formatter):
            def format(self, record):
                log_entry = {
                    'timestamp': datetime.utcnow().isoformat(),
                    'service': service_name,
                    'level': record.levelname,
                    'logger': record.name,
                    'message': record.getMessage(),
                    'module': record.module,
                    'function': record.funcName,
                    'line': record.lineno
                }
                
                # Add exception info if present
                if record.exc_info:
                    log_entry['exception'] = logging.Formatter().formatException(record.exc_info)
                
                # Add extra fields if present
                if hasattr(record, 'extra_fields'):
                    log_entry.update(record.extra_fields)
                
                return json.dumps(log_entry)
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, log_level.upper()))
        
        # Remove existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Add structured handler
        handler = logging.StreamHandler()
        handler.setFormatter(StructuredFormatter())
        root_logger.addHandler(handler)
    
    @staticmethod
    def log_with_context(
        logger_instance: logging.Logger,
        level: str,
        message: str,
        **context
    ) -> None:
        """Log message with additional context"""
        extra_fields = {k: v for k, v in context.items() if k != 'message'}
        
        # Create a log record with extra fields
        record = logging.LogRecord(
            name=logger_instance.name,
            level=getattr(logging, level.upper()),
            pathname='',
            lineno=0,
            msg=message,
            args=(),
            exc_info=None
        )
        record.extra_fields = extra_fields
        
        logger_instance.handle(record)


class HealthChecker:
    """Predefined health check functions"""
    
    @staticmethod
    async def check_bigquery_connectivity(bq_client) -> Dict[str, Any]:
        """Health check for BigQuery connectivity"""
        try:
            # Simple query to test connectivity
            query = "SELECT 1 as test_value"
            job = bq_client.client.query(query)
            result = list(job.result())
            
            return {
                'connectivity': 'ok',
                'query_time_ms': job.ended - job.started if job.ended and job.started else 0,
                'project_id': bq_client.project_id,
                'dataset_id': bq_client.dataset_id
            }
        except Exception as e:
            raise Exception(f"BigQuery connectivity failed: {str(e)}")
    
    @staticmethod
    async def check_price_fetcher_health(price_fetcher) -> Dict[str, Any]:
        """Health check for price fetcher service"""
        if not price_fetcher:
            raise Exception("Price fetcher not initialized")
        
        try:
            stats = price_fetcher.get_statistics()
            health_result = await price_fetcher.health_check()
            
            return {
                'price_sources_healthy': health_result.get('overall', {}).get('healthy_sources', 0),
                'cache_hit_rate': stats.get('cache_hit_rate_pct', 0),
                'success_rate': stats.get('success_rate_pct', 0),
                'avg_response_time_ms': stats.get('average_response_time_ms', 0)
            }
        except Exception as e:
            raise Exception(f"Price fetcher health check failed: {str(e)}")


class DataExporter:
    """Export data in various formats"""
    
    @staticmethod
    def to_csv_string(data: List[Dict[str, Any]], include_headers: bool = True) -> str:
        """Convert data to CSV string"""
        if not data:
            return ""
        
        import csv
        import io
        
        output = io.StringIO()
        
        # Get all column names
        columns = list(data[0].keys()) if data else []
        
        writer = csv.DictWriter(output, fieldnames=columns)
        
        if include_headers:
            writer.writeheader()
        
        for row in data:
            # Sanitize row data
            clean_row = {}
            for key, value in row.items():
                if isinstance(value, datetime):
                    clean_row[key] = value.isoformat()
                elif isinstance(value, (dict, list)):
                    clean_row[key] = json.dumps(value)
                else:
                    clean_row[key] = value
            
            writer.writerow(clean_row)
        
        return output.getvalue()
    
    @staticmethod
    def to_excel_bytes(data: List[Dict[str, Any]], sheet_name: str = 'Data') -> bytes:
        """Convert data to Excel bytes (requires openpyxl)"""
        try:
            import openpyxl
            from openpyxl.utils.dataframe import dataframe_to_rows
            import pandas as pd
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Create workbook
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = sheet_name
            
            # Add data
            for r in dataframe_to_rows(df, index=False, header=True):
                ws.append(r)
            
            # Save to bytes
            import io
            output = io.BytesIO()
            wb.save(output)
            output.seek(0)
            
            return output.read()
            
        except ImportError:
            raise Exception("openpyxl and pandas required for Excel export")


class RequestIDGenerator:
    """Generate unique request IDs for tracing"""
    
    @staticmethod
    def generate_request_id(prefix: str = 'req') -> str:
        """Generate unique request ID"""
        timestamp = str(int(time.time() * 1000))  # Milliseconds
        random_part = hashlib.md5(f"{timestamp}{time.time()}".encode()).hexdigest()[:8]
        return f"{prefix}_{timestamp}_{random_part}"
    
    @staticmethod
    def extract_timestamp_from_id(request_id: str) -> Optional[datetime]:
        """Extract timestamp from request ID"""
        try:
            parts = request_id.split('_')
            if len(parts) >= 2:
                timestamp_ms = int(parts[1])
                return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        except (ValueError, IndexError):
            pass
        return None


class ConfigurationValidator:
    """Validate service configuration"""
    
    @staticmethod
    def validate_service_config() -> Dict[str, Any]:
        """Validate all service configuration"""
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'config_values': {}
        }
        
        # Check required environment variables
        required_env_vars = ConfigHelper.validate_required_config()
        if required_env_vars:
            validation_results['valid'] = False
            validation_results['errors'].extend([
                f"Missing required environment variable: {var}" 
                for var in required_env_vars
            ])
        
        # Check numeric configurations
        try:
            port = ConfigHelper.get_env_int('PORT', 8080, min_value=1024, max_value=65535)
            validation_results['config_values']['port'] = port
        except Exception as e:
            validation_results['warnings'].append(f"Port configuration issue: {e}")
        
        try:
            log_level = ConfigHelper.get_env_var('LOG_LEVEL', 'INFO')
            if log_level.upper() not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
                validation_results['warnings'].append(f"Invalid log level: {log_level}")
            validation_results['config_values']['log_level'] = log_level
        except Exception as e:
            validation_results['warnings'].append(f"Log level configuration issue: {e}")
        
        # Check BigQuery configuration
        try:
            project_id = ConfigHelper.get_env_var('GOOGLE_CLOUD_PROJECT')
            dataset_id = ConfigHelper.get_env_var('BIGQUERY_DATASET', 'crypto_analysis')
            validation_results['config_values'].update({
                'project_id': project_id,
                'dataset_id': dataset_id
            })
        except Exception as e:
            validation_results['errors'].append(f"BigQuery configuration error: {e}")
            validation_results['valid'] = False
        
        # Check optional API configurations
        api_keys = {
            'coingecko_api_key': ConfigHelper.get_env_var('COINGECKO_API_KEY'),
            'coinmarketcap_api_key': ConfigHelper.get_env_var('COINMARKETCAP_API_KEY'),
            'moralis_api_key': ConfigHelper.get_env_var('MORALIS_API_KEY')
        }
        
        missing_apis = [name for name, key in api_keys.items() if not key]
        if missing_apis:
            validation_results['warnings'].append(
                f"Missing optional API keys: {', '.join(missing_apis)} - may affect price data quality"
            )
        
        validation_results['config_values']['api_keys_configured'] = [
            name for name, key in api_keys.items() if key
        ]
        
        return validation_results


# Utility functions for common operations
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


def calculate_compound_score(
    individual_scores: Dict[str, float],
    weights: Dict[str, float]
) -> float:
    """Calculate weighted compound score"""
    if not individual_scores or not weights:
        return 0.0
    
    total_weight = 0.0
    weighted_sum = 0.0
    
    for metric, score in individual_scores.items():
        weight = weights.get(metric, 0.0)
        if weight > 0:
            weighted_sum += score * weight
            total_weight += weight
    
    return weighted_sum / total_weight if total_weight > 0 else 0.0


# Exception classes for better error handling
class ValidationError(Exception):
    """Raised when data validation fails"""
    def __init__(self, message: str, field: str = None, value: Any = None):
        super().__init__(message)
        self.field = field
        self.value = value


class ConfigurationError(Exception):
    """Raised when configuration is invalid"""
    pass


class DataQualityError(Exception):
    """Raised when data quality issues are detected"""
    def __init__(self, message: str, data_source: str = None, quality_score: float = None):
        super().__init__(message)
        self.data_source = data_source
        self.quality_score = quality_score


# Performance tracking decorators
class PerformanceTracker:
    """Track performance metrics for functions"""
    
    def __init__(self):
        self.metrics = {}
    
    def track_performance(self, operation_name: str):
        """Decorator to track function performance"""
        def decorator(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = await func(*args, **kwargs)
                    self._record_success(operation_name, time.time() - start_time)
                    return result
                except Exception as e:
                    self._record_failure(operation_name, time.time() - start_time, str(e))
                    raise
            
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    self._record_success(operation_name, time.time() - start_time)
                    return result
                except Exception as e:
                    self._record_failure(operation_name, time.time() - start_time, str(e))
                    raise
            
            return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
        return decorator
    
    def _record_success(self, operation: str, duration: float):
        """Record successful operation"""
        if operation not in self.metrics:
            self.metrics[operation] = {
                'total_calls': 0,
                'successful_calls': 0,
                'failed_calls': 0,
                'total_duration': 0.0,
                'avg_duration': 0.0,
                'min_duration': float('inf'),
                'max_duration': 0.0,
                'last_error': None
            }
        
        metric = self.metrics[operation]
        metric['total_calls'] += 1
        metric['successful_calls'] += 1
        metric['total_duration'] += duration
        metric['avg_duration'] = metric['total_duration'] / metric['total_calls']
        metric['min_duration'] = min(metric['min_duration'], duration)
        metric['max_duration'] = max(metric['max_duration'], duration)
    
    def _record_failure(self, operation: str, duration: float, error: str):
        """Record failed operation"""
        if operation not in self.metrics:
            self.metrics[operation] = {
                'total_calls': 0,
                'successful_calls': 0,
                'failed_calls': 0,
                'total_duration': 0.0,
                'avg_duration': 0.0,
                'min_duration': float('inf'),
                'max_duration': 0.0,
                'last_error': None
            }
        
        metric = self.metrics[operation]
        metric['total_calls'] += 1
        metric['failed_calls'] += 1
        metric['total_duration'] += duration
        metric['avg_duration'] = metric['total_duration'] / metric['total_calls']
        metric['last_error'] = error
    
    def get_performance_report(self) -> Dict[str, Any]:
        """Get comprehensive performance report"""
        report = {
            'operations': self.metrics.copy(),
            'summary': {
                'total_operations': len(self.metrics),
                'total_calls': sum(m['total_calls'] for m in self.metrics.values()),
                'total_failures': sum(m['failed_calls'] for m in self.metrics.values()),
                'overall_success_rate': 0.0,
                'avg_operation_time': 0.0
            },
            'generated_at': datetime.utcnow().isoformat()
        }
        
        total_calls = report['summary']['total_calls']
        total_failures = report['summary']['total_failures']
        
        if total_calls > 0:
            report['summary']['overall_success_rate'] = ((total_calls - total_failures) / total_calls) * 100
            total_duration = sum(m['total_duration'] for m in self.metrics.values())
            report['summary']['avg_operation_time'] = total_duration / total_calls
        
        return report


# Global performance tracker
performance_tracker = PerformanceTracker()


# Initialization function for the utils module
def initialize_utils(service_name: str = "wallet-scoring-service", log_level: str = "INFO") -> Dict[str, Any]:
    """Initialize all utility components"""
    initialization_results = {
        'service_name': service_name,
        'initialized_at': datetime.utcnow().isoformat(),
        'components': {}
    }
    
    try:
        # Setup logging
        LoggingHelper.setup_structured_logging(service_name, log_level)
        initialization_results['components']['logging'] = 'initialized'
        
        # Validate configuration
        config_validation = ConfigurationValidator.validate_service_config()
        initialization_results['components']['configuration'] = {
            'status': 'valid' if config_validation['valid'] else 'invalid',
            'errors': config_validation['errors'],
            'warnings': config_validation['warnings']
        }
        
        # Initialize monitoring
        # Initialize monitoring
        initialization_results['components']['monitoring'] = 'initialized'
        
        # Setup performance tracking
        initialization_results['components']['performance_tracking'] = 'initialized'
        
        logger.info(f"Utils module initialized successfully for {service_name}")
        
    except Exception as e:
        logger.error(f"Error initializing utils module: {e}")
        initialization_results['components']['initialization_error'] = str(e)
    
    return initialization_results


# Context managers for better resource management
class TimedOperation:
    """Context manager for timing operations"""
    
    def __init__(self, operation_name: str, logger_instance: Optional[logging.Logger] = None):
        self.operation_name = operation_name
        self.logger = logger_instance or logger
        self.start_time = None
        self.duration = None
    
    def __enter__(self):
        self.start_time = time.time()
        self.logger.debug(f"Starting operation: {self.operation_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.duration = time.time() - self.start_time
        
        if exc_type is None:
            self.logger.info(f"Operation {self.operation_name} completed in {self.duration:.3f}s")
        else:
            self.logger.error(f"Operation {self.operation_name} failed after {self.duration:.3f}s: {exc_val}")
        
        # Record in performance tracker
        performance_tracker._record_success(self.operation_name, self.duration)


class AsyncTimedOperation:
    """Async context manager for timing operations"""
    
    def __init__(self, operation_name: str, logger_instance: Optional[logging.Logger] = None):
        self.operation_name = operation_name
        self.logger = logger_instance or logger
        self.start_time = None
        self.duration = None
    
    async def __aenter__(self):
        self.start_time = time.time()
        self.logger.debug(f"Starting async operation: {self.operation_name}")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.duration = time.time() - self.start_time
        
        if exc_type is None:
            self.logger.info(f"Async operation {self.operation_name} completed in {self.duration:.3f}s")
        else:
            self.logger.error(f"Async operation {self.operation_name} failed after {self.duration:.3f}s: {exc_val}")


# Utility classes for specific domain operations
class ScoreValidation:
    """Validation utilities specific to wallet scoring"""
    
    @staticmethod
    def validate_score_adjustment(
        current_score: int,
        adjustment: int,
        min_score: int = 25,
        max_score: int = 1000
    ) -> Tuple[bool, str, int]:
        """Validate and clamp score adjustment"""
        
        if not DataValidator.validate_score_range(current_score, min_score, max_score):
            return False, f"Current score {current_score} out of valid range", current_score
        
        new_score = current_score + adjustment
        
        # Clamp to valid range
        clamped_score = max(min_score, min(max_score, new_score))
        
        # Check if clamping occurred
        was_clamped = clamped_score != new_score
        warning = ""
        
        if was_clamped:
            if new_score < min_score:
                warning = f"Score adjustment clamped to minimum ({min_score})"
            else:
                warning = f"Score adjustment clamped to maximum ({max_score})"
        
        return True, warning, clamped_score
    
    @staticmethod
    def assess_score_change_impact(score_change: int) -> Dict[str, str]:
        """Assess the impact level of a score change"""
        abs_change = abs(score_change)
        
        if abs_change >= 200:
            impact = 'critical'
            description = 'Major score change requiring immediate attention'
        elif abs_change >= 100:
            impact = 'high'
            description = 'Significant score change requiring review'
        elif abs_change >= 50:
            impact = 'medium'
            description = 'Notable score change worth monitoring'
        elif abs_change >= 25:
            impact = 'low'
            description = 'Minor score adjustment'
        else:
            impact = 'minimal'
            description = 'Negligible score change'
        
        direction = 'improvement' if score_change < 0 else 'degradation' if score_change > 0 else 'neutral'
        
        return {
            'impact_level': impact,
            'change_direction': direction,
            'description': description,
            'requires_review': impact in ['critical', 'high']
        }


class MetricAggregator:
    """Aggregate and summarize metrics across different dimensions"""
    
    @staticmethod
    def aggregate_wallet_metrics(wallet_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate metrics across multiple wallets"""
        if not wallet_data:
            return {}
        
        # Numeric fields to aggregate
        numeric_fields = [
            'sophistication_score', 'total_transactions', 'unique_tokens',
            'total_volume_usd', 'win_rate', 'avg_profit_pct', 'gas_efficiency_score',
            'diversification_index', 'activity_frequency_score'
        ]
        
        aggregated = {
            'wallet_count': len(wallet_data),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        for field in numeric_fields:
            values = [
                wallet.get(field, 0) for wallet in wallet_data 
                if wallet.get(field) is not None
            ]
            
            if values:
                aggregated[f'{field}_avg'] = round(sum(values) / len(values), 3)
                aggregated[f'{field}_min'] = round(min(values), 3)
                aggregated[f'{field}_max'] = round(max(values), 3)
                aggregated[f'{field}_median'] = round(sorted(values)[len(values)//2], 3)
        
        # Tier distribution
        tiers = [WalletTierClassifier.get_tier(w.get('sophistication_score', 250)) for w in wallet_data]
        tier_counts = {}
        for tier in tiers:
            tier_counts[tier] = tier_counts.get(tier, 0) + 1
        
        aggregated['tier_distribution'] = tier_counts
        
        return aggregated
    
    @staticmethod
    def aggregate_token_metrics(token_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate metrics across multiple tokens"""
        if not token_data:
            return {}
        
        aggregated = {
            'token_count': len(token_data),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Smart money metrics
        smart_money_scores = [t.get('smart_money_score', 0) for t in token_data if t.get('smart_money_score')]
        if smart_money_scores:
            aggregated['avg_smart_money_score'] = round(sum(smart_money_scores) / len(smart_money_scores), 3)
            aggregated['top_smart_money_tokens'] = len([s for s in smart_money_scores if s > 0.7])
        
        # Elite holder analysis
        elite_holders = [t.get('elite_wallet_holders', 0) for t in token_data]
        total_elite_interest = sum(elite_holders)
        
        aggregated['total_elite_interest'] = total_elite_interest
        aggregated['avg_elite_holders_per_token'] = round(total_elite_interest / len(token_data), 1)
        aggregated['tokens_with_elite_interest'] = len([h for h in elite_holders if h > 0])
        
        return aggregated


class HealthMetrics:
    """Calculate health metrics for system components"""
    
    @staticmethod
    def calculate_data_pipeline_health(
        recent_transactions: int,
        recent_price_updates: int,
        recent_matches: int,
        recent_score_calculations: int,
        target_transactions_per_hour: int = 100
    ) -> Dict[str, Any]:
        """Calculate overall data pipeline health score"""
        
        # Component health scores (0-100)
        transaction_health = min(100, (recent_transactions / target_transactions_per_hour) * 100)
        price_health = min(100, (recent_price_updates / 50) * 100)  # Target 50 price updates per hour
        matching_health = min(100, (recent_matches / 20) * 100)     # Target 20 matches per hour
        scoring_health = 100 if recent_score_calculations > 0 else 0
        
        # Overall health (weighted average)
        overall_health = (
            transaction_health * 0.4 +    # 40% transactions
            price_health * 0.3 +          # 30% price data
            matching_health * 0.2 +       # 20% matching
            scoring_health * 0.1          # 10% scoring
        )
        
        # Health status
        if overall_health >= 90:
            status = 'excellent'
        elif overall_health >= 75:
            status = 'good'
        elif overall_health >= 60:
            status = 'fair'
        elif overall_health >= 40:
            status = 'poor'
        else:
            status = 'critical'
        
        return {
            'overall_health_score': round(overall_health, 1),
            'health_status': status,
            'component_scores': {
                'transactions': round(transaction_health, 1),
                'price_data': round(price_health, 1),
                'trade_matching': round(matching_health, 1),
                'score_calculation': round(scoring_health, 1)
            },
            'recommendations': HealthMetrics._generate_health_recommendations(
                transaction_health, price_health, matching_health, scoring_health
            )
        }
    
    @staticmethod
    def _generate_health_recommendations(
        transaction_health: float,
        price_health: float,
        matching_health: float,
        scoring_health: float
    ) -> List[str]:
        """Generate recommendations based on component health"""
        recommendations = []
        
        if transaction_health < 50:
            recommendations.append("Increase transaction data ingestion frequency")
        
        if price_health < 50:
            recommendations.append("Improve price data collection - add more sources or increase update frequency")
        
        if matching_health < 50:
            recommendations.append("Review trade matching algorithm - may need parameter tuning")
        
        if scoring_health < 50:
            recommendations.append("Score calculation pipeline may be failing - check for errors")
        
        if not recommendations:
            recommendations.append("All systems operating normally")
        
        return recommendations


# Final utility functions
def create_response_with_metadata(
    data: Any,
    operation: str,
    duration_seconds: float,
    additional_metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Create response with performance metadata"""
    response = {
        'data': data,
        'metadata': {
            'operation': operation,
            'duration_seconds': round(duration_seconds, 3),
            'timestamp': datetime.utcnow().isoformat(),
            'service': 'wallet-scoring-service'
        }
    }
    
    if additional_metadata:
        response['metadata'].update(additional_metadata)
    
    return response


def calculate_service_uptime(start_time: datetime) -> Dict[str, Any]:
    """Calculate service uptime metrics"""
    current_time = DateTimeHelper.utc_now()
    uptime_delta = current_time - start_time
    
    uptime_seconds = uptime_delta.total_seconds()
    uptime_hours = uptime_seconds / 3600
    uptime_days = uptime_hours / 24
    
    return {
        'start_time': start_time.isoformat(),
        'current_time': current_time.isoformat(),
        'uptime_seconds': round(uptime_seconds, 2),
        'uptime_hours': round(uptime_hours, 2),
        'uptime_days': round(uptime_days, 2),
        'uptime_formatted': DateTimeHelper.format_duration(uptime_hours)
    }


# Export commonly used instances and functions
__all__ = [
    'ResponseFormatter', 'AddressValidator', 'DateTimeHelper', 'PriceCalculator',
    'MetricsCalculator', 'DataValidator', 'CacheManager', 'ConfigHelper',
    'WalletTierClassifier', 'TradeTypeClassifier', 'StatisticsHelper',
    'HashHelper', 'PerformanceMonitor', 'BatchProcessor', 'ErrorAnalyzer',
    'DataSanitizer', 'QueryBuilder', 'WalletAnalyzer', 'TokenAnalyzer',
    'RiskCalculator', 'JSONHelper', 'AlertManager', 'SystemMonitor',
    'LoggingHelper', 'HealthChecker', 'DataExporter', 'RequestIDGenerator',
    'ConfigurationValidator', 'ScoreValidation', 'MetricAggregator',
    'HealthMetrics', 'PerformanceTracker', 'TimedOperation', 'AsyncTimedOperation',
    'ValidationError', 'ConfigurationError', 'DataQualityError',
    'performance_monitor', 'alert_manager', 'system_monitor', 'performance_tracker',
    'async_timer', 'retry_on_failure', 'safe_divide', 'safe_percentage',
    'truncate_address', 'format_large_number', 'calculate_compound_score',
    'create_response_with_metadata', 'calculate_service_uptime', 'initialize_utils'
]
        
class AddressValidator:
    @staticmethod
    def get_address_checksum(address: str) -> str:
        """Get EIP-55 checksum address (simplified version)"""
        if not AddressValidator.is_valid_address(address):
            raise ValueError(f"Invalid address: {address}")
        
        # Simplified checksum - just return lowercase for consistency
        # Full EIP-55 implementation would require Keccak-256 hashing
        return AddressValidator.normalize_address(address)


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
                # Try parsing ISO format first
                try:
                    return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except ValueError:
                    # Try parsing as numeric string
                    timestamp = float(timestamp)
            
            if isinstance(timestamp, (int, float)):
                # Handle both seconds and milliseconds
                if timestamp > 1e12:  # Milliseconds
                    timestamp = timestamp / 1000
                return datetime.fromtimestamp(timestamp, tz=timezone.utc)
                
        except (ValueError, TypeError, OSError) as e:
            raise ValueError(f"Invalid timestamp format: {timestamp}") from e
    
    @staticmethod
    def to_timestamp(dt: datetime) -> int:
        """Convert datetime to Unix timestamp"""
        return int(dt.timestamp())
    
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
    
    @staticmethod
    def get_time_bucket(dt: datetime, bucket_size_hours: int = 1) -> datetime:
        """Round datetime to specified hour bucket"""
        hour = (dt.hour // bucket_size_hours) * bucket_size_hours
        return dt.replace(hour=hour, minute=0, second=0, microsecond=0)
    
    @staticmethod
    def is_recent(dt: datetime, hours_threshold: int = 24) -> bool:
        """Check if datetime is within recent threshold"""
        return (DateTimeHelper.utc_now() - dt).total_seconds() / 3600 <= hours_threshold
    
    @staticmethod
    def business_hours_only(dt: datetime) -> bool:
        """Check if datetime falls within business hours (Mon-Fri, 9-17 UTC)"""
        return (dt.weekday() < 5 and 9 <= dt.hour < 17)


class PriceCalculator:
    """Price calculation and validation utilities"""
    
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
        
        # Use minimum of buy/sell amount for matched portion
        traded_amount = min(buy_amount, sell_amount)
        
        buy_value = traded_amount * buy_price
        sell_value = traded_amount * sell_price
        gas_cost_usd = gas_cost_eth * eth_price_usd
        
        return sell_value - buy_value - gas_cost_usd
    
    @staticmethod
    def calculate_roi(profit_usd: float, invested_usd: float) -> Optional[float]:
        """Calculate return on investment percentage"""
        if invested_usd <= 0:
            return None
        return (profit_usd / invested_usd) * 100
    
    @staticmethod
    def annualize_return(roi_percent: float, hold_hours: float) -> Optional[float]:
        """Annualize a return based on holding period"""
        if hold_hours <= 0:
            return None
        return roi_percent * (8760 / hold_hours)  # 8760 hours in a year
    
    @staticmethod
    def calculate_gas_efficiency(gas_cost_usd: float, trade_value_usd: float) -> float:
        """Calculate gas efficiency ratio (0-1, higher is better)"""
        if trade_value_usd <= 0:
            return 0.0
        
        gas_ratio = gas_cost_usd / trade_value_usd
        # Convert to efficiency score (invert and clamp)
        return max(0, min(1, 1 / (1 + gas_ratio)))
    
    @staticmethod
    def is_price_reasonable(price: float, symbol: str) -> bool:
        """Basic price reasonableness check"""
        if price <= 0:
            return False
        
        # Very loose bounds - mainly to catch obvious data errors
        if price > 1_000_000:  # $1M per token is extreme
            logger.warning(f"Unusually high price for {symbol}: ${price}")
            return False
        
        if price < 0.000001:  # Less than $0.000001 is suspicious
            logger.warning(f"Unusually low price for {symbol}: ${price}")
            return False
        
        return True
    
    @staticmethod
    def validate_price_change(change_percent: float) -> bool:
        """Validate price change for reasonableness"""
        # Allow large gains but be suspicious of extreme values
        if abs(change_percent) > 50000:  # 50000% change is likely an error
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
    def calculate_sharpe_ratio(
        returns: List[float], 
        risk_free_rate: float = 2.0
    ) -> Optional[float]:
        """Calculate Sharpe ratio for a series of returns"""
        if len(returns) < 2:
            return None
        
        avg_return = sum(returns) / len(returns)
        
        # Calculate standard deviation
        variance = sum((r - avg_return) ** 2 for r in returns) / (len(returns) - 1)
        std_dev = variance ** 0.5
        
        if std_dev == 0:
            return None
        
        return (avg_return - risk_free_rate) / std_dev
    
    @staticmethod
    def calculate_max_drawdown(returns: List[float]) -> float:
        """Calculate maximum drawdown from a series of returns"""
        if not returns:
            return 0.0
        
        # Calculate cumulative returns
        cumulative = [1.0]
        for ret in returns:
            cumulative.append(cumulative[-1] * (1 + ret / 100))
        
        # Calculate drawdowns
        max_value = cumulative[0]
        max_drawdown = 0.0
        
        for value in cumulative[1:]:
            max_value = max(max_value, value)
            drawdown = (max_value - value) / max_value
            max_drawdown = max(max_drawdown, drawdown)
        
        return max_drawdown * 100  # Return as percentage
    
    @staticmethod
    def calculate_diversification_score(unique_assets: int, total_trades: int) -> float:
        """Calculate diversification score (0-1)"""
        if total_trades <= 0:
            return 0.0
        
        # Normalize based on reasonable diversification expectations
        max_reasonable_assets = min(20, total_trades)  # Can't have more assets than trades
        normalized_assets = min(unique_assets, max_reasonable_assets)
        
        return normalized_assets / max_reasonable_assets
    
    @staticmethod
    def calculate_activity_frequency_score(
        transactions: int, 
        days_active: int,
        max_daily_frequency: int = 10
    ) -> float:
        """Calculate activity frequency score (0-1)"""
        if days_active <= 0:
            return 0.0
        
        avg_daily_transactions = transactions / days_active
        return min(1.0, avg_daily_transactions / max_daily_frequency)
    
    @staticmethod
    def calculate_sophistication_indicators(
        unique_platforms: int,
        avg_gas_efficiency: float,
        diversification_score: float,
        activity_frequency: float,
        win_rate: float
    ) -> Dict[str, float]:
        """Calculate comprehensive sophistication indicators"""
        
        # Platform diversity score (0-1)
        platform_score = min(1.0, unique_platforms / 5)  # 5+ platforms = max score
        
        # Gas efficiency score (already 0-1)
        gas_score = max(0, min(1, avg_gas_efficiency))
        
        # Performance score based on win rate (0-1)
        performance_score = min(1.0, win_rate / 70)  # 70%+ win rate = max score
        
        # Combined sophistication score (weighted average)
        overall_score = (
            platform_score * 0.20 +      # 20% platform diversity
            gas_score * 0.25 +           # 25% gas efficiency
            diversification_score * 0.20 + # 20% diversification
            activity_frequency * 0.15 +   # 15% activity
            performance_score * 0.20       # 20% performance
        )
        
        return {
            'platform_diversity': round(platform_score, 3),
            'gas_efficiency': round(gas_score, 3),
            'diversification': round(diversification_score, 3),
            'activity_frequency': round(activity_frequency, 3),
            'performance': round(performance_score, 3),
            'overall_sophistication': round(overall_score, 3)
        }


class DataValidator:
    """Data validation utilities"""
    
    @staticmethod
    def validate_transaction_data(transaction: Dict[str, Any]) -> Tuple[bool, List[str]]:
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
            elif isinstance(transaction[field], str) and not transaction[field].strip():
                errors.append(f"Empty required field: {field}")
        
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
        
        # Validate numeric fields
        numeric_fields = ['token_amount', 'cost_in_eth', 'wallet_sophistication_score']
        for field in numeric_fields:
            if field in transaction:
                try:
                    value = float(transaction[field])
                    if field == 'wallet_sophistication_score' and (value < 0 or value > 1000):
                        errors.append(f"wallet_sophistication_score must be between 0-1000")
                    elif field in ['token_amount', 'cost_in_eth'] and value < 0:
                        errors.append(f"{field} cannot be negative")
                except (ValueError, TypeError):
                    errors.append(f"Invalid numeric value for {field}")
        
        # Validate timestamp
        if 'timestamp' in transaction:
            if isinstance(transaction['timestamp'], str):
                try:
                    DateTimeHelper.from_timestamp(transaction['timestamp'])
                except ValueError:
                    errors.append("Invalid timestamp format")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def validate_price_data(price_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate price data structure and values"""
        errors = []
        
        required_fields = ['token_address', 'token_symbol', 'price_usd']
        for field in required_fields:
            if field not in price_data or price_data[field] is None:
                errors.append(f"Missing required field: {field}")
        
        # Validate address
        if 'token_address' in price_data:
            if not AddressValidator.is_valid_address(price_data['token_address']):
                errors.append("Invalid token_address format")
        
        # Validate prices
        price_fields = ['price_usd', 'price_eth', 'volume_24h', 'market_cap']
        for field in price_fields:
            if field in price_data and price_data[field] is not None:
                try:
                    value = float(price_data[field])
                    if value < 0:
                        errors.append(f"{field} cannot be negative")
                    elif field == 'price_usd' and not PriceCalculator.is_price_reasonable(value, price_data.get('token_symbol', '')):
                        errors.append(f"price_usd value seems unreasonable: {value}")
                except (ValueError, TypeError):
                    errors.append(f"Invalid numeric value for {field}")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def sanitize_token_symbol(symbol: str) -> str:
        """Clean and standardize token symbol"""
        if not symbol or not isinstance(symbol, str):
            return "UNKNOWN"
        
        # Remove whitespace and convert to uppercase
        symbol = symbol.strip().upper()
        
        # Remove special characters except common ones
        symbol = re.sub(r'[^A-Z0-9._-]', '', symbol)
        
        # Limit length
        return symbol[:20]
    
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
    
    def delete(self, key: str) -> bool:
        """Delete key from cache"""
        if key in self.cache:
            del self.cache[key]
            return True
        return False
    
    def clear(self) -> int:
        """Clear all cache entries and return count cleared"""
        count = len(self.cache)
        self.cache.clear()
        return count
    
    def cleanup_expired(self) -> int:
        """Remove expired entries and return count removed"""
        current_time = datetime.utcnow()
        expired_keys = [
            key for key, (_, expiry) in self.cache.items()
            if current_time >= expiry
        ]
        
        for key in expired_keys:
            del self.cache[key]
        
        return len(expired_keys)
    
    def get_stats(self) -> Dict[str, int]:
        """Get cache statistics"""
        current_time = datetime.utcnow()
        expired_count = sum(
            1 for _, expiry in self.cache.values()
            if current_time >= expiry
        )
        
        return {
            'total_entries': len(self.cache),
            'expired_entries': expired_count,
            'active_entries': len(self.cache) - expired_count
        }


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
    
    @staticmethod
    def get_env_list(key: str, default: Optional[List[str]] = None, separator: str = ',') -> List[str]:
        """Get list from environment variable"""
        value = os.environ.get(key)
        if not value:
            return default or []
        
        return [item.strip() for item in value.split(separator) if item.strip()]
    
    @staticmethod
    def validate_required_config() -> List[str]:
        """Validate all required configuration is present"""
        missing = []
        
        # Add your required environment variables here
        required_vars = [
            'GOOGLE_CLOUD_PROJECT',  # Usually required for BigQuery
        ]
        
        for var in required_vars:
            if not os.environ.get(var):
                missing.append(var)
        
        return missing


# Decorators and utility functions
def async_timer(func):
    """Decorator to time async function execution"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = await func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.debug(f"{func.__name__} executed in {execution_time:.3f}s")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"{func.__name__} failed after {execution_time:.3f}s: {e}")
            raise
    return wrapper


def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Decorator to retry function on failure with exponential backoff"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        logger.error(f"{func.__name__} failed after {max_retries + 1} attempts: {e}")
                        raise e
                    
                    sleep_time = delay * (backoff ** attempt)
                    logger.warning(f"{func.__name__} attempt {attempt + 1} failed: {e}. Retrying in {sleep_time:.1f}s...")
                    await asyncio.sleep(sleep_time)
            
            raise last_exception
        return wrapper
    return decorator


class WalletTierClassifier:
    """Classify wallets into tiers based on sophistication scores"""
    
    TIER_THRESHOLDS = {
        'Elite': (0, 179),      # 0-179: Elite traders
        'High': (180, 219),     # 180-219: High-skill traders  
        'Medium': (220, 259),   # 220-259: Medium-skill traders
        'Low': (260, 1000)      # 260+: Low-skill traders
    }
    
    @classmethod
    def get_tier(cls, sophistication_score: int) -> str:
        """Get wallet tier from sophistication score"""
        for tier, (min_score, max_score) in cls.TIER_THRESHOLDS.items():
            if min_score <= sophistication_score <= max_score:
                return tier
        return 'Unknown'
    
    @classmethod
    def get_tier_bounds(cls, tier: str) -> Optional[Tuple[int, int]]:
        """Get score bounds for a tier"""
        return cls.TIER_THRESHOLDS.get(tier)
    
    @classmethod
    def get_all_tiers(cls) -> List[str]:
        """Get list of all tier names"""
        return list(cls.TIER_THRESHOLDS.keys())
    
    @classmethod
    def calculate_tier_percentile(cls, score: int, tier: str) -> float:
        """Calculate percentile within a tier (0-100)"""
        bounds = cls.get_tier_bounds(tier)
        if not bounds:
            return 0.0
        
        min_score, max_score = bounds
        if score < min_score or score > max_score:
            return 0.0
        
        # Lower scores are better, so invert the percentile
        tier_range = max_score - min_score
        position = score - min_score
        percentile = (1 - (position / tier_range)) * 100
        
        return round(percentile, 1)


class TradeTypeClassifier:
    """Classify trades based on holding period and other factors"""
    
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
    
    @staticmethod
    def classify_by_performance(
        hold_hours: float, 
        price_change_percent: float,
        trade_volume_usd: float
    ) -> Dict[str, str]:
        """Classify trade with multiple dimensions"""
        
        # Time-based classification
        time_type = TradeTypeClassifier.classify_by_hold_time(hold_hours)
        
        # Performance classification
        if price_change_percent > 100:
            performance_type = 'Exceptional Gain'
        elif price_change_percent > 20:
            performance_type = 'Strong Gain'
        elif price_change_percent > 5:
            performance_type = 'Moderate Gain'
        elif price_change_percent > -5:
            performance_type = 'Small Move'
        elif price_change_percent > -20:
            performance_type = 'Moderate Loss'
        elif price_change_percent > -50:
            performance_type = 'Large Loss'
        else:
            performance_type = 'Major Loss'
        
        # Size classification
        if trade_volume_usd >= 100000:
            size_type = 'Whale Trade'
        elif trade_volume_usd >= 10000:
            size_type = 'Large Trade'
        elif trade_volume_usd >= 1000:
            size_type = 'Medium Trade'
        elif trade_volume_usd >= 100:
            size_type = 'Small Trade'
        else:
            size_type = 'Micro Trade'
        
        return {
            'time_classification': time_type,
            'performance_classification': performance_type,
            'size_classification': size_type,
            'composite_type': f"{time_type} - {performance_type}"
        }
    
    @staticmethod
    def get_trade_risk_level(
        hold_hours: float,
        price_change_percent: float,
        gas_efficiency_ratio: float
    ) -> str:
        """Determine risk level of a trade"""
        risk_score = 0
        
        # Time risk (shorter = higher risk)
        if hold_hours < 1:
            risk_score += 3
        elif hold_hours < 24:
            risk_score += 2
        elif hold_hours < 168:
            risk_score += 1
        
        # Performance risk (extreme moves = higher risk)
        if abs(price_change_percent) > 500:
            risk_score += 4
        elif abs(price_change_percent) > 100:
            risk_score += 3
        elif abs(price_change_percent) > 50:
            risk_score += 2
        elif abs(price_change_percent) > 20:
            risk_score += 1
        
        # Gas efficiency risk (inefficient = higher risk)
        if gas_efficiency_ratio > 0.1:  # Gas > 10% of trade
            risk_score += 3
        elif gas_efficiency_ratio > 0.05:  # Gas > 5% of trade
            risk_score += 2
        elif gas_efficiency_ratio > 0.02:  # Gas > 2% of trade
            risk_score += 1
        
        # Convert score to risk level
        if risk_score >= 8:
            return 'Extreme'
        elif risk_score >= 6:
            return 'High'
        elif risk_score >= 4:
            return 'Medium'
        elif risk_score >= 2:
            return 'Low'
        else:
            return 'Minimal'
    
    @staticmethod
    def analyze_trade_quality(
        price_change_percent: float,
        hold_hours: float,
        gas_efficiency_ratio: float,
        trade_volume_usd: float
    ) -> Dict[str, Any]:
        """Comprehensive trade quality analysis"""
        
        # Base quality score (0-100)
        quality_score = 50  # Start neutral
        
        # Performance impact (±40 points max)
        if price_change_percent > 100:
            quality_score += 40
        elif price_change_percent > 50:
            quality_score += 30
        elif price_change_percent > 20:
            quality_score += 20
        elif price_change_percent > 5:
            quality_score += 10
        elif price_change_percent > 0:
            quality_score += 5
        elif price_change_percent > -10:
            quality_score -= 5
        elif price_change_percent > -25:
            quality_score -= 15
        elif price_change_percent > -50:
            quality_score -= 25
        else:
            quality_score -= 35
        
        # Gas efficiency impact (±20 points max)
        if gas_efficiency_ratio < 0.005:  # < 0.5%
            quality_score += 20
        elif gas_efficiency_ratio < 0.01:  # < 1%
            quality_score += 15
        elif gas_efficiency_ratio < 0.02:  # < 2%
            quality_score += 10
        elif gas_efficiency_ratio < 0.05:  # < 5%
            quality_score += 5
        elif gas_efficiency_ratio > 0.2:   # > 20%
            quality_score -= 20
        elif gas_efficiency_ratio > 0.1:   # > 10%
            quality_score -= 15
        elif gas_efficiency_ratio > 0.05:  # > 5%
            quality_score -= 10
        
        # Size impact (±15 points max)
        if trade_volume_usd >= 100000:
            quality_score += 15
        elif trade_volume_usd >= 10000:
            quality_score += 10
        elif trade_volume_usd >= 1000:
            quality_score += 5
        elif trade_volume_usd < 50:
            quality_score -= 10
        
        # Timing appropriateness (±10 points max)
        trade_type = TradeTypeClassifier.classify_by_hold_time(hold_hours)
        if trade_type == 'Scalp' and price_change_percent > 5:
            quality_score += 10  # Good scalp
        elif trade_type == 'Day Trade' and abs(price_change_percent) > 10:
            quality_score += 8   # Good day trade
        elif trade_type == 'Swing Trade' and abs(price_change_percent) > 20:
            quality_score += 10  # Good swing trade
        elif trade_type == 'Position Trade' and price_change_percent > 30:
            quality_score += 10  # Good position trade
        
        # Clamp to 0-100 range
        quality_score = max(0, min(100, quality_score))
        
        # Quality grade
        if quality_score >= 90:
            grade = 'A+'
        elif quality_score >= 85:
            grade = 'A'
        elif quality_score >= 80:
            grade = 'B+'
        elif quality_score >= 75:
            grade = 'B'
        elif quality_score >= 70:
            grade = 'C+'
        elif quality_score >= 65:
            grade = 'C'
        elif quality_score >= 60:
            grade = 'D'
        else:
            grade = 'F'
        
        return {
            'quality_score': round(quality_score, 1),
            'quality_grade': grade,
            'trade_type': trade_type,
            'risk_level': TradeTypeClassifier.get_trade_risk_level(
                hold_hours, price_change_percent, gas_efficiency_ratio
            ),
            'strengths': TradeTypeClassifier._identify_trade_strengths(
                price_change_percent, gas_efficiency_ratio, trade_volume_usd
            ),
            'weaknesses': TradeTypeClassifier._identify_trade_weaknesses(
                price_change_percent, gas_efficiency_ratio, trade_volume_usd
            )
        }
    
    @staticmethod
    def _identify_trade_strengths(
        price_change_percent: float,
        gas_efficiency_ratio: float,
        trade_volume_usd: float
    ) -> List[str]:
        """Identify positive aspects of a trade"""
        strengths = []
        
        if price_change_percent > 50:
            strengths.append("Exceptional profit margin")
        elif price_change_percent > 20:
            strengths.append("Strong profit margin")
        elif price_change_percent > 5:
            strengths.append("Positive return")
        
        if gas_efficiency_ratio < 0.01:
            strengths.append("Excellent gas efficiency")
        elif gas_efficiency_ratio < 0.02:
            strengths.append("Good gas efficiency")
        
        if trade_volume_usd >= 50000:
            strengths.append("Large position size")
        elif trade_volume_usd >= 5000:
            strengths.append("Significant trade size")
        
        return strengths
    
    @staticmethod
    def _identify_trade_weaknesses(
        price_change_percent: float,
        gas_efficiency_ratio: float,
        trade_volume_usd: float
    ) -> List[str]:
        """Identify negative aspects of a trade"""
        weaknesses = []
        
        if price_change_percent < -50:
            weaknesses.append("Major loss")
        elif price_change_percent < -20:
            weaknesses.append("Significant loss")
        elif price_change_percent < 0:
            weaknesses.append("Unprofitable trade")
        
        if gas_efficiency_ratio > 0.1:
            weaknesses.append("Poor gas efficiency")
        elif gas_efficiency_ratio > 0.05:
            weaknesses.append("High gas costs")
        
        if trade_volume_usd < 100:
            weaknesses.append("Very small trade size")
        
        return weaknesses


class StatisticsHelper:
    """Statistical calculation utilities"""
    
    @staticmethod
    def calculate_percentiles(values: List[float], percentiles: List[int] = [25, 50, 75, 90, 95]) -> Dict[int, float]:
        """Calculate percentiles for a list of values"""
        if not values:
            return {p: 0.0 for p in percentiles}
        
        sorted_values = sorted(values)
        n = len(sorted_values)
        result = {}
        
        for p in percentiles:
            if p < 0 or p > 100:
                continue
            
            if p == 0:
                result[p] = sorted_values[0]
            elif p == 100:
                result[p] = sorted_values[-1]
            else:
                # Linear interpolation method
                index = (p / 100) * (n - 1)
                lower_index = int(index)
                upper_index = min(lower_index + 1, n - 1)
                
                if lower_index == upper_index:
                    result[p] = sorted_values[lower_index]
                else:
                    weight = index - lower_index
                    result[p] = (sorted_values[lower_index] * (1 - weight) + 
                               sorted_values[upper_index] * weight)
        
        return result
    
    @staticmethod
    def calculate_moving_average(values: List[float], window: int) -> List[float]:
        """Calculate moving average with specified window"""
        if window <= 0 or window > len(values):
            return values.copy()
        
        moving_averages = []
        for i in range(len(values)):
            start_idx = max(0, i - window + 1)
            window_values = values[start_idx:i + 1]
            moving_averages.append(sum(window_values) / len(window_values))
        
        return moving_averages
    
    @staticmethod
    def calculate_correlation(x_values: List[float], y_values: List[float]) -> Optional[float]:
        """Calculate Pearson correlation coefficient"""
        if len(x_values) != len(y_values) or len(x_values) < 2:
            return None
        
        n = len(x_values)
        sum_x = sum(x_values)
        sum_y = sum(y_values)
        sum_xx = sum(x * x for x in x_values)
        sum_yy = sum(y * y for y in y_values)
        sum_xy = sum(x * y for x, y in zip(x_values, y_values))
        
        # Calculate correlation
        numerator = n * sum_xy - sum_x * sum_y
        denominator = ((n * sum_xx - sum_x * sum_x) * (n * sum_yy - sum_y * sum_y)) ** 0.5
        
        if denominator == 0:
            return 0.0
        
        return numerator / denominator
    
    @staticmethod
    def detect_outliers(values: List[float], method: str = 'iqr', factor: float = 1.5) -> Tuple[List[int], Dict[str, float]]:
        """Detect outliers using IQR or Z-score method"""
        if len(values) < 4:
            return [], {}
        
        outlier_indices = []
        stats = {}
        
        if method == 'iqr':
            percentiles = StatisticsHelper.calculate_percentiles(values, [25, 50, 75])
            q1, median, q3 = percentiles[25], percentiles[50], percentiles[75]
            iqr = q3 - q1
            
            lower_bound = q1 - factor * iqr
            upper_bound = q3 + factor * iqr
            
            outlier_indices = [
                i for i, val in enumerate(values)
                if val < lower_bound or val > upper_bound
            ]
            
            stats = {
                'q1': q1, 'median': median, 'q3': q3, 'iqr': iqr,
                'lower_bound': lower_bound, 'upper_bound': upper_bound
            }
        
        elif method == 'zscore':
            mean_val = sum(values) / len(values)
            variance = sum((x - mean_val) ** 2 for x in values) / (len(values) - 1)
            std_dev = variance ** 0.5
            
            outlier_indices = [
                i for i, val in enumerate(values)
                if abs((val - mean_val) / std_dev) > factor
            ]
            
            stats = {
                'mean': mean_val, 'std_dev': std_dev,
                'threshold_z_score': factor
            }
        
        return outlier_indices, stats
    
    @staticmethod
    def calculate_volatility(returns: List[float], annualize: bool = False) -> float:
        """Calculate volatility (standard deviation of returns)"""
        if len(returns) < 2:
            return 0.0
        
        mean_return = sum(returns) / len(returns)
        variance = sum((r - mean_return) ** 2 for r in returns) / (len(returns) - 1)
        volatility = variance ** 0.5
        
        if annualize:
            # Assume daily returns, annualize by sqrt(365)
            volatility *= (365 ** 0.5)
        
        return volatility
    
    @staticmethod
    def calculate_skewness(values: List[float]) -> float:
        """Calculate skewness of a distribution"""
        if len(values) < 3:
            return 0.0
        
        n = len(values)
        mean_val = sum(values) / n
        variance = sum((x - mean_val) ** 2 for x in values) / (n - 1)
        std_dev = variance ** 0.5
        
        if std_dev == 0:
            return 0.0
        
        skewness = sum((x - mean_val) ** 3 for x in values) / (n * std_dev ** 3)
        return skewness
    
    @staticmethod
    def calculate_kurtosis(values: List[float]) -> float:
        """Calculate kurtosis (excess kurtosis) of a distribution"""
        if len(values) < 4:
            return 0.0
        
        n = len(values)
        mean_val = sum(values) / n
        variance = sum((x - mean_val) ** 2 for x in values) / (n - 1)
        std_dev = variance ** 0.5
        
        if std_dev == 0:
            return 0.0
        
        kurtosis = sum((x - mean_val) ** 4 for x in values) / (n * std_dev ** 4) - 3
        return kurtosis


class HashHelper:
    """Hashing and encoding utilities"""
    
    @staticmethod
    def hash_string(text: str, algorithm: str = 'md5') -> str:
        """Generate hash of string"""
        if algorithm == 'md5':
            return hashlib.md5(text.encode()).hexdigest()
        elif algorithm == 'sha256':
            return hashlib.sha256(text.encode()).hexdigest()
        else:
            raise ValueError(f"Unsupported hash algorithm: {algorithm}")
    
    @staticmethod
    def create_cache_key(*args, separator: str = ':') -> str:
        """Create cache key from multiple arguments"""
        key_parts = [str(arg) for arg in args if arg is not None]
        combined = separator.join(key_parts)
        return HashHelper.hash_string(combined)
    
    @staticmethod
    def create_transaction_id(wallet_address: str, token_address: str, timestamp: datetime) -> str:
        """Create unique transaction identifier"""
        timestamp_str = str(int(timestamp.timestamp()))
        combined = f"{wallet_address}:{token_address}:{timestamp_str}"
        return HashHelper.hash_string(combined, 'sha256')[:16]
    
    @staticmethod
    def create_trade_pair_id(buy_hash: str, sell_hash: str) -> str:
        """Create unique identifier for a trade pair"""
        combined = f"{buy_hash}:{sell_hash}"
        return HashHelper.hash_string(combined, 'sha256')[:12]
    
    @staticmethod
    def generate_session_id() -> str:
        """Generate unique session identifier"""
        timestamp = str(int(time.time() * 1000))
        random_part = os.urandom(8).hex()
        return f"session_{timestamp}_{random_part}"


class PerformanceMonitor:
    """Monitor and track performance metrics"""
    
    def __init__(self):
        self.metrics = {}
        self.start_times = {}
    
    def start_timer(self, operation: str) -> None:
        """Start timing an operation"""
        self.start_times[operation] = time.time()
    
    def end_timer(self, operation: str) -> Optional[float]:
        """End timing and record duration"""
        if operation not in self.start_times:
            return None
        
        duration = time.time() - self.start_times[operation]
        del self.start_times[operation]
        
        if operation not in self.metrics:
            self.metrics[operation] = {
                'count': 0,
                'total_time': 0.0,
                'min_time': float('inf'),
                'max_time': 0.0,
                'avg_time': 0.0,
                'success_count': 0,
                'failure_count': 0,
                'last_error': None
            }
        
        metric = self.metrics[operation]
        metric['count'] += 1
        metric['total_time'] += duration
        metric['min_time'] = min(metric['min_time'], duration)
        metric['max_time'] = max(metric['max_time'], duration)
        metric['avg_time'] = metric['total_time'] / metric['count']
        metric['success_count'] += 1
        
        return duration
    
    def record_failure(self, operation: str, error_message: str) -> None:
        """Record a failed operation"""
        if operation in self.start_times:
            duration = time.time() - self.start_times[operation]
            del self.start_times[operation]
        else:
            duration = 0.0
        
        if operation not in self.metrics:
            self.metrics[operation] = {
                'count': 0,
                'total_time': 0.0,
                'min_time': float('inf'),
                'max_time': 0.0,
                'avg_time': 0.0,
                'success_count': 0,
                'failure_count': 0,
                'last_error': None
            }
        
        metric = self.metrics[operation]
        metric['count'] += 1
        metric['failure_count'] += 1
        metric['total_time'] += duration
        metric['avg_time'] = metric['total_time'] / metric['count']
        metric['last_error'] = error_message
    
    def get_metrics(self) -> Dict[str, Dict[str, float]]:
        """Get all recorded metrics"""
        enriched_metrics = {}
        
        for operation, metric in self.metrics.items():
            enriched_metric = metric.copy()
            
            # Calculate additional derived metrics
            if metric['count'] > 0:
                enriched_metric['success_rate'] = (metric['success_count'] / metric['count']) * 100
                enriched_metric['failure_rate'] = (metric['failure_count'] / metric['count']) * 100
            else:
                enriched_metric['success_rate'] = 0.0
                enriched_metric['failure_rate'] = 0.0
            
            # Performance rating
            if enriched_metric['success_rate'] >= 99:
                enriched_metric['performance_rating'] = 'Excellent'
            elif enriched_metric['success_rate'] >= 95:
                enriched_metric['performance_rating'] = 'Good'
            elif enriched_metric['success_rate'] >= 90:
                enriched_metric['performance_rating'] = 'Fair'
            else:
                enriched_metric['performance_rating'] = 'Poor'
            
            enriched_metrics[operation] = enriched_metric
        
        return enriched_metrics
    
    def reset_metrics(self) -> None:
        """Reset all metrics"""
        self.metrics.clear()
        self.start_times.clear()
    
    def get_top_operations_by_time(self, limit: int = 10) -> List[Tuple[str, float]]:
        """Get operations sorted by total time spent"""
        operations_by_time = [
            (op, metrics['total_time']) 
            for op, metrics in self.metrics.items()
        ]
        return sorted(operations_by_time, key=lambda x: x[1], reverse=True)[:limit]
    
    def get_slowest_operations(self, limit: int = 10) -> List[Tuple[str, float]]:
        """Get operations sorted by average time"""
        operations_by_avg_time = [
            (op, metrics['avg_time']) 
            for op, metrics in self.metrics.items()
        ]
        return sorted(operations_by_avg_time, key=lambda x: x[1], reverse=True)[:limit]


# Global performance monitor instance
performance_monitor = PerformanceMonitor()


class BatchProcessor:
    """Utilities for batch processing operations"""
    
    @staticmethod
    async def process_in_batches(
        items: List[Any],
        batch_size: int,
        processor_func,
        max_concurrent: int = 5,
        delay_between_batches: float = 0.1
    ) -> List[Any]:
        """Process items in batches with concurrency control"""
        
        if batch_size <= 0:
            raise ValueError("Batch size must be positive")
        
        if max_concurrent <= 0:
            raise ValueError("Max concurrent must be positive")
        
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_batch(batch: List[Any]) -> Any:
            async with semaphore:
                return await processor_func(batch)
        
        # Create batches
        batches = [
            items[i:i + batch_size]
            for i in range(0, len(items), batch_size)
        ]
        
        results = []
        for i, batch in enumerate(batches):
            try:
                result = await process_batch(batch)
                results.append(result)
                
                # Add delay between batches (except for the last one)
                if i < len(batches) - 1 and delay_between_batches > 0:
                    await asyncio.sleep(delay_between_batches)
                    
            except Exception as e:
                logger.error(f"Error processing batch {i + 1}/{len(batches)}: {e}")
                results.append(None)
        
        return results
    
    @staticmethod
    def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
        """Split list into chunks of specified size"""
        return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
    
    @staticmethod
    async def process_with_progress(
        items: List[Any],
        processor_func,
        batch_size: int = 100,
        progress_callback: Optional[callable] = None
    ) -> List[Any]:
        """Process items with progress tracking"""
        
        total_items = len(items)
        processed_items = 0
        results = []
        
        batches = BatchProcessor.chunk_list(items, batch_size)
        
        for i, batch in enumerate(batches):
            try:
                batch_result = await processor_func(batch)
                results.extend(batch_result if isinstance(batch_result, list) else [batch_result])
                
                processed_items += len(batch)
                progress_percent = (processed_items / total_items) * 100
                
                if progress_callback:
                    await progress_callback(processed_items, total_items, progress_percent)
                
                logger.debug(f"Processed batch {i + 1}/{len(batches)} - {progress_percent:.1f}% complete")
                
            except Exception as e:
                logger.error(f"Error processing batch {i + 1}: {e}")
                # Continue with next batch rather than failing entirely
                continue
        
        return results


class ErrorAnalyzer:
    """Analyze and categorize errors for better debugging"""
    
    ERROR_CATEGORIES = {
        'network': [
            'connection', 'timeout', 'unreachable', 'dns', 'ssl',
            'certificate', 'proxy', 'gateway'
        ],
        'authentication': [
            'unauthorized', 'forbidden', 'auth', 'permission', 'credential',
            'token', 'key', 'secret'
        ],
        'data': [
            'validation', 'format', 'parse', 'decode', 'encode',
            'schema', 'missing', 'invalid', 'corrupt'
        ],
        'rate_limit': [
            'rate', 'limit', 'throttle', 'quota', 'too many requests'
        ],
        'resource': [
            'memory', 'disk', 'cpu', 'storage', 'capacity', 'resource'
        ],
        'bigquery': [
            'bigquery', 'query', 'dataset', 'table', 'sql', 'syntax'
        ],
        'price_data': [
            'price', 'market', 'coingecko', 'dexscreener', 'api'
        ],
        'business_logic': [
            'scoring', 'matching', 'calculation', 'algorithm'
        ]
    }
    
    @classmethod
    def categorize_error(cls, error_message: str) -> str:
        """Categorize error based on message content"""
        error_lower = error_message.lower()
        
        for category, keywords in cls.ERROR_CATEGORIES.items():
            if any(keyword in error_lower for keyword in keywords):
                return category
        
        return 'unknown'
    
    @classmethod
    def suggest_resolution(cls, error_message: str, error_category: str = None) -> List[str]:
        """Suggest resolution steps based on error category"""
        if not error_category:
            error_category = cls.categorize_error(error_message)
        
        suggestions = {
            'network': [
                "Check internet connectivity",
                "Verify API endpoint URLs",
                "Check firewall and proxy settings",
                "Retry with exponential backoff"
            ],
            'authentication': [
                "Verify API keys and credentials",
                "Check authentication token expiry",
                "Confirm service account permissions",
                "Review access control settings"
            ],
            'data': [
                "Validate input data format",
                "Check required fields are present",
                "Verify data types and ranges",
                "Review data schema requirements"
            ],
            'rate_limit': [
                "Reduce request frequency",
                "Implement request queuing",
                "Use batch operations where possible",
                "Consider upgrading API plan"
            ],
            'resource': [
                "Monitor system resources",
                "Optimize memory usage",
                "Check available disk space",
                "Scale up infrastructure if needed"
            ],
            'bigquery': [
                "Check SQL query syntax",
                "Verify table and dataset names",
                "Review BigQuery quotas",
                "Check dataset permissions"
            ],
            'price_data': [
                "Check price API availability",
                "Verify token addresses and symbols",
                "Review price data source configurations",
                "Consider alternative price sources"
            ],
            'business_logic': [
                "Review algorithm parameters",
                "Check calculation logic",
                "Validate input data quality",
                "Review business rules implementation"
            ],
            'unknown': [
                "Review error logs for patterns",
                "Check recent code changes",
                "Verify system configuration",
                "Contact support if issue persists"
            ]
        }
        
        return suggestions.get(error_category, suggestions['unknown'])
    
    @classmethod
    def analyze_error_frequency(cls, error_messages: List[str]) -> Dict[str, Any]:
        """Analyze frequency of different error types"""
        if not error_messages:
            return {
                'total_errors': 0,
                'category_breakdown': {},
                'most_common_category': None,
                'analysis_timestamp': datetime.utcnow().isoformat()
            }
        
        category_counts = {}
        total_errors = len(error_messages)
        
        for error in error_messages:
            category = cls.categorize_error(error)
            category_counts[category] = category_counts.get(category, 0) + 1
        
        # Calculate percentages and create analysis
        analysis = {
            'total_errors': total_errors,
            'category_breakdown': {},
            'most_common_category': None,
            'analysis_timestamp': datetime.utcnow().isoformat()
        }
        
        for category, count in category_counts.items():
            percentage = (count / total_errors) * 100
            analysis['category_breakdown'][category] = {
                'count': count,
                'percentage': round(percentage, 2)
            }
        
        # Find most common category
        if category_counts:
            analysis['most_common_category'] = max(category_counts.items(), key=lambda x: x[1])[0]
        
        return analysis
import aiohttp
import asyncio
import logging
from typing import Dict, Optional, List
import time
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class PriceSource(Enum):
    COINGECKO = "coingecko"
    DEXSCREENER = "dexscreener"

@dataclass
class PriceData:
    usd: float
    source: str = ""
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()

class PriceFetcher:
    def __init__(self, max_concurrent_requests: int = 10, request_timeout: int = 15):
        self.session: Optional[aiohttp.ClientSession] = None
        self.max_concurrent_requests = max_concurrent_requests
        self.request_timeout = request_timeout
        
        # Rate limiting
        self.rate_limits = {
            PriceSource.COINGECKO: {'last_requests': [], 'delay': 1.2},
            PriceSource.DEXSCREENER: {'last_requests': [], 'delay': 0.2}
        }
        
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        # Simple cache
        self.cache = {}
        
        # Headers
        self.headers = {
            'User-Agent': 'WalletScoring/1.0',
            'Accept': 'application/json'
        }
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session"""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=self.request_timeout)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers=self.headers
            )
        return self.session
    
    async def _rate_limit(self, source: PriceSource):
        """Simple rate limiting"""
        if source not in self.rate_limits:
            return
        
        rate_info = self.rate_limits[source]
        current_time = time.time()
        
        # Clean old requests (older than 1 minute)
        rate_info['last_requests'] = [
            req_time for req_time in rate_info['last_requests']
            if current_time - req_time < 60
        ]
        
        # Add minimum delay between requests
        if rate_info['last_requests']:
            time_since_last = current_time - rate_info['last_requests'][-1]
            if time_since_last < rate_info['delay']:
                await asyncio.sleep(rate_info['delay'] - time_since_last)
        
        rate_info['last_requests'].append(current_time)
    
    async def get_token_price(self, address: str, symbol: str) -> Optional[Dict]:
        """Get token price from multiple sources with fallback"""
        address = address.lower()
        symbol = symbol.upper()
        
        # Check cache first
        cache_key = f"{address}:{symbol}"
        if cache_key in self.cache:
            cached_data, timestamp = self.cache[cache_key]
            if (datetime.utcnow() - timestamp).seconds < 120:  # 2 min cache
                return cached_data
        
        async with self.semaphore:
            # Try DexScreener first
            price_data = await self._try_dexscreener(address, symbol)
            
            # Fallback to CoinGecko
            if not price_data:
                price_data = await self._try_coingecko(address, symbol)
            
            if price_data:
                self.cache[cache_key] = (price_data, datetime.utcnow())
                logger.debug(f"Fetched price for {symbol}: ${price_data['usd']:.6f}")
            else:
                logger.warning(f"Could not fetch price for {symbol}")
            
            return price_data
    
    async def _try_dexscreener(self, address: str, symbol: str) -> Optional[Dict]:
        """Try DexScreener API"""
        try:
            session = await self._get_session()
            url = f"https://api.dexscreener.com/latest/dex/tokens/{address}"
            
            await self._rate_limit(PriceSource.DEXSCREENER)
            
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get('pairs') and len(data['pairs']) > 0:
                        pair = data['pairs'][0]
                        return {
                            'usd': float(pair.get('priceUsd', 0)),
                            'source': PriceSource.DEXSCREENER.value
                        }
        except Exception as e:
            logger.debug(f"DexScreener error for {address}: {e}")
        
        return None
    
    async def _try_coingecko(self, address: str, symbol: str) -> Optional[Dict]:
        """Try CoinGecko API"""
        try:
            session = await self._get_session()
            url = "https://api.coingecko.com/api/v3/simple/token_price/ethereum"
            params = {
                'contract_addresses': address,
                'vs_currencies': 'usd'
            }
            
            await self._rate_limit(PriceSource.COINGECKO)
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    token_data = data.get(address.lower())
                    
                    if token_data:
                        return {
                            'usd': token_data.get('usd', 0),
                            'source': PriceSource.COINGECKO.value
                        }
        except Exception as e:
            logger.debug(f"CoinGecko error for {address}: {e}")
        
        return None
    
    async def close(self):
        """Close the aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()
        self.cache.clear()
        logger.info("PriceFetcher closed")
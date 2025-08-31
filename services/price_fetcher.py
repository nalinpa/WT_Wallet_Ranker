import aiohttp
import asyncio
import logging
from typing import Dict, Optional, List, Tuple, Any
import time
from datetime import datetime, timedelta
import json
import hashlib
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class PriceSource(Enum):
    COINGECKO = "coingecko"
    DEXSCREENER = "dexscreener"
    COINMARKETCAP = "coinmarketcap"
    UNISWAP = "uniswap"
    ONEINCH = "1inch"
    MORALIS = "moralis"

@dataclass
class PriceData:
    """Standard price data structure"""
    usd: float
    eth: float = 0.0
    volume_24h: float = 0.0
    market_cap: float = 0.0
    price_change_24h: float = 0.0
    liquidity_usd: float = 0.0
    source: str = ""
    timestamp: datetime = None
    confidence: float = 1.0  # 0-1 confidence score
    
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
            PriceSource.COINGECKO: {'requests_per_minute': 50, 'last_requests': [], 'delay': 1.2},
            PriceSource.DEXSCREENER: {'requests_per_minute': 300, 'last_requests': [], 'delay': 0.2},
            PriceSource.COINMARKETCAP: {'requests_per_minute': 333, 'last_requests': [], 'delay': 0.18},
            PriceSource.UNISWAP: {'requests_per_minute': 100, 'last_requests': [], 'delay': 0.6},
            PriceSource.ONEINCH: {'requests_per_minute': 60, 'last_requests': [], 'delay': 1.0},
            PriceSource.MORALIS: {'requests_per_minute': 25, 'last_requests': [], 'delay': 2.4}
        }
        
        # Semaphore for controlling concurrent requests
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        # Cache for reducing redundant requests
        self.cache = {}
        self.cache_duration = timedelta(minutes=2)  # Cache for 2 minutes
        
        # API endpoints
        self.endpoints = {
            PriceSource.COINGECKO: {
                'token_price': 'https://api.coingecko.com/api/v3/simple/token_price/ethereum',
                'simple_price': 'https://api.coingecko.com/api/v3/simple/price',
                'token_info': 'https://api.coingecko.com/api/v3/coins/ethereum/contract/{address}'
            },
            PriceSource.DEXSCREENER: {
                'token': 'https://api.dexscreener.com/latest/dex/tokens/{address}',
                'search': 'https://api.dexscreener.com/latest/dex/search/?q={query}'
            },
            PriceSource.COINMARKETCAP: {
                'token_info': 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/info',
                'quotes': 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'
            },
            PriceSource.UNISWAP: {
                'subgraph': 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'
            }
        }
        
        # Headers for different APIs
        self.headers = {
            'User-Agent': 'WalletScoring/1.0 (Professional Crypto Analytics)',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate'
        }
        
        # Statistics tracking
        self.stats = {
            'requests_made': 0,
            'cache_hits': 0,
            'successful_fetches': 0,
            'failed_fetches': 0,
            'rate_limit_hits': 0,
            'average_response_time': 0,
            'sources_used': {source: 0 for source in PriceSource}
        }
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session"""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=self.request_timeout)
            connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers=self.headers
            )
        return self.session
    
    async def _rate_limit(self, source: PriceSource):
        """Advanced rate limiting with sliding window"""
        if source not in self.rate_limits:
            return
        
        rate_info = self.rate_limits[source]
        current_time = time.time()
        
        # Clean old requests (older than 1 minute)
        rate_info['last_requests'] = [
            req_time for req_time in rate_info['last_requests']
            if current_time - req_time < 60
        ]
        
        # Check if we need to wait
        if len(rate_info['last_requests']) >= rate_info['requests_per_minute']:
            sleep_time = 60 - (current_time - rate_info['last_requests'][0])
            if sleep_time > 0:
                logger.debug(f"Rate limiting {source.value}: sleeping {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)
                self.stats['rate_limit_hits'] += 1
        
        # Add minimum delay between requests
        if rate_info['last_requests']:
            time_since_last = current_time - rate_info['last_requests'][-1]
            if time_since_last < rate_info['delay']:
                await asyncio.sleep(rate_info['delay'] - time_since_last)
        
        # Record this request
        rate_info['last_requests'].append(current_time)
    
    def _get_cache_key(self, address: str, symbol: str, source: str) -> str:
        """Generate cache key for price data"""
        return hashlib.md5(f"{address}:{symbol}:{source}".encode()).hexdigest()
    
    def _get_cached_price(self, cache_key: str) -> Optional[PriceData]:
        """Get cached price data if still valid"""
        if cache_key in self.cache:
            cached_data, timestamp = self.cache[cache_key]
            if datetime.utcnow() - timestamp < self.cache_duration:
                self.stats['cache_hits'] += 1
                return cached_data
            else:
                # Remove expired cache entry
                del self.cache[cache_key]
        return None
    
    def _cache_price(self, cache_key: str, price_data: PriceData):
        """Cache price data"""
        self.cache[cache_key] = (price_data, datetime.utcnow())
        
        # Cleanup old cache entries periodically
        if len(self.cache) > 1000:  # Max 1000 cached entries
            current_time = datetime.utcnow()
            expired_keys = [
                key for key, (_, timestamp) in self.cache.items()
                if current_time - timestamp > self.cache_duration
            ]
            for key in expired_keys:
                del self.cache[key]
    
    async def get_token_price(self, address: str, symbol: str) -> Optional[PriceData]:
        """Get token price from multiple sources with fallback"""
        address = address.lower()
        symbol = symbol.upper()
        
        # Check cache first
        cache_key = self._get_cache_key(address, symbol, "combined")
        cached_price = self._get_cached_price(cache_key)
        if cached_price:
            return cached_price
        
        async with self.semaphore:
            start_time = time.time()
            
            # Try multiple sources in order of preference
            sources_to_try = [
                (PriceSource.DEXSCREENER, self._try_dexscreener),
                (PriceSource.COINGECKO, self._try_coingecko),
                (PriceSource.UNISWAP, self._try_uniswap),
                (PriceSource.ONEINCH, self._try_1inch),
            ]
            
            best_price = None
            all_prices = []
            
            for source, fetch_func in sources_to_try:
                try:
                    await self._rate_limit(source)
                    price_data = await fetch_func(address, symbol)
                    
                    if price_data and price_data.usd > 0:
                        all_prices.append(price_data)
                        self.stats['sources_used'][source] += 1
                        
                        # DexScreener is preferred for new/small tokens
                        if source == PriceSource.DEXSCREENER and price_data.confidence > 0.7:
                            best_price = price_data
                            break
                        
                        # CoinGecko is preferred for established tokens
                        elif source == PriceSource.COINGECKO and price_data.confidence > 0.8:
                            best_price = price_data
                            break
                        
                        # Keep the first valid price as fallback
                        if best_price is None:
                            best_price = price_data
                            
                except Exception as e:
                    logger.debug(f"{source.value} failed for {symbol}: {e}")
                    continue
            
            # If we have multiple prices, create a consensus price
            if len(all_prices) > 1:
                best_price = self._create_consensus_price(all_prices, symbol)
            
            # Update statistics
            response_time = time.time() - start_time
            self.stats['requests_made'] += 1
            self.stats['average_response_time'] = (
                (self.stats['average_response_time'] * (self.stats['requests_made'] - 1) + response_time) /
                self.stats['requests_made']
            )
            
            if best_price:
                self.stats['successful_fetches'] += 1
                self._cache_price(cache_key, best_price)
                logger.debug(f"Fetched price for {symbol}: ${best_price.usd:.6f} from {best_price.source}")
            else:
                self.stats['failed_fetches'] += 1
                logger.warning(f"Could not fetch price for {symbol} ({address})")
            
            return best_price
    
    def _create_consensus_price(self, prices: List[PriceData], symbol: str) -> PriceData:
        """Create consensus price from multiple sources"""
        if len(prices) == 1:
            return prices[0]
        
        # Weight prices by confidence and recency
        weighted_usd = 0
        weighted_eth = 0
        total_weight = 0
        
        combined_volume = 0
        combined_market_cap = 0
        combined_liquidity = 0
        sources_used = []
        
        for price in prices:
            # Higher weight for higher confidence and more recent data
            age_minutes = (datetime.utcnow() - price.timestamp).total_seconds() / 60
            age_penalty = max(0.5, 1 - (age_minutes / 30))  # Penalty for data older than 30min
            weight = price.confidence * age_penalty
            
            weighted_usd += price.usd * weight
            weighted_eth += price.eth * weight
            total_weight += weight
            
            combined_volume += price.volume_24h
            combined_market_cap = max(combined_market_cap, price.market_cap)
            combined_liquidity += price.liquidity_usd
            sources_used.append(price.source)
        
        if total_weight == 0:
            return prices[0]  # Fallback to first price
        
        # Calculate consensus price
        consensus_price = PriceData(
            usd=weighted_usd / total_weight,
            eth=weighted_eth / total_weight,
            volume_24h=combined_volume / len(prices),  # Average volume
            market_cap=combined_market_cap,
            liquidity_usd=combined_liquidity,
            source=f"consensus({'+'.join(set(sources_used))})",
            confidence=min(1.0, total_weight / len(prices))  # Confidence based on average weight
        )
        
        logger.debug(f"Created consensus price for {symbol} from {len(prices)} sources")
        return consensus_price
    
    async def _try_dexscreener(self, address: str, symbol: str) -> Optional[PriceData]:
        """Try DexScreener API - best for new/DeFi tokens"""
        try:
            session = await self._get_session()
            url = self.endpoints[PriceSource.DEXSCREENER]['token'].format(address=address)
            
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get('pairs') and len(data['pairs']) > 0:
                        # Sort pairs by liquidity and volume, prefer larger ones
                        pairs = sorted(data['pairs'], 
                                     key=lambda p: (p.get('liquidity', {}).get('usd', 0) + 
                                                   p.get('volume', {}).get('h24', 0)), 
                                     reverse=True)
                        
                        pair = pairs[0]  # Best pair by liquidity + volume
                        
                        # Calculate confidence based on liquidity and volume
                        liquidity = float(pair.get('liquidity', {}).get('usd', 0))
                        volume_24h = float(pair.get('volume', {}).get('h24', 0))
                        confidence = min(1.0, (liquidity + volume_24h) / 100000)  # Scale to 0-1
                        
                        return PriceData(
                            usd=float(pair.get('priceUsd', 0)),
                            eth=float(pair.get('priceNative', 0)),
                            volume_24h=volume_24h,
                            market_cap=float(pair.get('marketCap', 0)),
                            price_change_24h=float(pair.get('priceChange', {}).get('h24', 0)),
                            liquidity_usd=liquidity,
                            source=PriceSource.DEXSCREENER.value,
                            confidence=confidence
                        )
        except Exception as e:
            logger.debug(f"DexScreener error for {address}: {e}")
        
        return None
    
    async def _try_coingecko(self, address: str, symbol: str) -> Optional[PriceData]:
        """Try CoinGecko API - best for established tokens"""
        try:
            session = await self._get_session()
            
            # Try token price endpoint first
            url = self.endpoints[PriceSource.COINGECKO]['token_price']
            params = {
                'contract_addresses': address,
                'vs_currencies': 'usd,eth',
                'include_24hr_vol': 'true',
                'include_24hr_change': 'true',
                'include_market_cap': 'true'
            }
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    token_data = data.get(address.lower())
                    
                    if token_data:
                        # High confidence for CoinGecko data
                        confidence = 0.9 if token_data.get('usd_market_cap', 0) > 1000000 else 0.7
                        
                        return PriceData(
                            usd=token_data.get('usd', 0),
                            eth=token_data.get('eth', 0),
                            volume_24h=token_data.get('usd_24h_vol', 0),
                            market_cap=token_data.get('usd_market_cap', 0),
                            price_change_24h=token_data.get('usd_24h_change', 0),
                            source=PriceSource.COINGECKO.value,
                            confidence=confidence
                        )
            
            # If token address fails, try symbol lookup
            if symbol:
                url = self.endpoints[PriceSource.COINGECKO]['simple_price']
                params = {
                    'ids': symbol.lower(),
                    'vs_currencies': 'usd,eth',
                    'include_24hr_vol': 'true',
                    'include_24hr_change': 'true',
                    'include_market_cap': 'true'
                }
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if symbol.lower() in data:
                            token_data = data[symbol.lower()]
                            
                            return PriceData(
                                usd=token_data.get('usd', 0),
                                eth=token_data.get('eth', 0),
                                volume_24h=token_data.get('usd_24h_vol', 0),
                                market_cap=token_data.get('usd_market_cap', 0),
                                price_change_24h=token_data.get('usd_24h_change', 0),
                                source=f"{PriceSource.COINGECKO.value}_symbol",
                                confidence=0.8
                            )
        
        except Exception as e:
            logger.debug(f"CoinGecko error for {address}: {e}")
        
        return None
    
    async def _try_uniswap(self, address: str, symbol: str) -> Optional[PriceData]:
        """Try Uniswap subgraph - good for DEX prices"""
        try:
            session = await self._get_session()
            
            query = """
            {
              token(id: "%s") {
                decimals
                derivedETH
                totalSupply
                volume
                volumeUSD
                txCount
                totalLiquidity
                totalValueLockedUSD
              }
              bundle(id: "1") {
                ethPrice
              }
            }
            """ % address.lower()
            
            async with session.post(
                self.endpoints[PriceSource.UNISWAP]['subgraph'],
                json={'query': query}
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    data = result.get('data', {})
                    token = data.get('token')
                    bundle = data.get('bundle')
                    
                    if token and bundle:
                        eth_price = float(bundle.get('ethPrice', 0))
                        token_eth_price = float(token.get('derivedETH', 0))
                        token_usd_price = token_eth_price * eth_price
                        
                        if token_usd_price > 0:
                            # Calculate confidence based on liquidity and volume
                            tvl = float(token.get('totalValueLockedUSD', 0))
                            volume = float(token.get('volumeUSD', 0))
                            confidence = min(1.0, (tvl + volume) / 500000)
                            
                            return PriceData(
                                usd=token_usd_price,
                                eth=token_eth_price,
                                volume_24h=volume,
                                liquidity_usd=tvl,
                                source=PriceSource.UNISWAP.value,
                                confidence=confidence
                            )
        
        except Exception as e:
            logger.debug(f"Uniswap error for {address}: {e}")
        
        return None
    
    async def _try_1inch(self, address: str, symbol: str) -> Optional[PriceData]:
        """Try 1inch API for price data"""
        try:
            session = await self._get_session()
            
            # 1inch spot price API
            url = f"https://api.1inch.io/v5.0/1/quote"
            params = {
                'fromTokenAddress': address,
                'toTokenAddress': '0xA0b86991c6218a36C1d19D4a2e9Eb0cE3606eB48',  # USDC
                'amount': str(10**18)  # 1 token worth of wei
            }
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    to_token_amount = float(data.get('toTokenAmount', 0))
                    if to_token_amount > 0:
                        # Convert to USD (assuming USDC ~= $1)
                        usd_price = to_token_amount / (10**6)  # USDC has 6 decimals
                        
                        return PriceData(
                            usd=usd_price,
                            eth=0,  # Would need additional call to get ETH price
                            source=PriceSource.ONEINCH.value,
                            confidence=0.6  # Lower confidence as it's just a quote
                        )
        
        except Exception as e:
            logger.debug(f"1inch error for {address}: {e}")
        
        return None
    
    async def batch_get_prices(self, tokens: List[Tuple[str, str]]) -> Dict[str, Optional[PriceData]]:
        """Get prices for multiple tokens efficiently"""
        tasks = []
        for address, symbol in tokens:
            task = self.get_token_price(address, symbol)
            tasks.append((f"{address}:{symbol}", task))
        
        results = {}
        completed_tasks = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)
        
        for (token_key, _), result in zip(tasks, completed_tasks):
            if isinstance(result, Exception):
                logger.error(f"Error fetching price for {token_key}: {result}")
                results[token_key] = None
            else:
                results[token_key] = result
        
        return results
    
    async def get_historical_price(self, address: str, symbol: str, timestamp: datetime) -> Optional[PriceData]:
        """Get historical price data (limited support)"""
        # Only CoinGecko supports historical data well
        try:
            session = await self._get_session()
            
            # Format date for CoinGecko API
            date_str = timestamp.strftime("%d-%m-%Y")
            url = f"https://api.coingecko.com/api/v3/coins/ethereum/contract/{address}/market_chart/range"
            params = {
                'vs_currency': 'usd',
                'from': int(timestamp.timestamp()),
                'to': int(timestamp.timestamp()) + 86400  # +1 day
            }
            
            await self._rate_limit(PriceSource.COINGECKO)
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    prices = data.get('prices', [])
                    
                    if prices:
                        # Get closest price to requested timestamp
                        closest_price = min(prices, key=lambda p: abs(p[0] - timestamp.timestamp() * 1000))
                        
                        return PriceData(
                            usd=closest_price[1],
                            source=f"{PriceSource.COINGECKO.value}_historical",
                            timestamp=datetime.fromtimestamp(closest_price[0] / 1000),
                            confidence=0.8
                        )
        
        except Exception as e:
            logger.error(f"Historical price error for {address} at {timestamp}: {e}")
        
        return None
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get fetcher performance statistics"""
        return {
            'requests_made': self.stats['requests_made'],
            'successful_fetches': self.stats['successful_fetches'],
            'failed_fetches': self.stats['failed_fetches'],
            'cache_hits': self.stats['cache_hits'],
            'rate_limit_hits': self.stats['rate_limit_hits'],
            'success_rate_pct': (
                (self.stats['successful_fetches'] / max(1, self.stats['requests_made'])) * 100
            ),
            'cache_hit_rate_pct': (
                (self.stats['cache_hits'] / max(1, self.stats['requests_made'] + self.stats['cache_hits'])) * 100
            ),
            'average_response_time_ms': self.stats['average_response_time'] * 1000,
            'sources_used': dict(self.stats['sources_used']),
            'cache_size': len(self.cache),
            'active_rate_limits': {
                source.value: len(limits['last_requests']) 
                for source, limits in self.rate_limits.items()
            }
        }
    
    def clear_cache(self):
        """Clear the price cache"""
        old_size = len(self.cache)
        self.cache.clear()
        logger.info(f"Cleared price cache ({old_size} entries)")
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on all price sources"""
        health_results = {}
        
        # Test token (USDC) for health checks
        test_address = "0xA0b86991c6218a36C1d19D4a2e9Eb0cE3606eB48"
        test_symbol = "USDC"
        
        sources_to_test = [
            (PriceSource.DEXSCREENER, self._try_dexscreener),
            (PriceSource.COINGECKO, self._try_coingecko),
            (PriceSource.UNISWAP, self._try_uniswap),
        ]
        
        for source, fetch_func in sources_to_test:
            start_time = time.time()
            try:
                await self._rate_limit(source)
                result = await fetch_func(test_address, test_symbol)
                response_time = time.time() - start_time
                
                health_results[source.value] = {
                    'status': 'healthy' if result and result.usd > 0 else 'degraded',
                    'response_time_ms': response_time * 1000,
                    'price_returned': result.usd if result else 0,
                    'last_tested': datetime.utcnow().isoformat()
                }
                
            except Exception as e:
                health_results[source.value] = {
                    'status': 'unhealthy',
                    'error': str(e),
                    'response_time_ms': (time.time() - start_time) * 1000,
                    'last_tested': datetime.utcnow().isoformat()
                }
        
        # Overall health status
        healthy_sources = sum(1 for h in health_results.values() if h['status'] == 'healthy')
        total_sources = len(health_results)
        
        health_results['overall'] = {
            'status': (
                'healthy' if healthy_sources == total_sources else
                'degraded' if healthy_sources > 0 else
                'unhealthy'
            ),
            'healthy_sources': healthy_sources,
            'total_sources': total_sources,
            'health_percentage': (healthy_sources / total_sources * 100) if total_sources > 0 else 0
        }
        
        return health_results
    
    async def close(self):
        """Close the aiohttp session and cleanup"""
        if self.session and not self.session.closed:
            await self.session.close()
            
        # Clear cache to free memory
        self.cache.clear()
        
        logger.info(f"PriceFetcher closed. Final stats: {self.get_statistics()}")

# Utility functions for price validation and analysis
def validate_price_data(price_data: PriceData, symbol: str) -> bool:
    """Validate price data for reasonableness"""
    if not price_data or price_data.usd <= 0:
        return False
    
    # Basic sanity checks
    if price_data.usd > 1000000:  # Very expensive token
        logger.warning(f"Unusually high price for {symbol}: ${price_data.usd}")
    
    if price_data.confidence < 0.3:  # Low confidence
        logger.warning(f"Low confidence price for {symbol}: {price_data.confidence}")
        return False
    
    return True

def calculate_price_deviation(prices: List[PriceData]) -> float:
    """Calculate price deviation across multiple sources"""
    if len(prices) < 2:
        return 0.0
    
    usd_prices = [p.usd for p in prices if p.usd > 0]
    if len(usd_prices) < 2:
        return 0.0
    
    mean_price = sum(usd_prices) / len(usd_prices)
    variance = sum((p - mean_price) ** 2 for p in usd_prices) / len(usd_prices)
    std_dev = variance ** 0.5
    
    return (std_dev / mean_price) * 100 if mean_price > 0 else 0.0
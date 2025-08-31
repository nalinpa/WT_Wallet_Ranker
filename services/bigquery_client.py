from google.cloud import bigquery
from google.cloud.exceptions import NotFound, BadRequest
import logging
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta
import asyncio
from concurrent.futures import ThreadPoolExecutor
import json

logger = logging.getLogger(__name__)

class BigQueryClient:
    def __init__(self, project_id: str = None, dataset_id: str = "crypto_analysis"):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id or self.client.project
        self.dataset_id = dataset_id
        self.dataset_ref = f"{self.project_id}.{self.dataset_id}"
        
        # Thread pool for async operations
        self.executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="bq")
        
        # Initialize dataset and tables
        self._create_dataset_if_not_exists()
        self._create_all_tables()
        
        logger.info(f"BigQuery client initialized for project: {self.project_id}, dataset: {self.dataset_id}")
    
    def _create_dataset_if_not_exists(self):
        """Create the dataset if it doesn't exist"""
        try:
            self.client.get_dataset(self.dataset_ref)
            logger.info(f"Dataset {self.dataset_ref} already exists")
        except NotFound:
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = "US"
            dataset.description = "Crypto wallet scoring and analysis data"
            
            # Set default table expiration to 90 days for cost management
            dataset.default_table_expiration_ms = 90 * 24 * 60 * 60 * 1000
            
            self.client.create_dataset(dataset)
            logger.info(f"Created dataset {self.dataset_ref}")
    
    def _create_all_tables(self):
        """Create all necessary tables"""
        
        # Define all table schemas
        table_schemas = {
            "transactions": [
                bigquery.SchemaField("transaction_hash", "STRING", mode="REQUIRED", description="Unique blockchain transaction hash"),
                bigquery.SchemaField("block_number", "INTEGER", description="Blockchain block number"),
                bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED", description="Transaction timestamp"),
                bigquery.SchemaField("wallet_address", "STRING", mode="REQUIRED", description="Wallet address involved in transaction"),
                bigquery.SchemaField("token_address", "STRING", mode="REQUIRED", description="Token contract address"),
                bigquery.SchemaField("token_symbol", "STRING", mode="REQUIRED", description="Token symbol (e.g., ETH, BTC)"),
                bigquery.SchemaField("token_amount", "FLOAT", mode="REQUIRED", description="Amount of tokens transferred"),
                bigquery.SchemaField("transfer_type", "STRING", mode="REQUIRED", description="Type of transfer: buy or sell"),
                bigquery.SchemaField("cost_in_eth", "FLOAT", description="Gas cost paid in ETH"),
                bigquery.SchemaField("platform", "STRING", description="Trading platform used"),
                bigquery.SchemaField("network", "STRING", description="Blockchain network"),
                bigquery.SchemaField("wallet_sophistication_score", "INTEGER", mode="REQUIRED", description="Current wallet sophistication score"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", description="Record creation timestamp")
            ],
            
            "token_prices": [
                bigquery.SchemaField("token_address", "STRING", mode="REQUIRED", description="Token contract address"),
                bigquery.SchemaField("token_symbol", "STRING", mode="REQUIRED", description="Token symbol"),
                bigquery.SchemaField("price_usd", "FLOAT", mode="REQUIRED", description="Price in USD"),
                bigquery.SchemaField("price_eth", "FLOAT", description="Price in ETH"),
                bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED", description="Price timestamp"),
                bigquery.SchemaField("volume_24h", "FLOAT", description="24-hour trading volume in USD"),
                bigquery.SchemaField("market_cap", "FLOAT", description="Market capitalization in USD"),
                bigquery.SchemaField("source", "STRING", description="Price data source (coingecko, dexscreener, etc.)"),
                bigquery.SchemaField("price_change_24h", "FLOAT", description="24-hour price change percentage"),
                bigquery.SchemaField("liquidity_usd", "FLOAT", description="Available liquidity in USD")
            ],
            
            "matched_trades": [
                bigquery.SchemaField("wallet_address", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("token_address", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("token_symbol", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("buy_hash", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("sell_hash", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("buy_time", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("sell_time", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("buy_amount", "FLOAT", mode="REQUIRED"),
                bigquery.SchemaField("sell_amount", "FLOAT", mode="REQUIRED"),
                bigquery.SchemaField("buy_gas", "FLOAT"),
                bigquery.SchemaField("sell_gas", "FLOAT"),
                bigquery.SchemaField("buy_price", "FLOAT"),
                bigquery.SchemaField("sell_price", "FLOAT"),
                bigquery.SchemaField("hold_hours", "FLOAT"),
                bigquery.SchemaField("price_change_percent", "FLOAT"),
                bigquery.SchemaField("profit_usd", "FLOAT"),
                bigquery.SchemaField("trade_volume_usd", "FLOAT"),
                bigquery.SchemaField("trade_type", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED")
            ],
            
            "score_adjustments": [
                bigquery.SchemaField("wallet_address", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("buy_hash", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("sell_hash", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("token_symbol", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("price_change_percent", "FLOAT"),
                bigquery.SchemaField("profit_usd", "FLOAT"),
                bigquery.SchemaField("hold_hours", "FLOAT"),
                bigquery.SchemaField("trade_type", "STRING"),
                bigquery.SchemaField("trade_volume_usd", "FLOAT"),
                bigquery.SchemaField("buy_gas", "FLOAT"),
                bigquery.SchemaField("sell_gas", "FLOAT"),
                bigquery.SchemaField("buy_amount", "FLOAT"),
                bigquery.SchemaField("buy_price", "FLOAT"),
                bigquery.SchemaField("score_delta", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("calculated_at", "TIMESTAMP", mode="REQUIRED")
            ],
            
            "wallet_score_updates": [
                bigquery.SchemaField("wallet_address", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("trades_analyzed", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("total_score_change", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("avg_score_per_trade", "FLOAT"),
                bigquery.SchemaField("avg_profit_pct", "FLOAT"),
                bigquery.SchemaField("winning_trades", "INTEGER"),
                bigquery.SchemaField("losing_trades", "INTEGER"),
                bigquery.SchemaField("win_rate", "FLOAT"),
                bigquery.SchemaField("current_score", "INTEGER"),
                bigquery.SchemaField("suggested_new_score", "INTEGER"),
                bigquery.SchemaField("total_volume_usd", "FLOAT"),
                bigquery.SchemaField("avg_trade_size_usd", "FLOAT"),
                bigquery.SchemaField("update_calculated_at", "TIMESTAMP", mode="REQUIRED")
            ],
            
            "score_update_recommendations": [
                bigquery.SchemaField("wallet_address", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("current_score", "INTEGER"),
                bigquery.SchemaField("suggested_new_score", "INTEGER"),
                bigquery.SchemaField("total_score_change", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("trades_analyzed", "INTEGER"),
                bigquery.SchemaField("win_rate", "FLOAT"),
                bigquery.SchemaField("avg_profit_pct", "FLOAT"),
                bigquery.SchemaField("winning_trades", "INTEGER"),
                bigquery.SchemaField("losing_trades", "INTEGER"),
                bigquery.SchemaField("total_volume_usd", "FLOAT"),
                bigquery.SchemaField("avg_trade_size_usd", "FLOAT"),
                bigquery.SchemaField("recommendation_message", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("priority", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("update_calculated_at", "TIMESTAMP", mode="REQUIRED")
            ],
            
            "wallet_features": [
                bigquery.SchemaField("wallet_address", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("sophistication_score", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("total_transactions", "INTEGER"),
                bigquery.SchemaField("unique_tokens", "INTEGER"),
                bigquery.SchemaField("avg_hold_duration_hours", "FLOAT"),
                bigquery.SchemaField("total_volume_eth", "FLOAT"),
                bigquery.SchemaField("total_volume_usd", "FLOAT"),
                bigquery.SchemaField("profitable_trades_ratio", "FLOAT"),
                bigquery.SchemaField("gas_efficiency_score", "FLOAT"),
                bigquery.SchemaField("diversification_index", "FLOAT"),
                bigquery.SchemaField("last_activity", "TIMESTAMP"),
                bigquery.SchemaField("wallet_tier", "STRING"),
                bigquery.SchemaField("avg_trade_size_usd", "FLOAT"),
                bigquery.SchemaField("max_trade_size_usd", "FLOAT"),
                bigquery.SchemaField("activity_frequency_score", "FLOAT"),
                bigquery.SchemaField("feature_update_time", "TIMESTAMP", mode="REQUIRED")
            ],
            
            "token_features": [
                bigquery.SchemaField("token_address", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("token_symbol", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("smart_money_ratio", "FLOAT"),
                bigquery.SchemaField("elite_wallet_holders", "INTEGER"),
                bigquery.SchemaField("avg_holder_sophistication", "FLOAT"),
                bigquery.SchemaField("volume_trend_7d", "FLOAT"),
                bigquery.SchemaField("accumulation_score", "FLOAT"),
                bigquery.SchemaField("holder_stability_score", "FLOAT"),
                bigquery.SchemaField("price_momentum", "FLOAT"),
                bigquery.SchemaField("total_holders", "INTEGER"),
                bigquery.SchemaField("avg_hold_duration_hours", "FLOAT"),
                bigquery.SchemaField("whale_concentration_ratio", "FLOAT"),
                bigquery.SchemaField("feature_update_time", "TIMESTAMP", mode="REQUIRED")
            ]
        }
        
        # Create each table with appropriate partitioning and clustering
        for table_name, schema in table_schemas.items():
            self._create_table_with_schema(table_name, schema)
    
    def _create_table_with_schema(self, table_name: str, schema: List[bigquery.SchemaField]):
        """Create a table with the given schema"""
        table_ref = f"{self.dataset_ref}.{table_name}"
        
        try:
            self.client.get_table(table_ref)
            logger.info(f"Table {table_ref} already exists")
            return
        except NotFound:
            pass
        
        table = bigquery.Table(table_ref, schema=schema)
        
        # Add partitioning and clustering based on table type
        if table_name in ["transactions", "token_prices"]:
            # Time partitioning for time-series data
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="timestamp"
            )
            if table_name == "transactions":
                table.clustering_fields = ["wallet_address", "token_address", "transfer_type"]
            else:  # token_prices
                table.clustering_fields = ["token_address", "source"]
        
        elif table_name == "matched_trades":
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="buy_time"
            )
            table.clustering_fields = ["wallet_address", "token_address"]
        
        elif table_name in ["wallet_features", "token_features"]:
            # Partition by feature_update_time
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="feature_update_time"
            )
            if table_name == "wallet_features":
                table.clustering_fields = ["wallet_tier", "sophistication_score"]
            else:  # token_features
                table.clustering_fields = ["token_symbol", "smart_money_ratio"]
        
        elif table_name in ["score_adjustments", "wallet_score_updates", "score_update_recommendations"]:
            # Use calculated_at or update_calculated_at for partitioning
            time_field = "calculated_at" if table_name == "score_adjustments" else "update_calculated_at"
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=time_field
            )
            table.clustering_fields = ["wallet_address"]
        
        # Add table description
        table.description = f"Crypto wallet scoring table: {table_name}"
        
        try:
            self.client.create_table(table)
            logger.info(f"Created table {table_ref} with partitioning and clustering")
        except Exception as e:
            logger.error(f"Failed to create table {table_ref}: {e}")
            raise
    
    # Async wrapper methods
    async def get_active_tokens(self, hours_back: int = 24) -> List[Dict[str, str]]:
        """Get tokens that have been traded recently"""
        query = f"""
        SELECT DISTINCT 
          token_address as address,
          token_symbol as symbol,
          COUNT(*) as transaction_count,
          COUNT(DISTINCT wallet_address) as unique_wallets,
          SUM(CASE WHEN transfer_type = 'buy' THEN token_amount ELSE 0 END) as total_buy_volume,
          SUM(CASE WHEN transfer_type = 'sell' THEN token_amount ELSE 0 END) as total_sell_volume
        FROM `{self.dataset_ref}.transactions`
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
          AND token_address != '0x0000000000000000000000000000000000000000'
          AND token_address IS NOT NULL
          AND token_symbol IS NOT NULL
          AND token_address != ''
          AND token_symbol != ''
        GROUP BY token_address, token_symbol
        HAVING transaction_count >= 2  -- Only tokens with multiple transactions
        ORDER BY transaction_count DESC, unique_wallets DESC
        LIMIT 500
        """
        
        def _execute_query():
            try:
                results = self.client.query(query).result()
                return [dict(row) for row in results]
            except Exception as e:
                logger.error(f"Error getting active tokens: {e}")
                return []
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, _execute_query)
    
    async def insert_price_data(self, price_data: List[Dict]) -> bool:
        """Insert price data into BigQuery"""
        if not price_data:
            logger.warning("No price data to insert")
            return True
        
        def _insert_data():
            try:
                # Add timestamps and validate data
                processed_data = []
                for price in price_data:
                    if not price.get('token_address') or not price.get('token_symbol'):
                        continue
                    
                    # Ensure required fields
                    processed_price = {
                        'token_address': price['token_address'].lower(),
                        'token_symbol': price['token_symbol'].upper(),
                        'price_usd': float(price.get('price_usd', 0)),
                        'price_eth': float(price.get('price_eth', 0)),
                        'timestamp': price.get('timestamp', datetime.utcnow()),
                        'volume_24h': float(price.get('volume_24h', 0)),
                        'market_cap': float(price.get('market_cap', 0)),
                        'source': price.get('source', 'unknown'),
                        'price_change_24h': float(price.get('price_change_24h', 0)),
                        'liquidity_usd': float(price.get('liquidity_usd', 0))
                    }
                    processed_data.append(processed_price)
                
                if not processed_data:
                    logger.warning("No valid price data after processing")
                    return True
                
                table_ref = f"{self.dataset_ref}.token_prices"
                errors = self.client.insert_rows_json(table_ref, processed_data)
                
                if errors:
                    logger.error(f"Error inserting price data: {errors}")
                    return False
                
                logger.info(f"Successfully inserted {len(processed_data)} price records")
                return True
                
            except Exception as e:
                logger.error(f"Error inserting price data: {e}")
                return False
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, _insert_data)
    
    async def insert_transactions(self, transactions: List[Dict]) -> bool:
        """Insert transaction data into BigQuery with validation"""
        if not transactions:
            logger.warning("No transactions to insert")
            return True
        
        def _insert_data():
            try:
                # Validate and process transactions
                processed_transactions = []
                for txn in transactions:
                    try:
                        # Validate required fields
                        required_fields = ['transaction_hash', 'wallet_address', 'token_address', 
                                         'token_symbol', 'transfer_type', 'wallet_sophistication_score']
                        
                        if not all(txn.get(field) for field in required_fields):
                            logger.warning(f"Skipping transaction with missing required fields: {txn}")
                            continue
                        
                        # Validate address formats
                        wallet_addr = txn['wallet_address'].lower()
                        token_addr = txn['token_address'].lower()
                        
                        if not (wallet_addr.startswith('0x') and len(wallet_addr) == 42):
                            logger.warning(f"Invalid wallet address format: {wallet_addr}")
                            continue
                        
                        if not (token_addr.startswith('0x') and len(token_addr) == 42):
                            logger.warning(f"Invalid token address format: {token_addr}")
                            continue
                        
                        # Process transaction
                        processed_txn = {
                            'transaction_hash': txn['transaction_hash'],
                            'block_number': int(txn.get('block_number', 0)),
                            'timestamp': txn.get('timestamp', datetime.utcnow()),
                            'wallet_address': wallet_addr,
                            'token_address': token_addr,
                            'token_symbol': txn['token_symbol'].upper(),
                            'token_amount': float(txn.get('token_amount', 0)),
                            'transfer_type': txn['transfer_type'].lower(),
                            'cost_in_eth': float(txn.get('cost_in_eth', 0)),
                            'platform': txn.get('platform', 'unknown'),
                            'network': txn.get('network', 'ethereum'),
                            'wallet_sophistication_score': int(txn['wallet_sophistication_score']),
                            'created_at': datetime.utcnow()
                        }
                        
                        # Validate transfer_type
                        if processed_txn['transfer_type'] not in ['buy', 'sell']:
                            logger.warning(f"Invalid transfer_type: {processed_txn['transfer_type']}")
                            continue
                        
                        processed_transactions.append(processed_txn)
                        
                    except (ValueError, KeyError, TypeError) as e:
                        logger.warning(f"Error processing transaction {txn}: {e}")
                        continue
                
                if not processed_transactions:
                    logger.warning("No valid transactions after processing")
                    return True
                
                # Insert in batches for better performance
                table_ref = f"{self.dataset_ref}.transactions"
                batch_size = 1000
                
                for i in range(0, len(processed_transactions), batch_size):
                    batch = processed_transactions[i:i + batch_size]
                    errors = self.client.insert_rows_json(table_ref, batch)
                    
                    if errors:
                        logger.error(f"Error inserting transaction batch {i//batch_size + 1}: {errors}")
                        return False
                
                logger.info(f"Successfully inserted {len(processed_transactions)} transactions")
                return True
                
            except Exception as e:
                logger.error(f"Error inserting transactions: {e}")
                return False
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, _insert_data)
    
    async def execute_query(self, query: str, job_config: Optional[bigquery.QueryJobConfig] = None) -> bigquery.QueryJob:
        """Execute a BigQuery query asynchronously"""
        def _execute():
            try:
                job = self.client.query(query, job_config=job_config)
                job.result()  # Wait for completion
                logger.info(f"Query completed successfully. Processed {job.total_bytes_processed} bytes")
                return job
            except Exception as e:
                logger.error(f"Error executing query: {e}")
                logger.error(f"Query: {query[:500]}...")  # Log first 500 chars of query
                raise e
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, _execute)
    
    async def get_score_recommendations(
        self, 
        priorities: List[str], 
        hours_back: int = 24, 
        limit: int = 50
    ) -> List[Dict]:
        """Get recent score recommendations with filtering"""
        priorities_str = "', '".join([p.upper() for p in priorities])
        
        query = f"""
        SELECT 
          wallet_address,
          current_score,
          suggested_new_score,
          total_score_change,
          trades_analyzed,
          ROUND(win_rate * 100, 2) as win_rate_pct,
          ROUND(avg_profit_pct, 2) as avg_profit_pct,
          winning_trades,
          losing_trades,
          ROUND(total_volume_usd, 2) as total_volume_usd,
          ROUND(avg_trade_size_usd, 2) as avg_trade_size_usd,
          recommendation_message,
          priority,
          update_calculated_at
        FROM `{self.dataset_ref}.score_update_recommendations`  
        WHERE priority IN ('{priorities_str}')
          AND update_calculated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
        ORDER BY 
          CASE priority 
            WHEN 'CRITICAL' THEN 1 
            WHEN 'HIGH' THEN 2 
            WHEN 'MEDIUM' THEN 3 
            WHEN 'LOW' THEN 4
            ELSE 5
          END,
          ABS(total_score_change) DESC
        LIMIT {limit}
        """
        
        def _execute_query():
            try:
                results = self.client.query(query).result()
                recommendations = []
                for row in results:
                    rec = dict(row)
                    # Convert datetime to ISO string for JSON serialization
                    if rec.get('update_calculated_at'):
                        rec['update_calculated_at'] = rec['update_calculated_at'].isoformat()
                    recommendations.append(rec)
                return recommendations
            except NotFound:
                logger.warning("score_update_recommendations table not found")
                return []
            except Exception as e:
                logger.error(f"Error getting recommendations: {e}")
                return []
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, _execute_query)
    
async def get_wallet_analysis(self, wallet_address: str, days_back: int = 30) -> Dict[str, Any]:
    """Get detailed analysis for a specific wallet"""
    wallet_address = wallet_address.lower()
    
    query = f"""
    WITH wallet_transactions AS (
      SELECT 
        t.*,
        p_buy.price_usd as buy_price,
        p_sell.price_usd as sell_price
      FROM `{self.dataset_ref}.transactions` t
      LEFT JOIN `{self.dataset_ref}.token_prices` p_buy
        ON t.token_address = p_buy.token_address
        AND ABS(TIMESTAMP_DIFF(t.timestamp, p_buy.timestamp, MINUTE)) <= 30
        AND t.transfer_type = 'buy'
      LEFT JOIN `{self.dataset_ref}.token_prices` p_sell
        ON t.token_address = p_sell.token_address  
        AND ABS(TIMESTAMP_DIFF(t.timestamp, p_sell.timestamp, MINUTE)) <= 30
        AND t.transfer_type = 'sell'
      WHERE t.wallet_address = '{wallet_address}'
        AND t.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
    ),
    
    basic_stats AS (
      SELECT 
        wallet_sophistication_score,
        COUNT(*) as total_transactions,
        COUNT(DISTINCT token_address) as unique_tokens,
        SUM(CASE WHEN transfer_type = 'buy' THEN 1 ELSE 0 END) as buy_count,
        SUM(CASE WHEN transfer_type = 'sell' THEN 1 ELSE 0 END) as sell_count,
        SUM(cost_in_eth) as total_gas_cost,
        AVG(cost_in_eth) as avg_gas_cost,
        MIN(timestamp) as first_transaction,
        MAX(timestamp) as last_transaction,
        
        -- Price-based analysis
        AVG(CASE WHEN buy_price IS NOT NULL THEN buy_price END) as avg_buy_price,
        AVG(CASE WHEN sell_price IS NOT NULL THEN sell_price END) as avg_sell_price,
        COUNT(CASE WHEN buy_price IS NOT NULL THEN 1 END) as trades_with_price_data,
        
        -- Volume analysis
        SUM(CASE WHEN transfer_type = 'buy' AND buy_price IS NOT NULL 
            THEN token_amount * buy_price ELSE 0 END) as total_buy_volume_usd,
        SUM(CASE WHEN transfer_type = 'sell' AND sell_price IS NOT NULL 
            THEN token_amount * sell_price ELSE 0 END) as total_sell_volume_usd,
            
        -- Platform distribution
        COUNT(DISTINCT platform) as unique_platforms
        
      FROM wallet_transactions
      GROUP BY wallet_sophistication_score
    )
    
    SELECT 
      bs.*,
      -- Calculate activity frequency
      DATE_DIFF(DATE(bs.last_transaction), DATE(bs.first_transaction), DAY) as active_days,
      bs.total_transactions / NULLIF(DATE_DIFF(DATE(bs.last_transaction), DATE(bs.first_transaction), DAY), 0) as avg_transactions_per_day,
      
      -- Gas efficiency
      CASE WHEN bs.total_buy_volume_usd > 0 
           THEN (bs.total_gas_cost * 2500) / bs.total_buy_volume_usd 
           ELSE 0 END as gas_efficiency_ratio,
      
      -- Trading balance
      CASE WHEN bs.total_sell_volume_usd > 0 AND bs.total_buy_volume_usd > 0
           THEN (bs.total_sell_volume_usd - bs.total_buy_volume_usd) / bs.total_buy_volume_usd
           ELSE 0 END as net_trading_return,
      
      -- Wallet tier classification
      CASE 
        WHEN bs.wallet_sophistication_score < 180 THEN 'Elite'
        WHEN bs.wallet_sophistication_score < 220 THEN 'High'
        WHEN bs.wallet_sophistication_score < 260 THEN 'Medium'
        ELSE 'Low'
      END as wallet_tier
      
    FROM basic_stats bs
    """
    
    def _execute_query():
        try:
            results = list(self.client.query(query).result())
            if results:
                analysis = dict(results[0])
                
                # Convert datetime fields to ISO strings
                for field in ['first_transaction', 'last_transaction']:
                    if analysis.get(field):
                        analysis[field] = analysis[field].isoformat()
                
                # Get additional token diversity data
                token_query = f"""
                SELECT 
                  token_symbol,
                  COUNT(*) as transaction_count,
                  SUM(CASE WHEN transfer_type = 'buy' THEN token_amount ELSE 0 END) as total_bought,
                  SUM(CASE WHEN transfer_type = 'sell' THEN token_amount ELSE 0 END) as total_sold,
                  MIN(timestamp) as first_seen,
                  MAX(timestamp) as last_seen
                FROM `{self.dataset_ref}.transactions`
                WHERE wallet_address = '{wallet_address}'
                  AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
                GROUP BY token_symbol
                ORDER BY transaction_count DESC
                LIMIT 10
                """
                
                token_results = list(self.client.query(token_query).result())
                analysis['top_tokens'] = []
                for token_row in token_results:
                    token_data = dict(token_row)
                    for field in ['first_seen', 'last_seen']:
                        if token_data.get(field):
                            token_data[field] = token_data[field].isoformat()
                    analysis['top_tokens'].append(token_data)
                
                # Get recent activity pattern
                activity_query = f"""
                SELECT 
                  DATE(timestamp) as activity_date,
                  COUNT(*) as transaction_count,
                  COUNT(DISTINCT token_address) as unique_tokens,
                  SUM(cost_in_eth) as daily_gas_cost
                FROM `{self.dataset_ref}.transactions`
                WHERE wallet_address = '{wallet_address}'
                  AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
                GROUP BY DATE(timestamp)
                ORDER BY activity_date DESC
                LIMIT 14
                """
                
                activity_results = list(self.client.query(activity_query).result())
                analysis['recent_activity'] = []
                for activity_row in activity_results:
                    activity_data = dict(activity_row)
                    if activity_data.get('activity_date'):
                        activity_data['activity_date'] = activity_data['activity_date'].isoformat()
                    analysis['recent_activity'].append(activity_data)
                
                return analysis
            else:
                return {"error": "Wallet not found or no recent activity"}
        except Exception as e:
            logger.error(f"Error analyzing wallet {wallet_address}: {e}")
            return {"error": str(e)}
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(self.executor, _execute_query)

async def get_system_stats(self) -> Dict[str, Any]:
    """Get comprehensive system statistics"""
    query = f"""
    WITH transaction_stats AS (
      SELECT 
        'transactions' as table_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT wallet_address) as unique_wallets,
        COUNT(DISTINCT token_address) as unique_tokens,
        MIN(timestamp) as earliest_record,
        MAX(timestamp) as latest_record,
        SUM(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN 1 ELSE 0 END) as records_24h,
        SUM(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) THEN 1 ELSE 0 END) as records_7d
      FROM `{self.dataset_ref}.transactions`
    ),
    
    price_stats AS (
      SELECT 
        'token_prices' as table_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT token_address) as unique_tokens,
        0 as unique_wallets,
        MIN(timestamp) as earliest_record,
        MAX(timestamp) as latest_record,
        SUM(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN 1 ELSE 0 END) as records_24h,
        SUM(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) THEN 1 ELSE 0 END) as records_7d
      FROM `{self.dataset_ref}.token_prices`
    ),
    
    recommendation_stats AS (
      SELECT 
        'recommendations' as table_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT wallet_address) as unique_wallets,
        0 as unique_tokens,
        MIN(update_calculated_at) as earliest_record,
        MAX(update_calculated_at) as latest_record,
        SUM(CASE WHEN update_calculated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN 1 ELSE 0 END) as records_24h,
        SUM(CASE WHEN update_calculated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) THEN 1 ELSE 0 END) as records_7d
      FROM `{self.dataset_ref}.score_update_recommendations`
    )
    
    SELECT * FROM transaction_stats
    UNION ALL
    SELECT * FROM price_stats
    UNION ALL
    SELECT * FROM recommendation_stats
    """
    
    def _execute_query():
        try:
            # Get main stats
            main_results = list(self.client.query(query).result())
            stats = {"tables": []}
            
            for row in main_results:
                table_data = dict(row)
                # Convert datetime fields
                for field in ['earliest_record', 'latest_record']:
                    if table_data.get(field):
                        table_data[field] = table_data[field].isoformat()
                stats["tables"].append(table_data)
            
            # Get wallet tier distribution
            tier_query = f"""
            SELECT 
              CASE 
                WHEN wallet_sophistication_score < 180 THEN 'Elite'
                WHEN wallet_sophistication_score < 220 THEN 'High'
                WHEN wallet_sophistication_score < 260 THEN 'Medium'
                ELSE 'Low'
              END as wallet_tier,
              COUNT(DISTINCT wallet_address) as wallet_count,
              ROUND(AVG(wallet_sophistication_score), 1) as avg_score
            FROM `{self.dataset_ref}.transactions`
            WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
            GROUP BY 
              CASE 
                WHEN wallet_sophistication_score < 180 THEN 'Elite'
                WHEN wallet_sophistication_score < 220 THEN 'High'
                WHEN wallet_sophistication_score < 260 THEN 'Medium'
                ELSE 'Low'
              END
            ORDER BY avg_score
            """
            
            tier_results = list(self.client.query(tier_query).result())
            stats["wallet_tiers"] = [dict(row) for row in tier_results]
            
            # Get recent recommendation summary
            rec_summary_query = f"""
            SELECT 
              priority,
              COUNT(*) as count,
              ROUND(AVG(ABS(total_score_change)), 1) as avg_change
            FROM `{self.dataset_ref}.score_update_recommendations`
            WHERE update_calculated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            GROUP BY priority
            ORDER BY 
              CASE priority 
                WHEN 'CRITICAL' THEN 1 
                WHEN 'HIGH' THEN 2 
                WHEN 'MEDIUM' THEN 3 
                WHEN 'LOW' THEN 4
                ELSE 5
              END
            """
            
            try:
                rec_summary_results = list(self.client.query(rec_summary_query).result())
                stats["recent_recommendations"] = [dict(row) for row in rec_summary_results]
            except NotFound:
                stats["recent_recommendations"] = []
            
            # Add system metadata
            stats["metadata"] = {
                "project_id": self.project_id,
                "dataset_id": self.dataset_id,
                "generated_at": datetime.utcnow().isoformat()
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting system stats: {e}")
            return {"error": str(e)}
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(self.executor, _execute_query)

async def get_top_performing_wallets(self, days_back: int = 7, limit: int = 20) -> List[Dict]:
    """Get top performing wallets based on recent score improvements"""
    query = f"""
    SELECT 
      wsu.wallet_address,
      wsu.current_score,
      wsu.suggested_new_score,
      wsu.total_score_change,
      wsu.trades_analyzed,
      ROUND(wsu.win_rate * 100, 1) as win_rate_pct,
      ROUND(wsu.avg_profit_pct, 2) as avg_profit_pct,
      ROUND(wsu.total_volume_usd, 2) as total_volume_usd,
      ROUND(wsu.avg_trade_size_usd, 2) as avg_trade_size_usd,
      
      -- Get wallet tier
      CASE 
        WHEN wsu.current_score < 180 THEN 'Elite'
        WHEN wsu.current_score < 220 THEN 'High'
        WHEN wsu.current_score < 260 THEN 'Medium'
        ELSE 'Low'
      END as current_tier,
      
      CASE 
        WHEN wsu.suggested_new_score < 180 THEN 'Elite'
        WHEN wsu.suggested_new_score < 220 THEN 'High'
        WHEN wsu.suggested_new_score < 260 THEN 'Medium'
        ELSE 'Low'
      END as suggested_tier,
      
      wsu.update_calculated_at
      
    FROM `{self.dataset_ref}.wallet_score_updates` wsu
    WHERE wsu.update_calculated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
      AND wsu.total_score_change < 0  -- Only improvements (negative is better)
    ORDER BY wsu.total_score_change ASC  -- Most improved first
    LIMIT {limit}
    """
    
    def _execute_query():
        try:
            results = self.client.query(query).result()
            wallets = []
            for row in results:
                wallet_data = dict(row)
                if wallet_data.get('update_calculated_at'):
                    wallet_data['update_calculated_at'] = wallet_data['update_calculated_at'].isoformat()
                wallets.append(wallet_data)
            return wallets
        except NotFound:
            logger.warning("wallet_score_updates table not found")
            return []
        except Exception as e:
            logger.error(f"Error getting top performing wallets: {e}")
            return []
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(self.executor, _execute_query)

async def get_worst_performing_wallets(self, days_back: int = 7, limit: int = 20) -> List[Dict]:
    """Get worst performing wallets based on recent score degradations"""
    query = f"""
    SELECT 
      wsu.wallet_address,
      wsu.current_score,
      wsu.suggested_new_score,
      wsu.total_score_change,
      wsu.trades_analyzed,
      ROUND(wsu.win_rate * 100, 1) as win_rate_pct,
      ROUND(wsu.avg_profit_pct, 2) as avg_profit_pct,
      ROUND(wsu.total_volume_usd, 2) as total_volume_usd,
      
      -- Get wallet tier
      CASE 
        WHEN wsu.current_score < 180 THEN 'Elite'
        WHEN wsu.current_score < 220 THEN 'High'
        WHEN wsu.current_score < 260 THEN 'Medium'
        ELSE 'Low'
      END as current_tier,
      
      CASE 
        WHEN wsu.suggested_new_score < 180 THEN 'Elite'
        WHEN wsu.suggested_new_score < 220 THEN 'High'
        WHEN wsu.suggested_new_score < 260 THEN 'Medium'
        ELSE 'Low'
      END as suggested_tier,
      
      wsu.update_calculated_at
      
    FROM `{self.dataset_ref}.wallet_score_updates` wsu
    WHERE wsu.update_calculated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
      AND wsu.total_score_change > 0  -- Only degradations (positive is worse)
    ORDER BY wsu.total_score_change DESC  -- Most degraded first
    LIMIT {limit}
    """
    
    def _execute_query():
        try:
            results = self.client.query(query).result()
            wallets = []
            for row in results:
                wallet_data = dict(row)
                if wallet_data.get('update_calculated_at'):
                    wallet_data['update_calculated_at'] = wallet_data['update_calculated_at'].isoformat()
                wallets.append(wallet_data)
            return wallets
        except NotFound:
            logger.warning("wallet_score_updates table not found")
            return []
        except Exception as e:
            logger.error(f"Error getting worst performing wallets: {e}")
            return []
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(self.executor, _execute_query)

async def get_token_smart_money_analysis(self, limit: int = 50) -> List[Dict]:
    """Get tokens with highest smart money interest"""
    query = f"""
    WITH elite_wallet_activity AS (
      SELECT 
        t.token_address,
        t.token_symbol,
        COUNT(DISTINCT t.wallet_address) as total_wallets,
        COUNT(DISTINCT CASE WHEN t.wallet_sophistication_score < 180 THEN t.wallet_address END) as elite_wallets,
        COUNT(DISTINCT CASE WHEN t.wallet_sophistication_score < 220 THEN t.wallet_address END) as high_tier_wallets,
        
        -- Volume by wallet tiers
        SUM(CASE WHEN t.wallet_sophistication_score < 180 THEN t.token_amount ELSE 0 END) as elite_volume,
        SUM(CASE WHEN t.wallet_sophistication_score < 220 THEN t.token_amount ELSE 0 END) as high_tier_volume,
        SUM(t.token_amount) as total_volume,
        
        -- Recent activity (last 24h)
        COUNT(CASE WHEN t.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN 1 END) as recent_transactions,
        COUNT(DISTINCT CASE WHEN t.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) 
                                AND t.wallet_sophistication_score < 180 
                           THEN t.wallet_address END) as recent_elite_wallets,
        
        -- Buy vs Sell activity for elite wallets
        SUM(CASE WHEN t.wallet_sophistication_score < 180 AND t.transfer_type = 'buy' THEN t.token_amount ELSE 0 END) as elite_buy_volume,
        SUM(CASE WHEN t.wallet_sophistication_score < 180 AND t.transfer_type = 'sell' THEN t.token_amount ELSE 0 END) as elite_sell_volume,
        
        MAX(t.timestamp) as last_activity
        
      FROM `{self.dataset_ref}.transactions` t
      WHERE t.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        AND t.token_address != '0x0000000000000000000000000000000000000000'
      GROUP BY t.token_address, t.token_symbol
      HAVING total_wallets >= 3  -- At least 3 different wallets
    )
    
    SELECT 
      token_address,
      token_symbol,
      total_wallets,
      elite_wallets,
      high_tier_wallets,
      
      -- Smart money ratios
      ROUND(SAFE_DIVIDE(elite_wallets, total_wallets) * 100, 2) as elite_wallet_ratio_pct,
      ROUND(SAFE_DIVIDE(elite_volume, total_volume) * 100, 2) as elite_volume_ratio_pct,
      
      -- Net elite activity (positive = accumulating, negative = distributing)
      ROUND(elite_buy_volume - elite_sell_volume, 2) as elite_net_flow,
      CASE 
        WHEN elite_buy_volume + elite_sell_volume > 0 
        THEN ROUND((elite_buy_volume - elite_sell_volume) / (elite_buy_volume + elite_sell_volume) * 100, 2)
        ELSE 0
      END as elite_accumulation_score,
      
      recent_transactions,
      recent_elite_wallets,
      last_activity,
      
      -- Overall smart money score
      ROUND(
        (SAFE_DIVIDE(elite_wallets, total_wallets) * 40) +  -- 40% weight on elite wallet ratio
        (SAFE_DIVIDE(elite_volume, total_volume) * 30) +    -- 30% weight on elite volume ratio  
        (LEAST(SAFE_DIVIDE(recent_elite_wallets, 10), 1) * 30)  -- 30% weight on recent elite activity
      , 2) as smart_money_score
      
    FROM elite_wallet_activity
    WHERE elite_wallets > 0  -- Must have some elite wallet interest
    ORDER BY smart_money_score DESC, elite_wallets DESC
    LIMIT {limit}
    """
    
    def _execute_query():
        try:
            results = self.client.query(query).result()
            tokens = []
            for row in results:
                token_data = dict(row)
                if token_data.get('last_activity'):
                    token_data['last_activity'] = token_data['last_activity'].isoformat()
                tokens.append(token_data)
            return tokens
        except Exception as e:
            logger.error(f"Error getting token smart money analysis: {e}")
            return []
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(self.executor, _execute_query)

async def get_wallet_portfolio_analysis(self, wallet_address: str, days_back: int = 30) -> Dict[str, Any]:
    """Get detailed portfolio analysis for a wallet"""
    wallet_address = wallet_address.lower()
    
    query = f"""
    WITH wallet_positions AS (
      SELECT 
        token_address,
        token_symbol,
        SUM(CASE WHEN transfer_type = 'buy' THEN token_amount ELSE 0 END) as total_bought,
        SUM(CASE WHEN transfer_type = 'sell' THEN token_amount ELSE 0 END) as total_sold,
        SUM(CASE WHEN transfer_type = 'buy' THEN token_amount ELSE -token_amount END) as net_position,
        COUNT(*) as total_transactions,
        MIN(timestamp) as first_transaction,
        MAX(timestamp) as last_transaction,
        
        -- Price analysis (if available)
        AVG(CASE WHEN transfer_type = 'buy' THEN 
          (SELECT price_usd FROM `{self.dataset_ref}.token_prices` p 
           WHERE p.token_address = t.token_address 
             AND ABS(TIMESTAMP_DIFF(t.timestamp, p.timestamp, MINUTE)) <= 30
           LIMIT 1)
        END) as avg_buy_price,
        
        AVG(CASE WHEN transfer_type = 'sell' THEN 
          (SELECT price_usd FROM `{self.dataset_ref}.token_prices` p 
           WHERE p.token_address = t.token_address 
             AND ABS(TIMESTAMP_DIFF(t.timestamp, p.timestamp, MINUTE)) <= 30
           LIMIT 1)
        END) as avg_sell_price
        
      FROM `{self.dataset_ref}.transactions` t
      WHERE wallet_address = '{wallet_address}'
        AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
      GROUP BY token_address, token_symbol
    )
    
    SELECT 
      token_address,
      token_symbol,
      total_bought,
      total_sold,
      net_position,
      total_transactions,
      first_transaction,
      last_transaction,
      
      -- Calculate holding period
      TIMESTAMP_DIFF(last_transaction, first_transaction, HOUR) as holding_period_hours,
      
      -- Price performance (if price data available)
      avg_buy_price,
      avg_sell_price,
      CASE 
        WHEN avg_buy_price > 0 AND avg_sell_price > 0 
        THEN ROUND((avg_sell_price - avg_buy_price) / avg_buy_price * 100, 2)
        ELSE NULL 
      END as price_performance_pct,
      
      -- Position status
      CASE 
        WHEN net_position > 0 THEN 'Long'
        WHEN net_position < 0 THEN 'Short' 
        ELSE 'Flat'
      END as position_status,
      
      ABS(net_position) as position_size
      
    FROM wallet_positions
    ORDER BY total_transactions DESC, ABS(net_position) DESC
    """
    
    def _execute_query():
        try:
            results = list(self.client.query(query).result())
            portfolio = {"positions": [], "summary": {}}
            
            total_positions = len(results)
            long_positions = sum(1 for row in results if dict(row)['net_position'] > 0)
            flat_positions = sum(1 for row in results if dict(row)['net_position'] == 0)
            
            # Process individual positions
            for row in results:
                position_data = dict(row)
                # Convert datetime fields
                for field in ['first_transaction', 'last_transaction']:
                    if position_data.get(field):
                        position_data[field] = position_data[field].isoformat()
                portfolio["positions"].append(position_data)
            
            # Create summary
            portfolio["summary"] = {
                "total_tokens_traded": total_positions,
                "long_positions": long_positions,
                "short_positions": total_positions - long_positions - flat_positions,
                "flat_positions": flat_positions,
                "portfolio_diversification": min(total_positions / 10.0, 1.0),  # Normalized 0-1
                "analysis_period_days": days_back
            }
            
            return portfolio
            
        except Exception as e:
            logger.error(f"Error analyzing wallet portfolio: {e}")
            return {"error": str(e)}
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(self.executor, _execute_query)

async def get_wallet_trading_patterns(self, wallet_address: str, days_back: int = 30) -> Dict[str, Any]:
    """Analyze trading patterns and behavior for a specific wallet"""
    wallet_address = wallet_address.lower()
    
    query = f"""
    WITH trading_sessions AS (
      SELECT 
        DATE(timestamp) as trading_date,
        EXTRACT(HOUR FROM timestamp) as trading_hour,
        transfer_type,
        token_symbol,
        token_amount,
        cost_in_eth,
        timestamp,
        
        -- Group consecutive transactions within 1 hour as sessions
        TIMESTAMP_DIFF(timestamp, LAG(timestamp) OVER (ORDER BY timestamp), MINUTE) as minutes_since_last
        
      FROM `{self.dataset_ref}.transactions`
      WHERE wallet_address = '{wallet_address}'
        AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
      ORDER BY timestamp
    ),
    
    hourly_patterns AS (
      SELECT 
        trading_hour,
        COUNT(*) as transaction_count,
        COUNT(DISTINCT trading_date) as active_days_in_hour,
        AVG(cost_in_eth) as avg_gas_cost
      FROM trading_sessions
      GROUP BY trading_hour
    ),
    
    daily_patterns AS (
      SELECT 
        EXTRACT(DAYOFWEEK FROM trading_date) as day_of_week,
        COUNT(*) as transaction_count,
        COUNT(DISTINCT trading_date) as active_days,
        AVG(cost_in_eth) as avg_gas_cost
      FROM trading_sessions
      GROUP BY EXTRACT(DAYOFWEEK FROM trading_date)
    ),
    
    session_analysis AS (
      SELECT 
        AVG(CASE WHEN minutes_since_last IS NOT NULL AND minutes_since_last > 60 THEN minutes_since_last END) as avg_session_gap_minutes,
        COUNT(CASE WHEN minutes_since_last IS NOT NULL AND minutes_since_last <= 5 THEN 1 END) as rapid_fire_transactions,
        COUNT(CASE WHEN minutes_since_last IS NOT NULL AND minutes_since_last > 1440 THEN 1 END) as new_day_transactions
      FROM trading_sessions
    )
    
    SELECT 
      'hourly_patterns' as pattern_type,
      TO_JSON_STRING(ARRAY_AGG(STRUCT(trading_hour, transaction_count, active_days_in_hour) ORDER BY trading_hour)) as data
    FROM hourly_patterns
    
    UNION ALL
    
    SELECT 
      'daily_patterns' as pattern_type,
      TO_JSON_STRING(ARRAY_AGG(STRUCT(day_of_week, transaction_count, active_days) ORDER BY day_of_week)) as data
    FROM daily_patterns
    
    UNION ALL
    
    SELECT 
      'session_analysis' as pattern_type,
      TO_JSON_STRING(STRUCT(
        avg_session_gap_minutes,
        rapid_fire_transactions,
        new_day_transactions
      )) as data
    FROM session_analysis
    """
    
    def _execute_query():
        try:
            results = list(self.client.query(query).result())
            patterns = {}
            
            for row in results:
                pattern_type = row['pattern_type']
                data = json.loads(row['data'])
                patterns[pattern_type] = data
            
            return patterns
            
        except Exception as e:
            logger.error(f"Error analyzing trading patterns for {wallet_address}: {e}")
            return {"error": str(e)}
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(self.executor, _execute_query)

async def get_token_correlation_analysis(self, token_address: str, days_back: int = 30) -> Dict[str, Any]:
    """Analyze which other tokens are traded by holders of a specific token"""
    token_address = token_address.lower()
    
    query = f"""
    WITH target_token_holders AS (
      SELECT DISTINCT wallet_address
      FROM `{self.dataset_ref}.transactions`
      WHERE token_address = '{token_address}'
        AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
    ),
    
    correlated_tokens AS (
      SELECT 
        t.token_address,
        t.token_symbol,
        COUNT(DISTINCT t.wallet_address) as shared_holders,
        COUNT(*) as total_transactions,
        SUM(CASE WHEN t.transfer_type = 'buy' THEN t.token_amount ELSE 0 END) as total_buy_volume,
        SUM(CASE WHEN t.transfer_type = 'sell' THEN t.token_amount ELSE 0 END) as total_sell_volume,
        
        -- Wallet sophistication analysis
        AVG(t.wallet_sophistication_score) as avg_holder_sophistication,
        COUNT(DISTINCT CASE WHEN t.wallet_sophistication_score < 180 THEN t.wallet_address END) as elite_holders,
        
        -- Time correlation
        MIN(t.timestamp) as first_transaction,
        MAX(t.timestamp) as last_transaction
        
      FROM `{self.dataset_ref}.transactions` t
      WHERE t.wallet_address IN (SELECT wallet_address FROM target_token_holders)
        AND t.token_address != '{token_address}'
        AND t.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
      GROUP BY t.token_address, t.token_symbol
      HAVING shared_holders >= 3  -- At least 3 shared holders
    ),
    
    holder_count AS (
      SELECT COUNT(*) as total_target_holders
      FROM target_token_holders
    )
    
    SELECT 
      ct.token_address,
      ct.token_symbol,
      ct.shared_holders,
      hc.total_target_holders,
      ROUND(ct.shared_holders / hc.total_target_holders * 100, 2) as holder_overlap_pct,
      ct.total_transactions,
      ct.total_buy_volume,
      ct.total_sell_volume,
      ROUND(ct.avg_holder_sophistication, 1) as avg_holder_sophistication,
      ct.elite_holders,
      ct.first_transaction,
      ct.last_transaction,
      
      -- Net flow analysis
      ROUND(ct.total_buy_volume - ct.total_sell_volume, 2) as net_flow,
      
      -- Correlation score
      ROUND(
        (ct.shared_holders / hc.total_target_holders * 50) +  -- 50% weight on holder overlap
        (LEAST(ct.elite_holders / 10.0, 1) * 30) +           -- 30% weight on elite holder count  
        (LEAST(ct.total_transactions / 100.0, 1) * 20)       -- 20% weight on activity
      , 2) as correlation_score
      
    FROM correlated_tokens ct
    CROSS JOIN holder_count hc
    ORDER BY correlation_score DESC, shared_holders DESC
    LIMIT 20
    """
    
    def _execute_query():
        try:
            results = list(self.client.query(query).result())
            correlations = []
            
            for row in results:
                correlation_data = dict(row)
                # Convert datetime fields
                for field in ['first_transaction', 'last_transaction']:
                    if correlation_data.get(field):
                        correlation_data[field] = correlation_data[field].isoformat()
                correlations.append(correlation_data)
            
            return {"correlated_tokens": correlations}
            
        except Exception as e:
            logger.error(f"Error analyzing token correlations for {token_address}: {e}")
            return {"error": str(e)}
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(self.executor, _execute_query)

async def get_market_sentiment_analysis(self, hours_back: int = 24) -> Dict[str, Any]:
    """Analyze overall market sentiment based on elite wallet activity"""
    query = f"""
    WITH elite_activity AS (
      SELECT 
        DATE(timestamp) as activity_date,
        EXTRACT(HOUR FROM timestamp) as activity_hour,
        transfer_type,
        token_symbol,
        wallet_sophistication_score,
        token_amount,
        cost_in_eth,
        
        -- Classify wallet tiers
        CASE 
          WHEN wallet_sophistication_score < 180 THEN 'Elite'
          WHEN wallet_sophistication_score < 220 THEN 'High'
          WHEN wallet_sophistication_score < 260 THEN 'Medium'
          ELSE 'Low'
        END as wallet_tier
        
      FROM `{self.dataset_ref}.transactions`
      WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
    ),
    
    sentiment_metrics AS (
      SELECT 
        wallet_tier,
        COUNT(*) as total_transactions,
        COUNT(DISTINCT token_symbol) as unique_tokens,
        SUM(CASE WHEN transfer_type = 'buy' THEN token_amount ELSE 0 END) as total_buy_volume,
        SUM(CASE WHEN transfer_type = 'sell' THEN token_amount ELSE 0 END) as total_sell_volume,
        SUM(cost_in_eth) as total_gas_spent,
        
        -- Calculate buy/sell ratios
        COUNT(CASE WHEN transfer_type = 'buy' THEN 1 END) as buy_transactions,
        COUNT(CASE WHEN transfer_type = 'sell' THEN 1 END) as sell_transactions
        
      FROM elite_activity
      GROUP BY wallet_tier
    ),
    
    hourly_sentiment AS (
      SELECT 
        activity_hour,
        COUNT(CASE WHEN wallet_tier = 'Elite' AND transfer_type = 'buy' THEN 1 END) as elite_buys,
        COUNT(CASE WHEN wallet_tier = 'Elite' AND transfer_type = 'sell' THEN 1 END) as elite_sells,
        COUNT(CASE WHEN wallet_tier = 'High' AND transfer_type = 'buy' THEN 1 END) as high_tier_buys,
        COUNT(CASE WHEN wallet_tier = 'High' AND transfer_type = 'sell' THEN 1 END) as high_tier_sells
      FROM elite_activity
      GROUP BY activity_hour
      ORDER BY activity_hour
    )
    
    SELECT 
      'tier_metrics' as analysis_type,
      TO_JSON_STRING(ARRAY_AGG(
        STRUCT(
          wallet_tier,
          total_transactions,
          unique_tokens,
          total_buy_volume,
          total_sell_volume,
          buy_transactions,
          sell_transactions,
          ROUND(SAFE_DIVIDE(buy_transactions, buy_transactions + sell_transactions) * 100, 2) as buy_ratio_pct,
          ROUND(total_gas_spent, 4) as total_gas_spent
        )
      )) as data
    FROM sentiment_metrics
    
    UNION ALL
    
    SELECT 
      'hourly_sentiment' as analysis_type,
      TO_JSON_STRING(ARRAY_AGG(
        STRUCT(
          activity_hour,
          elite_buys,
          elite_sells,
          high_tier_buys,
          high_tier_sells,
          CASE 
            WHEN elite_buys + elite_sells > 0 
            THEN ROUND(elite_buys / (elite_buys + elite_sells) * 100, 2)
            ELSE 0 
          END as elite_buy_ratio_pct
        ) ORDER BY activity_hour
      )) as data
    FROM hourly_sentiment
    """
    
    def _execute_query():
        try:
            results = list(self.client.query(query).result())
            sentiment = {}
            
            for row in results:
                analysis_type = row['analysis_type']
                data = json.loads(row['data'])
                sentiment[analysis_type] = data
            
            # Calculate overall sentiment score
            if 'tier_metrics' in sentiment:
                elite_metrics = next((tier for tier in sentiment['tier_metrics'] if tier['wallet_tier'] == 'Elite'), None)
                if elite_metrics and elite_metrics['buy_transactions'] + elite_metrics['sell_transactions'] > 0:
                    overall_sentiment = elite_metrics['buy_ratio_pct']
                    sentiment['overall_sentiment'] = {
                        'score': overall_sentiment,
                        'interpretation': (
                            'Bullish' if overall_sentiment > 60 else
                            'Bearish' if overall_sentiment < 40 else
                            'Neutral'
                        ),
                        'confidence': 'High' if elite_metrics['total_transactions'] > 50 else 'Medium' if elite_metrics['total_transactions'] > 10 else 'Low'
                    }
            
            return sentiment
            
        except Exception as e:
            logger.error(f"Error analyzing market sentiment: {e}")
            return {"error": str(e)}
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(self.executor, _execute_query)

async def batch_update_wallet_scores(self, score_updates: List[Dict[str, Union[str, int]]]) -> bool:
    """Batch update wallet sophistication scores"""
    if not score_updates:
        return True
    
    def _update_scores():
        try:
            # Create temporary table with updates
            temp_table_id = f"{self.dataset_ref}.temp_score_updates_{int(datetime.utcnow().timestamp())}"
            
            # Define schema for temporary table
            schema = [
                bigquery.SchemaField("wallet_address", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("new_score", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("score_change", "INTEGER"),
                bigquery.SchemaField("update_reason", "STRING")
            ]
            
            # Create temporary table
            temp_table = bigquery.Table(temp_table_id, schema=schema)
            temp_table = self.client.create_table(temp_table)
            
            try:
                # Insert update data
                processed_updates = []
                for update in score_updates:
                    processed_update = {
                        'wallet_address': update['wallet_address'].lower(),
                        'new_score': int(update['new_score']),
                        'score_change': int(update.get('score_change', 0)),
                        'update_reason': update.get('update_reason', 'Batch update')
                    }
                    processed_updates.append(processed_update)
                
                errors = self.client.insert_rows_json(temp_table, processed_updates)
                if errors:
                    logger.error(f"Error inserting batch updates: {errors}")
                    return False
                
                # Apply updates to transactions table
                update_query = f"""
                UPDATE `{self.dataset_ref}.transactions` t
                SET wallet_sophistication_score = u.new_score
                FROM `{temp_table_id}` u
                WHERE t.wallet_address = u.wallet_address
                """
                
                job = self.client.query(update_query)
                job.result()
                
                rows_updated = job.num_dml_affected_rows or 0
                logger.info(f"Batch updated {rows_updated} transaction records with new wallet scores")
                
                return True
                
            finally:
                # Clean up temporary table
                self.client.delete_table(temp_table)
                
        except Exception as e:
            logger.error(f"Error in batch score update: {e}")
            return False
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(self.executor, _update_scores)

async def cleanup_old_data(self, days_to_keep: int = 90) -> Dict[str, int]:
    """Clean up old data to manage storage costs"""
    cleanup_results = {}
    
    # Tables to clean up with their timestamp fields
    cleanup_tables = {
        "token_prices": "timestamp",
        "matched_trades": "buy_time",
        "score_adjustments": "calculated_at",
        "wallet_score_updates": "update_calculated_at",
        "score_update_recommendations": "update_calculated_at"
    }
    
    for table_name, timestamp_field in cleanup_tables.items():
        try:
            query = f"""
            DELETE FROM `{self.dataset_ref}.{table_name}`
            WHERE {timestamp_field} < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_to_keep} DAY)
            """
            
            job = await self.execute_query(query)
            rows_deleted = job.num_dml_affected_rows or 0
            cleanup_results[table_name] = rows_deleted
            
            if rows_deleted > 0:
                logger.info(f"Cleaned up {rows_deleted} old records from {table_name}")
            
        except Exception as e:
            logger.error(f"Error cleaning up {table_name}: {e}")
            cleanup_results[table_name] = -1
    
    return cleanup_results

async def get_elite_wallet_leaderboard(self, limit: int = 100) -> List[Dict]:
    """Get leaderboard of elite wallets by recent performance"""
    query = f"""
    WITH recent_performance AS (
      SELECT 
        t.wallet_address,
        t.wallet_sophistication_score,
        COUNT(*) as recent_transactions,
        COUNT(DISTINCT t.token_address) as tokens_traded,
        SUM(t.cost_in_eth) as total_gas_spent,
        MIN(t.timestamp) as first_recent_activity,
        MAX(t.timestamp) as last_activity,
        
        -- Calculate estimated profits (where price data is available)
        SUM(CASE 
          WHEN t.transfer_type = 'sell' AND p.price_usd IS NOT NULL 
          THEN t.token_amount * p.price_usd 
          ELSE 0 
        END) as estimated_sell_volume_usd,
        
        SUM(CASE 
          WHEN t.transfer_type = 'buy' AND p.price_usd IS NOT NULL 
          THEN t.token_amount * p.price_usd 
          ELSE 0 
        END) as estimated_buy_volume_usd
        
      FROM `{self.dataset_ref}.transactions` t
      LEFT JOIN `{self.dataset_ref}.token_prices` p
        ON t.token_address = p.token_address
        AND ABS(TIMESTAMP_DIFF(t.timestamp, p.timestamp, MINUTE)) <= 30
      WHERE t.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        AND t.wallet_sophistication_score < 200  -- Focus on elite and high-tier wallets
      GROUP BY t.wallet_address, t.wallet_sophistication_score
      HAVING recent_transactions >= 5  -- Minimum activity threshold
    ),
    
    performance_scores AS (
      SELECT 
        *,
        -- Calculate performance metrics
        CASE 
          WHEN estimated_buy_volume_usd > 0 
          THEN (estimated_sell_volume_usd - estimated_buy_volume_usd) / estimated_buy_volume_usd * 100
          ELSE 0 
        END as estimated_return_pct,
        
        -- Activity score (transactions per day)
        recent_transactions / GREATEST(DATE_DIFF(DATE(last_activity), DATE(first_recent_activity), DAY), 1) as activity_score,
        
        -- Diversification score
        LEAST(tokens_traded / 10.0, 1.0) as diversification_score,
        
        -- Gas efficiency (lower is better, so invert)
        CASE 
          WHEN estimated_buy_volume_usd > 0 
          THEN 1.0 / (1.0 + (total_gas_spent * 2500) / estimated_buy_volume_usd)
          ELSE 0.5 
        END as gas_efficiency_score
        
      FROM recent_performance
    )
    
    SELECT 
      wallet_address,
      wallet_sophistication_score,
      recent_transactions,
      tokens_traded,
      ROUND(total_gas_spent, 4) as total_gas_spent,
      ROUND(estimated_buy_volume_usd, 2) as estimated_buy_volume_usd,
      ROUND(estimated_sell_volume_usd, 2) as estimated_sell_volume_usd,
      ROUND(estimated_return_pct, 2) as estimated_return_pct,
      ROUND(activity_score, 2) as activity_score,
      ROUND(diversification_score, 2) as diversification_score,
      ROUND(gas_efficiency_score, 3) as gas_efficiency_score,
      
      -- Overall elite score
      ROUND(
        (CASE WHEN estimated_return_pct > 0 THEN LEAST(estimated_return_pct / 50.0, 1.0) ELSE 0 END * 40) +  -- 40% performance
        (activity_score / 10.0 * 25) +  -- 25% activity
        (diversification_score * 20) +  -- 20% diversification
        (gas_efficiency_score * 15)     -- 15% gas efficiency
      , 2) as elite_score,
      
      -- Wallet tier
      CASE 
        WHEN wallet_sophistication_score < 180 THEN 'Elite'
        WHEN wallet_sophistication_score < 220 THEN 'High'
        ELSE 'Medium'
      END as wallet_tier,
      
      first_recent_activity,
      last_activity
      
    FROM performance_scores
    ORDER BY elite_score DESC, wallet_sophistication_score ASC
    LIMIT {limit}
    """
    
    def _execute_query():
        try:
            results = self.client.query(query).result()
            leaderboard = []
            
            for i, row in enumerate(results, 1):
                wallet_data = dict(row)
                wallet_data['rank'] = i
                
                # Convert datetime fields
                for field in ['first_recent_activity', 'last_activity']:
                    if wallet_data.get(field):
                        wallet_data[field] = wallet_data[field].isoformat()
                
                leaderboard.append(wallet_data)
            
            return leaderboard
            
        except Exception as e:
            logger.error(f"Error getting elite wallet leaderboard: {e}")
            return []
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(self.executor, _execute_query)

def __del__(self):
    """Cleanup on deletion"""
    try:
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=False)
    except Exception as e:
        logger.warning(f"Error shutting down executor: {e}")
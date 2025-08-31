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
            dataset.default_table_expiration_ms = 90 * 24 * 60 * 60 * 1000  # 90 days
            
            self.client.create_dataset(dataset)
            logger.info(f"Created dataset {self.dataset_ref}")
    
    def _create_all_tables(self):
        """Create all necessary tables"""
        
        # Define table schemas
        table_schemas = {
            "transactions": [
                bigquery.SchemaField("transaction_hash", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("block_number", "INTEGER"),
                bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("wallet_address", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("token_address", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("token_symbol", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("token_amount", "FLOAT", mode="REQUIRED"),
                bigquery.SchemaField("transfer_type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("cost_in_eth", "FLOAT"),
                bigquery.SchemaField("platform", "STRING"),
                bigquery.SchemaField("network", "STRING"),
                bigquery.SchemaField("wallet_sophistication_score", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED")
            ],
            
            "token_prices": [
                bigquery.SchemaField("token_address", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("token_symbol", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("price_usd", "FLOAT", mode="REQUIRED"),
                bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("source", "STRING")
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
                bigquery.SchemaField("score_delta", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("calculated_at", "TIMESTAMP", mode="REQUIRED")
            ],
            
            "wallet_score_updates": [
                bigquery.SchemaField("wallet_address", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("trades_analyzed", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("total_score_change", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("avg_profit_pct", "FLOAT"),
                bigquery.SchemaField("winning_trades", "INTEGER"),
                bigquery.SchemaField("losing_trades", "INTEGER"),
                bigquery.SchemaField("win_rate", "FLOAT"),
                bigquery.SchemaField("current_score", "INTEGER"),
                bigquery.SchemaField("suggested_new_score", "INTEGER"),
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
                bigquery.SchemaField("recommendation_message", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("priority", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("update_calculated_at", "TIMESTAMP", mode="REQUIRED")
            ]
        }
        
        # Create each table with partitioning and clustering
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
        
        # Add partitioning based on table type
        if table_name in ["transactions", "token_prices"]:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="timestamp"
            )
            if table_name == "transactions":
                table.clustering_fields = ["wallet_address", "token_address", "transfer_type"]
            else:
                table.clustering_fields = ["token_address", "source"]
        
        elif table_name == "matched_trades":
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="buy_time"
            )
            table.clustering_fields = ["wallet_address", "token_address"]
        
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
          token_symbol as symbol
        FROM `{self.dataset_ref}.transactions`
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
          AND token_address != '0x0000000000000000000000000000000000000000'
          AND token_address IS NOT NULL
          AND token_symbol IS NOT NULL
        ORDER BY token_symbol
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
                processed_data = []
                for price in price_data:
                    processed_price = {
                        'token_address': price['token_address'].lower(),
                        'token_symbol': price['token_symbol'].upper(),
                        'price_usd': float(price.get('price_usd', 0)),
                        'timestamp': price.get('timestamp', datetime.utcnow()),
                        'source': price.get('source', 'unknown')
                    }
                    processed_data.append(processed_price)
                
                if not processed_data:
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
            return True
        
        def _insert_data():
            try:
                processed_transactions = []
                for txn in transactions:
                    try:
                        processed_txn = {
                            'transaction_hash': txn['transaction_hash'],
                            'block_number': int(txn.get('block_number', 0)),
                            'timestamp': txn.get('timestamp', datetime.utcnow()),
                            'wallet_address': txn['wallet_address'].lower(),
                            'token_address': txn['token_address'].lower(),
                            'token_symbol': txn['token_symbol'].upper(),
                            'token_amount': float(txn.get('token_amount', 0)),
                            'transfer_type': txn['transfer_type'].lower(),
                            'cost_in_eth': float(txn.get('cost_in_eth', 0)),
                            'platform': txn.get('platform', 'unknown'),
                            'network': txn.get('network', 'ethereum'),
                            'wallet_sophistication_score': int(txn['wallet_sophistication_score']),
                            'created_at': datetime.utcnow()
                        }
                        processed_transactions.append(processed_txn)
                        
                    except (ValueError, KeyError, TypeError) as e:
                        logger.warning(f"Error processing transaction {txn}: {e}")
                        continue
                
                if not processed_transactions:
                    return True
                
                table_ref = f"{self.dataset_ref}.transactions"
                errors = self.client.insert_rows_json(table_ref, processed_transactions)
                
                if errors:
                    logger.error(f"Error inserting transactions: {errors}")
                    return False
                
                logger.info(f"Successfully inserted {len(processed_transactions)} transactions")
                return True
                
            except Exception as e:
                logger.error(f"Error inserting transactions: {e}")
                return False
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, _insert_data)
    
    async def execute_query(self, query: str) -> bigquery.QueryJob:
        """Execute a BigQuery query asynchronously"""
        def _execute():
            try:
                job = self.client.query(query)
                job.result()  # Wait for completion
                logger.info(f"Query completed successfully")
                return job
            except Exception as e:
                logger.error(f"Error executing query: {e}")
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
            wallet_sophistication_score,
            COUNT(*) as total_transactions,
            COUNT(DISTINCT token_address) as unique_tokens,
            SUM(CASE WHEN transfer_type = 'buy' THEN 1 ELSE 0 END) as buy_count,
            SUM(CASE WHEN transfer_type = 'sell' THEN 1 ELSE 0 END) as sell_count,
            SUM(cost_in_eth) as total_gas_cost,
            MIN(timestamp) as first_transaction,
            MAX(timestamp) as last_transaction
          FROM `{self.dataset_ref}.transactions`
          WHERE wallet_address = '{wallet_address}'
            AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
          GROUP BY wallet_sophistication_score
        )
        
        SELECT 
          *,
          DATE_DIFF(DATE(last_transaction), DATE(first_transaction), DAY) as active_days,
          CASE 
            WHEN wallet_sophistication_score < 180 THEN 'Elite'
            WHEN wallet_sophistication_score < 220 THEN 'High'
            WHEN wallet_sophistication_score < 260 THEN 'Medium'
            ELSE 'Low'
          END as wallet_tier
        FROM wallet_transactions
        """
        
        def _execute_query():
            try:
                results = list(self.client.query(query).result())
                if results:
                    analysis = dict(results[0])
                    for field in ['first_transaction', 'last_transaction']:
                        if analysis.get(field):
                            analysis[field] = analysis[field].isoformat()
                    return analysis
                else:
                    return {"error": "Wallet not found or no recent activity"}
            except Exception as e:
                logger.error(f"Error analyzing wallet {wallet_address}: {e}")
                return {"error": str(e)}
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, _execute_query)
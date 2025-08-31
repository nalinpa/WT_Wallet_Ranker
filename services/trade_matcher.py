import logging
from typing import Optional
from datetime import datetime
from .bigquery_client import BigQueryClient

logger = logging.getLogger(__name__)

class TradeMatcher:
    def __init__(self, bq_client: BigQueryClient):
        self.bq_client = bq_client
    
    async def create_matched_trades_table(self, days_back: int = 7) -> int:
        """Create matched trades table and return count of matches"""
        
        query = f"""
        CREATE OR REPLACE TABLE `{self.bq_client.dataset_ref}.matched_trades` AS
        WITH buy_sell_pairs AS (
          SELECT 
            buy.wallet_address,
            buy.token_address,
            buy.token_symbol,
            buy.transaction_hash as buy_hash,
            sell.transaction_hash as sell_hash,
            buy.timestamp as buy_time,
            sell.timestamp as sell_time,
            buy.token_amount as buy_amount,
            sell.token_amount as sell_amount,
            buy.cost_in_eth as buy_gas,
            sell.cost_in_eth as sell_gas,
            
            -- Get prices at buy and sell times (closest match within 30 minutes)
            buy_price.price_usd as buy_price,
            sell_price.price_usd as sell_price,
            
            -- Calculate holding period
            TIMESTAMP_DIFF(sell.timestamp, buy.timestamp, HOUR) as hold_hours,
            
            -- FIFO matching
            ROW_NUMBER() OVER (
              PARTITION BY buy.wallet_address, buy.token_address 
              ORDER BY buy.timestamp
            ) as buy_seq,
            ROW_NUMBER() OVER (
              PARTITION BY sell.wallet_address, sell.token_address 
              ORDER BY sell.timestamp
            ) as sell_seq
            
          FROM `{self.bq_client.dataset_ref}.transactions` buy
          JOIN `{self.bq_client.dataset_ref}.transactions` sell
            ON buy.wallet_address = sell.wallet_address
            AND buy.token_address = sell.token_address
            AND buy.transfer_type = 'buy'
            AND sell.transfer_type = 'sell'
            AND sell.timestamp > buy.timestamp
            AND buy.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
            
          -- Join with price data
          LEFT JOIN LATERAL (
            SELECT price_usd
            FROM `{self.bq_client.dataset_ref}.token_prices` p
            WHERE p.token_address = buy.token_address
              AND ABS(TIMESTAMP_DIFF(buy.timestamp, p.timestamp, MINUTE)) <= 30
            ORDER BY ABS(TIMESTAMP_DIFF(buy.timestamp, p.timestamp, MINUTE))
            LIMIT 1
          ) buy_price
          
          LEFT JOIN LATERAL (
            SELECT price_usd
            FROM `{self.bq_client.dataset_ref}.token_prices` p
            WHERE p.token_address = sell.token_address
              AND ABS(TIMESTAMP_DIFF(sell.timestamp, p.timestamp, MINUTE)) <= 30
            ORDER BY ABS(TIMESTAMP_DIFF(sell.timestamp, p.timestamp, MINUTE))
            LIMIT 1
          ) sell_price
        ),
        
        final_matched_trades AS (
          SELECT 
            wallet_address,
            token_address,
            token_symbol,
            buy_hash,
            sell_hash,
            buy_time,
            sell_time,
            buy_amount,
            sell_amount,
            buy_price,
            sell_price,
            hold_hours,
            
            -- Calculate performance metrics
            CASE 
              WHEN buy_price > 0 AND sell_price > 0 THEN
                ((sell_price - buy_price) / buy_price) * 100
              ELSE NULL
            END as price_change_percent,
            
            CASE 
              WHEN buy_price > 0 AND sell_price > 0 THEN
                (sell_amount * sell_price) - (buy_amount * buy_price) - 
                ((buy_gas + sell_gas) * 2500) -- Approximate ETH price
              ELSE NULL
            END as profit_usd,
            
            CASE 
              WHEN buy_price > 0 THEN buy_amount * buy_price
              ELSE NULL
            END as trade_volume_usd,
            
            -- Trade type classification
            CASE 
              WHEN hold_hours < 1 THEN 'Scalp'
              WHEN hold_hours < 24 THEN 'Day Trade'  
              WHEN hold_hours < 168 THEN 'Swing Trade'
              ELSE 'Position Trade'
            END as trade_type,
            
            CURRENT_TIMESTAMP() as created_at
            
          FROM buy_sell_pairs
          WHERE buy_seq = sell_seq  -- FIFO matching
            AND buy_price IS NOT NULL 
            AND sell_price IS NOT NULL
            AND buy_price > 0
            AND sell_price > 0
            AND hold_hours >= 0
        )
        
        SELECT * FROM final_matched_trades
        ORDER BY wallet_address, buy_time
        """
        
        try:
            await self.bq_client.execute_query(query)
            
            # Get count of matched trades
            count_query = f"""
            SELECT COUNT(*) as trade_count
            FROM `{self.bq_client.dataset_ref}.matched_trades`
            """
            
            count_result = await self.bq_client.execute_query(count_query)
            count_rows = list(count_result.result())
            trade_count = count_rows[0]['trade_count'] if count_rows else 0
            
            logger.info(f"Created matched_trades table with {trade_count} trade pairs")
            return trade_count
            
        except Exception as e:
            logger.error(f"Error creating matched trades table: {e}")
            raise e
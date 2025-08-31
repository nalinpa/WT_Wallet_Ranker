import logging
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
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
            buy.platform as buy_platform,
            sell.platform as sell_platform,
            
            -- Get prices at buy and sell times (closest match within 30 minutes)
            buy_price.price_usd as buy_price,
            sell_price.price_usd as sell_price,
            buy_price.source as buy_price_source,
            sell_price.source as sell_price_source,
            
            -- Calculate holding period
            TIMESTAMP_DIFF(sell.timestamp, buy.timestamp, HOUR) as hold_hours,
            TIMESTAMP_DIFF(sell.timestamp, buy.timestamp, MINUTE) as hold_minutes,
            
            -- Row numbers for FIFO matching (First In, First Out)
            ROW_NUMBER() OVER (
              PARTITION BY buy.wallet_address, buy.token_address 
              ORDER BY buy.timestamp
            ) as buy_seq,
            ROW_NUMBER() OVER (
              PARTITION BY sell.wallet_address, sell.token_address 
              ORDER BY sell.timestamp
            ) as sell_seq,
            
            -- Additional context
            buy.wallet_sophistication_score as buy_sophistication,
            sell.wallet_sophistication_score as sell_sophistication
            
          FROM `{self.bq_client.dataset_ref}.transactions` buy
          JOIN `{self.bq_client.dataset_ref}.transactions` sell
            ON buy.wallet_address = sell.wallet_address
            AND buy.token_address = sell.token_address
            AND buy.transfer_type = 'buy'
            AND sell.transfer_type = 'sell'
            AND sell.timestamp > buy.timestamp
            AND buy.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
            
          -- Join with price data (closest timestamp match within 30 minutes)
          LEFT JOIN LATERAL (
            SELECT price_usd, source
            FROM `{self.bq_client.dataset_ref}.token_prices` p
            WHERE p.token_address = buy.token_address
              AND ABS(TIMESTAMP_DIFF(buy.timestamp, p.timestamp, MINUTE)) <= 30
            ORDER BY ABS(TIMESTAMP_DIFF(buy.timestamp, p.timestamp, MINUTE))
            LIMIT 1
          ) buy_price
          
          LEFT JOIN LATERAL (
            SELECT price_usd, source  
            FROM `{self.bq_client.dataset_ref}.token_prices` p
            WHERE p.token_address = sell.token_address
              AND ABS(TIMESTAMP_DIFF(sell.timestamp, p.timestamp, MINUTE)) <= 30
            ORDER BY ABS(TIMESTAMP_DIFF(sell.timestamp, p.timestamp, MINUTE))
            LIMIT 1
          ) sell_price
        ),
        
        matched_with_metrics AS (
          SELECT 
            *,
            -- Calculate profit metrics
            CASE 
              WHEN buy_price > 0 AND sell_price > 0 THEN
                ((sell_price - buy_price) / buy_price) * 100
              ELSE NULL
            END as price_change_percent,
            
            CASE 
              WHEN buy_price > 0 AND sell_price > 0 THEN
                (sell_amount * sell_price) - (buy_amount * buy_price) - 
                ((buy_gas + sell_gas) * 2500) -- Approximate ETH price in USD
              ELSE NULL
            END as profit_usd,
            
            -- Gross profit before gas
            CASE 
              WHEN buy_price > 0 AND sell_price > 0 THEN
                (sell_amount * sell_price) - (buy_amount * buy_price)
              ELSE NULL
            END as gross_profit_usd,
            
            -- Trade volume in USD
            CASE 
              WHEN buy_price > 0 THEN buy_amount * buy_price
              ELSE NULL
            END as trade_volume_usd,
            
            -- Gas cost in USD
            (buy_gas + sell_gas) * 2500 as gas_cost_usd,
            
            -- Gas efficiency ratio
            CASE 
              WHEN buy_price > 0 AND buy_amount * buy_price > 0 THEN
                ((buy_gas + sell_gas) * 2500) / (buy_amount * buy_price)
              ELSE NULL
            END as gas_efficiency_ratio,
            
            -- Trade quality assessment based on holding period
            CASE 
              WHEN hold_hours < 1 THEN 'Scalp'
              WHEN hold_hours < 24 THEN 'Day Trade'  
              WHEN hold_hours < 168 THEN 'Swing Trade'
              ELSE 'Position Trade'
            END as trade_type,
            
            -- Platform analysis
            CASE 
              WHEN buy_platform = sell_platform THEN buy_platform
              ELSE CONCAT(buy_platform, ' → ', sell_platform)
            END as platform_flow,
            
            -- Price data quality
            CASE 
              WHEN buy_price IS NOT NULL AND sell_price IS NOT NULL THEN 'Complete'
              WHEN buy_price IS NOT NULL OR sell_price IS NOT NULL THEN 'Partial'
              ELSE 'Missing'
            END as price_data_quality
            
          FROM buy_sell_pairs
          WHERE buy_seq = sell_seq  -- FIFO matching ensures correct pairing
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
            buy_gas,
            sell_gas,
            buy_price,
            sell_price,
            buy_price_source,
            sell_price_source,
            buy_platform,
            sell_platform,
            platform_flow,
            hold_hours,
            hold_minutes,
            price_change_percent,
            profit_usd,
            gross_profit_usd,
            trade_volume_usd,
            gas_cost_usd,
            gas_efficiency_ratio,
            trade_type,
            price_data_quality,
            
            -- Additional derived metrics
            CASE 
              WHEN profit_usd IS NOT NULL AND profit_usd != 0 THEN
                ABS(gas_cost_usd / profit_usd)
              ELSE NULL
            END as gas_to_profit_ratio,
            
            -- Return on investment
            CASE 
              WHEN trade_volume_usd > 0 THEN (profit_usd / trade_volume_usd) * 100
              ELSE NULL
            END as roi_percent,
            
            -- Annualized return (extrapolated)
            CASE 
              WHEN hold_hours > 0 AND trade_volume_usd > 0 THEN
                (profit_usd / trade_volume_usd) * (8760 / hold_hours) * 100  -- 8760 hours in a year
              ELSE NULL
            END as annualized_return_percent,
            
            -- Trade efficiency score (0-1, higher is better)
            CASE 
              WHEN price_change_percent IS NOT NULL AND gas_efficiency_ratio IS NOT NULL THEN
                GREATEST(0, LEAST(1, 
                  (CASE WHEN price_change_percent > 0 THEN 1 ELSE 0 END * 0.6) +  -- 60% for profitability
                  (GREATEST(0, LEAST(1, 1 - gas_efficiency_ratio)) * 0.4)  -- 40% for gas efficiency
                ))
              ELSE NULL
            END as trade_efficiency_score,
            
            CURRENT_TIMESTAMP() as created_at,
            
            -- Wallet sophistication at trade time (using buy-side score)
            buy_sophistication as wallet_sophistication_score
            
          FROM matched_with_metrics
          WHERE buy_price IS NOT NULL 
            AND sell_price IS NOT NULL
            AND buy_price > 0
            AND sell_price > 0
            AND hold_hours >= 0  -- Ensure valid holding period
            AND ABS(price_change_percent) <= 10000  -- Filter out extreme price changes (likely data errors)
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
    
    async def create_advanced_matched_trades(self, days_back: int = 7, min_hold_minutes: int = 1) -> int:
        """Create advanced matched trades with partial position matching"""
        
        query = f"""
        CREATE OR REPLACE TABLE `{self.bq_client.dataset_ref}.advanced_matched_trades` AS
        WITH wallet_positions AS (
          -- First, get all buy and sell transactions
          SELECT 
            wallet_address,
            token_address,
            token_symbol,
            transaction_hash,
            timestamp,
            token_amount,
            transfer_type,
            cost_in_eth,
            platform,
            wallet_sophistication_score,
            
            -- Running balance calculation
            SUM(CASE WHEN transfer_type = 'buy' THEN token_amount ELSE -token_amount END) 
              OVER (PARTITION BY wallet_address, token_address ORDER BY timestamp 
                    ROWS UNBOUNDED PRECEDING) as running_balance,
            
            -- Previous balance
            COALESCE(
              SUM(CASE WHEN transfer_type = 'buy' THEN token_amount ELSE -token_amount END) 
                OVER (PARTITION BY wallet_address, token_address ORDER BY timestamp 
                      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0
            ) as prev_balance
            
          FROM `{self.bq_client.dataset_ref}.transactions`
          WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
          ORDER BY wallet_address, token_address, timestamp
        ),
        
        position_changes AS (
          -- Identify significant position changes
          SELECT 
            *,
            -- Determine if this is opening, adding to, reducing, or closing a position
            CASE 
              WHEN prev_balance = 0 AND transfer_type = 'buy' THEN 'Open Long'
              WHEN prev_balance > 0 AND transfer_type = 'buy' THEN 'Add Long'
              WHEN prev_balance > 0 AND transfer_type = 'sell' AND running_balance > 0 THEN 'Reduce Long'
              WHEN prev_balance > 0 AND transfer_type = 'sell' AND running_balance = 0 THEN 'Close Long'
              WHEN prev_balance > 0 AND transfer_type = 'sell' AND running_balance < 0 THEN 'Long to Short'
              WHEN prev_balance = 0 AND transfer_type = 'sell' THEN 'Open Short'
              WHEN prev_balance < 0 AND transfer_type = 'sell' THEN 'Add Short'
              WHEN prev_balance < 0 AND transfer_type = 'buy' AND running_balance < 0 THEN 'Cover Short'
              WHEN prev_balance < 0 AND transfer_type = 'buy' AND running_balance = 0 THEN 'Close Short'
              WHEN prev_balance < 0 AND transfer_type = 'buy' AND running_balance > 0 THEN 'Short to Long'
              ELSE 'Other'
            END as position_action,
            
            -- Calculate position size change
            ABS(running_balance - prev_balance) as position_change,
            ABS(running_balance) as abs_position_size,
            ABS(prev_balance) as prev_abs_position_size
            
          FROM wallet_positions
        ),
        
        trade_pairs AS (
          -- Match opens with closes
          SELECT 
            open_pos.wallet_address,
            open_pos.token_address,
            open_pos.token_symbol,
            
            -- Open position details
            open_pos.transaction_hash as open_hash,
            open_pos.timestamp as open_time,
            open_pos.token_amount as open_amount,
            open_pos.transfer_type as open_type,
            open_pos.cost_in_eth as open_gas,
            open_pos.platform as open_platform,
            open_pos.wallet_sophistication_score,
            
            -- Close position details
            close_pos.transaction_hash as close_hash,
            close_pos.timestamp as close_time,
            close_pos.token_amount as close_amount,
            close_pos.transfer_type as close_type,
            close_pos.cost_in_eth as close_gas,
            close_pos.platform as close_platform,
            
            -- Position details
            open_pos.position_action as open_action,
            close_pos.position_action as close_action,
            
            -- Time calculations
            TIMESTAMP_DIFF(close_pos.timestamp, open_pos.timestamp, MINUTE) as hold_minutes,
            TIMESTAMP_DIFF(close_pos.timestamp, open_pos.timestamp, HOUR) as hold_hours,
            
            -- Match quality score
            CASE 
              WHEN open_pos.position_action IN ('Open Long', 'Add Long') 
                   AND close_pos.position_action IN ('Close Long', 'Reduce Long') THEN 1.0
              WHEN open_pos.position_action IN ('Open Short', 'Add Short') 
                   AND close_pos.position_action IN ('Close Short', 'Cover Short') THEN 1.0
              WHEN open_pos.position_action LIKE '%Long%' 
                   AND close_pos.position_action LIKE '%Long%' THEN 0.8
              WHEN open_pos.position_action LIKE '%Short%' 
                   AND close_pos.position_action LIKE '%Short%' THEN 0.8
              ELSE 0.5
            END as match_quality_score
            
          FROM position_changes open_pos
          JOIN position_changes close_pos
            ON open_pos.wallet_address = close_pos.wallet_address
            AND open_pos.token_address = close_pos.token_address
            AND close_pos.timestamp > open_pos.timestamp
            AND TIMESTAMP_DIFF(close_pos.timestamp, open_pos.timestamp, MINUTE) >= {min_hold_minutes}
            
          -- Match opening actions with closing actions
          WHERE (
            (open_pos.position_action IN ('Open Long', 'Add Long') 
             AND close_pos.position_action IN ('Close Long', 'Reduce Long', 'Long to Short'))
            OR
            (open_pos.position_action IN ('Open Short', 'Add Short') 
             AND close_pos.position_action IN ('Close Short', 'Cover Short', 'Short to Long'))
          )
        ),
        
        trades_with_prices AS (
          -- Add price data to matched trades
          SELECT 
            tp.*,
            
            -- Get prices at open and close times
            open_price.price_usd as open_price,
            close_price.price_usd as close_price,
            open_price.source as open_price_source,
            close_price.source as close_price_source,
            
            -- Price data quality
            CASE 
              WHEN open_price.price_usd IS NOT NULL AND close_price.price_usd IS NOT NULL THEN 'Complete'
              WHEN open_price.price_usd IS NOT NULL OR close_price.price_usd IS NOT NULL THEN 'Partial'
              ELSE 'Missing'
            END as price_data_quality
            
          FROM trade_pairs tp
          
          -- Join with price data for open time
          LEFT JOIN LATERAL (
            SELECT price_usd, source
            FROM `{self.bq_client.dataset_ref}.token_prices` p
            WHERE p.token_address = tp.token_address
              AND ABS(TIMESTAMP_DIFF(tp.open_time, p.timestamp, MINUTE)) <= 30
            ORDER BY ABS(TIMESTAMP_DIFF(tp.open_time, p.timestamp, MINUTE))
            LIMIT 1
          ) open_price
          
          -- Join with price data for close time
          LEFT JOIN LATERAL (
            SELECT price_usd, source  
            FROM `{self.bq_client.dataset_ref}.token_prices` p
            WHERE p.token_address = tp.token_address
              AND ABS(TIMESTAMP_DIFF(tp.close_time, p.timestamp, MINUTE)) <= 30
            ORDER BY ABS(TIMESTAMP_DIFF(tp.close_time, p.timestamp, MINUTE))
            LIMIT 1
          ) close_price
          
          WHERE open_price.price_usd IS NOT NULL 
            AND close_price.price_usd IS NOT NULL
            AND open_price.price_usd > 0
            AND close_price.price_usd > 0
        ),
        
        final_advanced_trades AS (
          SELECT 
            wallet_address,
            token_address,
            token_symbol,
            open_hash,
            close_hash,
            open_time,
            close_time,
            open_amount,
            close_amount,
            open_gas,
            close_gas,
            open_price,
            close_price,
            open_price_source,
            close_price_source,
            open_platform,
            close_platform,
            open_action,
            close_action,
            hold_minutes,
            hold_hours,
            match_quality_score,
            price_data_quality,
            wallet_sophistication_score,
            
            -- Calculate performance metrics
            CASE 
              WHEN open_price > 0 AND close_price > 0 THEN
                CASE 
                  WHEN open_action LIKE '%Long%' THEN
                    ((close_price - open_price) / open_price) * 100
                  WHEN open_action LIKE '%Short%' THEN
                    ((open_price - close_price) / open_price) * 100
                  ELSE 0
                END
              ELSE NULL
            END as price_change_percent,
            
            -- Profit calculation
            CASE 
              WHEN open_price > 0 AND close_price > 0 THEN
                CASE 
                  WHEN open_action LIKE '%Long%' THEN
                    (LEAST(open_amount, close_amount) * (close_price - open_price)) - 
                    ((open_gas + close_gas) * 2500)
                  WHEN open_action LIKE '%Short%' THEN
                    (LEAST(open_amount, close_amount) * (open_price - close_price)) - 
                    ((open_gas + close_gas) * 2500)
                  ELSE 0
                END
              ELSE NULL
            END as profit_usd,
            
            -- Trade volume
            CASE 
              WHEN open_price > 0 THEN 
                LEAST(open_amount, close_amount) * open_price
              ELSE NULL
            END as trade_volume_usd,
            
            -- Gas costs
            (open_gas + close_gas) * 2500 as gas_cost_usd,
            
            -- Trade type classification
            CASE 
              WHEN hold_hours < 1 THEN 'Scalp'
              WHEN hold_hours < 24 THEN 'Day Trade'  
              WHEN hold_hours < 168 THEN 'Swing Trade'
              ELSE 'Position Trade'
            END as trade_type,
            
            -- Strategy classification
            CASE 
              WHEN open_action LIKE '%Long%' THEN 'Long'
              WHEN open_action LIKE '%Short%' THEN 'Short'
              ELSE 'Complex'
            END as strategy_type,
            
            CURRENT_TIMESTAMP() as created_at
            
          FROM trades_with_prices
          WHERE match_quality_score >= 0.5  -- Only include reasonable matches
        )
        
        SELECT * FROM final_advanced_trades
        ORDER BY wallet_address, open_time
        """
        
        try:
            await self.bq_client.execute_query(query)
            
            # Get count
            count_query = f"""
            SELECT COUNT(*) as advanced_trade_count
            FROM `{self.bq_client.dataset_ref}.advanced_matched_trades`
            """
            
            count_result = await self.bq_client.execute_query(count_query)
            count_rows = list(count_result.result())
            trade_count = count_rows[0]['advanced_trade_count'] if count_rows else 0
            
            logger.info(f"Created advanced_matched_trades table with {trade_count} position-based trade pairs")
            return trade_count
            
        except Exception as e:
            logger.error(f"Error creating advanced matched trades table: {e}")
            raise e
    
    async def analyze_matching_quality(self, days_back: int = 7) -> Dict[str, Any]:
        """Analyze the quality of trade matching results"""
        
        quality_query = f"""
        WITH matching_stats AS (
          SELECT 
            COUNT(*) as total_matched_trades,
            COUNT(CASE WHEN price_data_quality = 'Complete' THEN 1 END) as complete_price_data,
            COUNT(CASE WHEN price_data_quality = 'Partial' THEN 1 END) as partial_price_data,
            COUNT(CASE WHEN price_data_quality = 'Missing' THEN 1 END) as missing_price_data,
            
            -- Trade type distribution
            COUNT(CASE WHEN trade_type = 'Scalp' THEN 1 END) as scalp_trades,
            COUNT(CASE WHEN trade_type = 'Day Trade' THEN 1 END) as day_trades,
            COUNT(CASE WHEN trade_type = 'Swing Trade' THEN 1 END) as swing_trades,
            COUNT(CASE WHEN trade_type = 'Position Trade' THEN 1 END) as position_trades,
            
            -- Performance distribution
            COUNT(CASE WHEN price_change_percent > 0 THEN 1 END) as profitable_trades,
            COUNT(CASE WHEN price_change_percent <= 0 THEN 1 END) as unprofitable_trades,
            
            -- Holding period stats
            AVG(hold_hours) as avg_hold_hours,
            MIN(hold_hours) as min_hold_hours,
            MAX(hold_hours) as max_hold_hours,
            
            -- Profit metrics
            AVG(price_change_percent) as avg_price_change_pct,
            AVG(CASE WHEN profit_usd IS NOT NULL THEN profit_usd ELSE 0 END) as avg_profit_usd,
            
            -- Gas efficiency
            AVG(gas_efficiency_ratio) as avg_gas_efficiency_ratio,
            COUNT(CASE WHEN gas_efficiency_ratio > 0.05 THEN 1 END) as high_gas_cost_trades,
            
            -- Data quality metrics
            COUNT(CASE WHEN ABS(price_change_percent) > 1000 THEN 1 END) as extreme_price_changes,
            COUNT(CASE WHEN hold_minutes < 1 THEN 1 END) as very_short_holds,
            COUNT(DISTINCT wallet_address) as unique_wallets,
            COUNT(DISTINCT token_address) as unique_tokens
            
          FROM `{self.bq_client.dataset_ref}.matched_trades`
          WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        ),
        
        wallet_matching_stats AS (
          SELECT 
            wallet_address,
            COUNT(*) as trades_matched,
            AVG(price_change_percent) as wallet_avg_performance,
            COUNT(CASE WHEN price_change_percent > 0 THEN 1 END) as profitable_count,
            AVG(hold_hours) as avg_hold_time,
            COUNT(DISTINCT token_address) as tokens_traded
          FROM `{self.bq_client.dataset_ref}.matched_trades`
          WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
          GROUP BY wallet_address
          ORDER BY trades_matched DESC
          LIMIT 10
        ),
        
        token_matching_stats AS (
          SELECT 
            token_symbol,
            COUNT(*) as trades_matched,
            COUNT(DISTINCT wallet_address) as unique_wallets,
            AVG(price_change_percent) as token_avg_performance,
            COUNT(CASE WHEN price_data_quality = 'Complete' THEN 1 END) as complete_price_data_count
          FROM `{self.bq_client.dataset_ref}.matched_trades`
          WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
          GROUP BY token_symbol
          ORDER BY trades_matched DESC
          LIMIT 10
        )
        
        SELECT 
          'overall_stats' as analysis_type,
          TO_JSON_STRING(STRUCT(
            total_matched_trades,
            complete_price_data,
            partial_price_data,
            missing_price_data,
            scalp_trades,
            day_trades,
            swing_trades,
            position_trades,
            profitable_trades,
            unprofitable_trades,
            avg_hold_hours,
            min_hold_hours,
            max_hold_hours,
            avg_price_change_pct,
            avg_profit_usd,
            avg_gas_efficiency_ratio,
            high_gas_cost_trades,
            extreme_price_changes,
            very_short_holds,
            unique_wallets,
            unique_tokens
          )) as data
        FROM matching_stats
        
        UNION ALL
        
        SELECT 
          'wallet_stats' as analysis_type,
          TO_JSON_STRING(ARRAY_AGG(STRUCT(
            wallet_address,
            trades_matched,
            wallet_avg_performance,
            profitable_count,
            avg_hold_time,
            tokens_traded
          ))) as data
        FROM wallet_matching_stats
        
        UNION ALL
        
        SELECT 
          'token_stats' as analysis_type,
          TO_JSON_STRING(ARRAY_AGG(STRUCT(
            token_symbol,
            trades_matched,
            unique_wallets,
            token_avg_performance,
            complete_price_data_count
          ))) as data
        FROM token_matching_stats
        """
        
        def _execute_query():
            try:
                results = list(self.bq_client.client.query(quality_query).result())
                analysis = {}
                
                for row in results:
                    analysis_type = row['analysis_type']
                    data = json.loads(row['data']) if row['data'] else {}
                    analysis[analysis_type] = data
                
                # Calculate quality metrics
                overall_stats = analysis.get('overall_stats', {})
                if overall_stats:
                    total_trades = overall_stats.get('total_matched_trades', 0)
                    complete_data = overall_stats.get('complete_price_data', 0)
                    extreme_changes = overall_stats.get('extreme_price_changes', 0)
                    
                    analysis['quality_metrics'] = {
                        'data_completeness_pct': (complete_data / total_trades * 100) if total_trades > 0 else 0,
                        'data_quality_score': max(0, min(100, 
                            100 - (extreme_changes / total_trades * 100) if total_trades > 0 else 100
                        )),
                        'matching_efficiency': (total_trades / analysis.get('unique_wallets', 1)) if analysis.get('unique_wallets') else 0,
                        'analysis_timestamp': datetime.utcnow().isoformat()
                    }
                
                return analysis
                
            except Exception as e:
                logger.error(f"Error analyzing matching quality: {e}")
                return {"error": str(e)}
        
        import asyncio
        from datetime import datetime
        import json
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.bq_client.executor, _execute_query)
    
async def get_unmatched_transactions(self, days_back: int = 7, limit: int = 100) -> List[Dict[str, Any]]:
    """Get transactions that couldn't be matched with counterparts"""
    
    unmatched_query = f"""
    WITH matched_hashes AS (
      SELECT buy_hash as transaction_hash FROM `{self.bq_client.dataset_ref}.matched_trades`
      UNION DISTINCT
      SELECT sell_hash as transaction_hash FROM `{self.bq_client.dataset_ref}.matched_trades`
    )
    
    SELECT 
      t.transaction_hash,
      t.wallet_address,
      t.token_address,
      t.token_symbol,
      t.transfer_type,
      t.token_amount,
      t.timestamp,
      t.cost_in_eth,
      t.platform,
      t.wallet_sophistication_score,
      
      -- Potential reasons for no match
      CASE 
        WHEN t.transfer_type = 'buy' AND NOT EXISTS (
          SELECT 1 FROM `{self.bq_client.dataset_ref}.transactions` t2 
          WHERE t2.wallet_address = t.wallet_address 
            AND t2.token_address = t.token_address 
            AND t2.transfer_type = 'sell' 
            AND t2.timestamp > t.timestamp
        ) THEN 'No subsequent sell'
        
        WHEN t.transfer_type = 'sell' AND NOT EXISTS (
          SELECT 1 FROM `{self.bq_client.dataset_ref}.transactions` t2 
          WHERE t2.wallet_address = t.wallet_address 
            AND t2.token_address = t.token_address 
            AND t2.transfer_type = 'buy' 
            AND t2.timestamp < t.timestamp
        ) THEN 'No prior buy'
        
        WHEN NOT EXISTS (
          SELECT 1 FROM `{self.bq_client.dataset_ref}.token_prices` p
          WHERE p.token_address = t.token_address
            AND ABS(TIMESTAMP_DIFF(t.timestamp, p.timestamp, MINUTE)) <= 30
        ) THEN 'No price data'
        
        WHEN t.transfer_type = 'buy' AND EXISTS (
          SELECT 1 FROM `{self.bq_client.dataset_ref}.transactions` t2 
          WHERE t2.wallet_address = t.wallet_address 
            AND t2.token_address = t.token_address 
            AND t2.transfer_type = 'sell' 
            AND t2.timestamp > t.timestamp
            AND TIMESTAMP_DIFF(t2.timestamp, t.timestamp, HOUR) > 8760  -- > 1 year
        ) THEN 'Sell too far in future'
        
        WHEN t.transfer_type = 'sell' AND EXISTS (
          SELECT 1 FROM `{self.bq_client.dataset_ref}.transactions` t2 
          WHERE t2.wallet_address = t.wallet_address 
            AND t2.token_address = t.token_address 
            AND t2.transfer_type = 'buy' 
            AND t2.timestamp < t.timestamp
            AND TIMESTAMP_DIFF(t.timestamp, t2.timestamp, HOUR) > 8760  -- > 1 year
        ) THEN 'Buy too far in past'
        
        ELSE 'Complex position or timing mismatch'
      END as unmatch_reason,
      
      -- Check for price data availability
      CASE 
        WHEN EXISTS (
          SELECT 1 FROM `{self.bq_client.dataset_ref}.token_prices` p
          WHERE p.token_address = t.token_address
            AND ABS(TIMESTAMP_DIFF(t.timestamp, p.timestamp, MINUTE)) <= 30
        ) THEN 'Available'
        WHEN EXISTS (
          SELECT 1 FROM `{self.bq_client.dataset_ref}.token_prices` p
          WHERE p.token_address = t.token_address
            AND ABS(TIMESTAMP_DIFF(t.timestamp, p.timestamp, HOUR)) <= 6
        ) THEN 'Available within 6h'
        ELSE 'Missing'
      END as price_data_status,
      
      -- Count potential matches
      (SELECT COUNT(*) 
       FROM `{self.bq_client.dataset_ref}.transactions` t3
       WHERE t3.wallet_address = t.wallet_address 
         AND t3.token_address = t.token_address 
         AND t3.transfer_type != t.transfer_type
         AND ((t.transfer_type = 'buy' AND t3.timestamp > t.timestamp) OR
              (t.transfer_type = 'sell' AND t3.timestamp < t.timestamp))
      ) as potential_counterparts,
      
      -- Time since transaction
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), t.timestamp, HOUR) as hours_since_transaction,
      
      -- Activity context
      (SELECT COUNT(*) 
       FROM `{self.bq_client.dataset_ref}.transactions` t4
       WHERE t4.wallet_address = t.wallet_address
         AND t4.timestamp >= TIMESTAMP_SUB(t.timestamp, INTERVAL 24 HOUR)
         AND t4.timestamp <= TIMESTAMP_ADD(t.timestamp, INTERVAL 24 HOUR)
      ) as daily_transaction_volume
      
    FROM `{self.bq_client.dataset_ref}.transactions` t
    WHERE t.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
      AND t.transaction_hash NOT IN (SELECT transaction_hash FROM matched_hashes)
    ORDER BY t.timestamp DESC
    LIMIT {limit}
    """
    
    def _execute_query():
        try:
            results = self.bq_client.client.query(unmatched_query).result()
            unmatched = []
            
            for row in results:
                transaction = dict(row)
                # Convert timestamp to ISO string
                if transaction.get('timestamp'):
                    transaction['timestamp'] = transaction['timestamp'].isoformat()
                
                # Add diagnostic insights
                transaction['diagnostic_insights'] = []
                
                if transaction['unmatch_reason'] == 'No price data':
                    transaction['diagnostic_insights'].append("Consider adding more price data sources or widening time window")
                
                if transaction['potential_counterparts'] == 0:
                    if transaction['transfer_type'] == 'buy':
                        transaction['diagnostic_insights'].append("Wallet may be holding this position long-term")
                    else:
                        transaction['diagnostic_insights'].append("May be selling from old position or complex trade")
                
                if transaction['daily_transaction_volume'] > 10:
                    transaction['diagnostic_insights'].append("High-frequency trader - may need advanced matching logic")
                
                if transaction['hours_since_transaction'] > 168:  # > 1 week
                    transaction['diagnostic_insights'].append("Old transaction - may be long-term position or data issue")
                
                unmatched.append(transaction)
            
            return unmatched
            
        except Exception as e:
            logger.error(f"Error getting unmatched transactions: {e}")
            return []
    
    import asyncio
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(self.bq_client.executor, _execute_query)

async def validate_trade_matches(self) -> Dict[str, Any]:
    """Validate the quality and accuracy of trade matching"""
    
    validation_query = f"""
    WITH match_validation AS (
      SELECT 
        COUNT(*) as total_matches,
        
        -- Time validation
        COUNT(CASE WHEN hold_hours < 0 THEN 1 END) as negative_hold_times,
        COUNT(CASE WHEN hold_hours > 8760 THEN 1 END) as excessive_hold_times,  -- > 1 year
        COUNT(CASE WHEN hold_minutes < 1 THEN 1 END) as instant_trades,
        AVG(hold_hours) as avg_hold_hours,
        STDDEV(hold_hours) as stddev_hold_hours,
        
        -- Price validation
        COUNT(CASE WHEN buy_price <= 0 OR sell_price <= 0 THEN 1 END) as invalid_prices,
        COUNT(CASE WHEN ABS(price_change_percent) > 10000 THEN 1 END) as extreme_price_changes,
        COUNT(CASE WHEN ABS(price_change_percent) > 1000 THEN 1 END) as very_high_returns,
        COUNT(CASE WHEN price_change_percent < -95 THEN 1 END) as near_total_losses,
        AVG(ABS(price_change_percent)) as avg_price_change,
        STDDEV(price_change_percent) as stddev_price_change,
        
        -- Volume validation
        COUNT(CASE WHEN buy_amount <= 0 OR sell_amount <= 0 THEN 1 END) as invalid_amounts,
        COUNT(CASE WHEN trade_volume_usd <= 0 THEN 1 END) as invalid_volumes,
        COUNT(CASE WHEN trade_volume_usd > 10000000 THEN 1 END) as whale_trades,  -- > $10M
        AVG(trade_volume_usd) as avg_trade_volume,
        
        -- Gas validation
        COUNT(CASE WHEN gas_efficiency_ratio > 1 THEN 1 END) as impossible_gas_ratios,
        COUNT(CASE WHEN gas_efficiency_ratio > 0.5 THEN 1 END) as high_gas_ratios,
        COUNT(CASE WHEN gas_efficiency_ratio > 0.1 THEN 1 END) as moderate_gas_ratios,
        AVG(gas_efficiency_ratio) as avg_gas_ratio,
        
        -- Profitability validation
        COUNT(CASE WHEN price_change_percent > 0 THEN 1 END) as profitable_matches,
        COUNT(CASE WHEN price_change_percent <= 0 THEN 1 END) as unprofitable_matches,
        COUNT(CASE WHEN price_change_percent > 100 THEN 1 END) as exceptional_gains,
        COUNT(CASE WHEN price_change_percent < -50 THEN 1 END) as major_losses,
        
        -- Data quality
        COUNT(CASE WHEN price_data_quality = 'Complete' THEN 1 END) as complete_data_matches,
        COUNT(CASE WHEN price_data_quality = 'Partial' THEN 1 END) as partial_data_matches,
        COUNT(CASE WHEN price_data_quality = 'Missing' THEN 1 END) as missing_data_matches,
        
        -- Matching logic validation
        COUNT(DISTINCT wallet_address) as unique_wallets_matched,
        COUNT(DISTINCT token_address) as unique_tokens_matched,
        COUNT(DISTINCT CONCAT(wallet_address, token_address)) as unique_wallet_token_pairs,
        
        -- Platform analysis
        COUNT(CASE WHEN buy_platform = sell_platform THEN 1 END) as same_platform_trades,
        COUNT(CASE WHEN buy_platform != sell_platform THEN 1 END) as cross_platform_trades,
        COUNT(DISTINCT buy_platform) as unique_buy_platforms,
        COUNT(DISTINCT sell_platform) as unique_sell_platforms,
        
        -- Trade type distribution
        COUNT(CASE WHEN trade_type = 'Scalp' THEN 1 END) as scalp_count,
        COUNT(CASE WHEN trade_type = 'Day Trade' THEN 1 END) as day_trade_count,
        COUNT(CASE WHEN trade_type = 'Swing Trade' THEN 1 END) as swing_trade_count,
        COUNT(CASE WHEN trade_type = 'Position Trade' THEN 1 END) as position_trade_count,
        
        -- Efficiency metrics
        AVG(trade_efficiency_score) as avg_efficiency_score,
        COUNT(CASE WHEN trade_efficiency_score > 0.8 THEN 1 END) as high_efficiency_trades,
        COUNT(CASE WHEN trade_efficiency_score < 0.2 THEN 1 END) as low_efficiency_trades
        
      FROM `{self.bq_client.dataset_ref}.matched_trades`
      WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    ),
    
    wallet_match_distribution AS (
      SELECT 
        wallet_address,
        COUNT(*) as matches_count,
        AVG(price_change_percent) as avg_performance,
        STDDEV(price_change_percent) as performance_volatility,
        COUNT(CASE WHEN price_change_percent > 0 THEN 1 END) / COUNT(*) as win_rate,
        AVG(hold_hours) as avg_hold_time,
        COUNT(DISTINCT token_address) as unique_tokens,
        AVG(gas_efficiency_ratio) as avg_gas_efficiency,
        SUM(trade_volume_usd) as total_volume_traded,
        
        -- Risk metrics
        MAX(ABS(price_change_percent)) as max_single_trade_return,
        COUNT(CASE WHEN ABS(price_change_percent) > 100 THEN 1 END) as extreme_trades,
        
        -- Consistency metrics
        STDDEV(hold_hours) as hold_time_consistency,
        COUNT(DISTINCT trade_type) as trade_type_diversity
        
      FROM `{self.bq_client.dataset_ref}.matched_trades`
      WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
      GROUP BY wallet_address
      HAVING COUNT(*) >= 5  -- Wallets with significant activity
      ORDER BY matches_count DESC
      LIMIT 20
    ),
    
    suspicious_patterns AS (
      -- Identify potentially problematic matches
      SELECT 
        'extreme_returns' as pattern_type,
        COUNT(*) as occurrence_count,
        'Matches with >1000% or <-90% returns - likely data errors' as description,
        ROUND(COUNT(*) / (SELECT COUNT(*) FROM `{self.bq_client.dataset_ref}.matched_trades` 
                         WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) * 100, 2) as percentage_of_total
      FROM `{self.bq_client.dataset_ref}.matched_trades`
      WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        AND (price_change_percent > 1000 OR price_change_percent < -90)
      
      UNION ALL
      
      SELECT 
        'instant_trades' as pattern_type,
        COUNT(*) as occurrence_count,
        'Matches with hold time < 1 minute - possible MEV or data issues' as description,
        ROUND(COUNT(*) / (SELECT COUNT(*) FROM `{self.bq_client.dataset_ref}.matched_trades` 
                         WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) * 100, 2) as percentage_of_total
      FROM `{self.bq_client.dataset_ref}.matched_trades`
      WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        AND hold_minutes < 1
      
      UNION ALL
      
      SELECT 
        'high_gas_trades' as pattern_type,
        COUNT(*) as occurrence_count,
        'Matches where gas costs > 50% of trade value - likely small trades or errors' as description,
        ROUND(COUNT(*) / (SELECT COUNT(*) FROM `{self.bq_client.dataset_ref}.matched_trades` 
                         WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) * 100, 2) as percentage_of_total
      FROM `{self.bq_client.dataset_ref}.matched_trades`
      WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        AND gas_efficiency_ratio > 0.5
      
      UNION ALL
      
      SELECT 
        'impossible_gas' as pattern_type,
        COUNT(*) as occurrence_count,
        'Matches where gas > trade value - definitely errors' as description,
        ROUND(COUNT(*) / (SELECT COUNT(*) FROM `{self.bq_client.dataset_ref}.matched_trades` 
                         WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) * 100, 2) as percentage_of_total
      FROM `{self.bq_client.dataset_ref}.matched_trades`
      WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        AND gas_efficiency_ratio > 1
      
      UNION ALL
      
      SELECT 
        'negative_hold_times' as pattern_type,
        COUNT(*) as occurrence_count,
        'Matches with negative hold times - matching logic errors' as description,
        ROUND(COUNT(*) / (SELECT COUNT(*) FROM `{self.bq_client.dataset_ref}.matched_trades` 
                         WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) * 100, 2) as percentage_of_total
      FROM `{self.bq_client.dataset_ref}.matched_trades`
      WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        AND hold_hours < 0
    )
    
    SELECT 
      'validation_metrics' as analysis_section,
      TO_JSON_STRING(STRUCT(
        total_matches,
        negative_hold_times,
        excessive_hold_times,
        instant_trades,
        avg_hold_hours,
        stddev_hold_hours,
        invalid_prices,
        extreme_price_changes,
        very_high_returns,
        near_total_losses,
        avg_price_change,
        stddev_price_change,
        invalid_amounts,
        invalid_volumes,
        whale_trades,
        avg_trade_volume,
        impossible_gas_ratios,
        high_gas_ratios,
        moderate_gas_ratios,
        avg_gas_ratio,
        profitable_matches,
        unprofitable_matches,
        exceptional_gains,
        major_losses,
        complete_data_matches,
        partial_data_matches,
        missing_data_matches,
        unique_wallets_matched,
        unique_tokens_matched,
        unique_wallet_token_pairs,
        same_platform_trades,
        cross_platform_trades,
        unique_buy_platforms,
        unique_sell_platforms,
        scalp_count,
        day_trade_count,
        swing_trade_count,
        position_trade_count,
        avg_efficiency_score,
        high_efficiency_trades,
        low_efficiency_trades
      )) as data
    FROM match_validation
    
    UNION ALL
    
    SELECT 
      'wallet_distribution' as analysis_section,
      TO_JSON_STRING(ARRAY_AGG(STRUCT(
        wallet_address,
        matches_count,
        avg_performance,
        performance_volatility,
        win_rate,
        avg_hold_time,
        unique_tokens,
        avg_gas_efficiency,
        total_volume_traded,
        max_single_trade_return,
        extreme_trades,
        hold_time_consistency,
        trade_type_diversity
      ))) as data
    FROM wallet_match_distribution
    
    UNION ALL
    
    SELECT 
      'suspicious_patterns' as analysis_section,
      TO_JSON_STRING(ARRAY_AGG(STRUCT(
        pattern_type,
        occurrence_count,
        description,
        percentage_of_total
      ))) as data
    FROM suspicious_patterns
    WHERE occurrence_count > 0
    """
    
    def _execute_validation():
        try:
            results = list(self.bq_client.client.query(validation_query).result())
            validation = {}
            
            for row in results:
                section = row['analysis_section']
                data = json.loads(row['data']) if row['data'] else {}
                validation[section] = data
            
            # Calculate overall validation score
            metrics = validation.get('validation_metrics', {})
            total_matches = metrics.get('total_matches', 0)
            
            if total_matches > 0:
                # Calculate quality score (0-100)
                quality_score = 100
                issues = []
                
                # Critical issues (major point deductions)
                if metrics.get('negative_hold_times', 0) > 0:
                    quality_score -= 25
                    issues.append(f"Found {metrics['negative_hold_times']} negative hold times")
                
                if metrics.get('impossible_gas_ratios', 0) > 0:
                    quality_score -= 20
                    issues.append(f"Found {metrics['impossible_gas_ratios']} impossible gas ratios")
                
                if metrics.get('invalid_prices', 0) > 0:
                    quality_score -= 20
                    issues.append(f"Found {metrics['invalid_prices']} invalid prices")
                
                # Major issues (moderate deductions)
                extreme_pct = (metrics.get('extreme_price_changes', 0) / total_matches * 100)
                if extreme_pct > 5:  # >5% extreme changes
                    quality_score -= 15
                    issues.append(f"{extreme_pct:.1f}% extreme price changes")
                
                high_gas_pct = (metrics.get('high_gas_ratios', 0) / total_matches * 100)
                if high_gas_pct > 10:  # >10% high gas
                    quality_score -= 10
                    issues.append(f"{high_gas_pct:.1f}% high gas cost trades")
                
                missing_data_pct = (metrics.get('missing_data_matches', 0) / total_matches * 100)
                if missing_data_pct > 20:  # >20% missing data
                    quality_score -= 15
                    issues.append(f"{missing_data_pct:.1f}% missing price data")
                
                # Minor issues (small deductions)
                instant_pct = (metrics.get('instant_trades', 0) / total_matches * 100)
                if instant_pct > 2:  # >2% instant trades
                    quality_score -= 5
                    issues.append(f"{instant_pct:.1f}% instant trades")
                
                # Check for suspicious patterns
                suspicious = validation.get('suspicious_patterns', [])
                for pattern in suspicious:
                    if pattern.get('percentage_of_total', 0) > 5:  # >5% occurrence
                        quality_score -= 10
                        issues.append(f"High occurrence of {pattern['pattern_type']}")
                
                # Positive adjustments for good metrics
                if metrics.get('avg_efficiency_score', 0) > 0.7:
                    quality_score += 5  # Bonus for high efficiency
                
                complete_data_pct = (metrics.get('complete_data_matches', 0) / total_matches * 100)
                if complete_data_pct > 90:
                    quality_score += 5  # Bonus for complete data
                
                validation['overall_assessment'] = {
                    'quality_score': max(0, min(100, quality_score)),
                    'grade': (
                        'A+' if quality_score >= 95 else
                        'A' if quality_score >= 90 else
                        'B+' if quality_score >= 85 else
                        'B' if quality_score >= 80 else
                        'C+' if quality_score >= 75 else
                        'C' if quality_score >= 70 else
                        'D' if quality_score >= 60 else 'F'
                    ),
                    'total_matches_analyzed': total_matches,
                    'data_completeness_pct': complete_data_pct,
                    'profitability_rate_pct': (metrics.get('profitable_matches', 0) / total_matches * 100),
                    'avg_efficiency_score': metrics.get('avg_efficiency_score', 0),
                    'issues_identified': issues,
                    'validation_timestamp': datetime.utcnow().isoformat(),
                    'recommendations': []
                }
                
                # Add recommendations based on issues
                if quality_score < 70:
                    validation['overall_assessment']['recommendations'].append("Consider reviewing matching logic and data quality")
                if missing_data_pct > 15:
                    validation['overall_assessment']['recommendations'].append("Improve price data coverage")
                if extreme_pct > 3:
                    validation['overall_assessment']['recommendations'].append("Add filters for extreme price changes")
                if high_gas_pct > 8:
                    validation['overall_assessment']['recommendations'].append("Review gas efficiency calculations")
            
            return validation
            
        except Exception as e:
            logger.error(f"Error validating trade matches: {e}")
            return {"error": str(e)}
    
    import asyncio
    from datetime import datetime
    import json
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(self.bq_client.executor, _execute_validation)


async def cleanup_invalid_matches(self, dry_run: bool = True) -> Dict[str, int]:
    """Remove invalid or low-quality trade matches"""
    
    # Define criteria for invalid matches with explanations
    cleanup_conditions = [
        {
            'name': 'negative_hold_times',
            'condition': 'hold_hours < 0',
            'description': 'Impossible negative hold times - matching logic error'
        },
        {
            'name': 'invalid_prices',
            'condition': 'buy_price <= 0 OR sell_price <= 0',
            'description': 'Invalid or zero prices - data quality issue'
        },
        {
            'name': 'extreme_price_changes',
            'condition': 'ABS(price_change_percent) > 10000',
            'description': 'Extreme price changes >10000% - likely data errors'
        },
        {
            'name': 'impossible_gas_ratios',
            'condition': 'gas_efficiency_ratio > 2',
            'description': 'Gas costs > 2x trade value - impossible scenario'
        },
        {
            'name': 'invalid_amounts',
            'condition': 'buy_amount <= 0 OR sell_amount <= 0',
            'description': 'Invalid transaction amounts - data corruption'
        },
        {
            'name': 'invalid_trade_volumes',
            'condition': 'trade_volume_usd <= 0',
            'description': 'Invalid trade volumes - calculation error'
        },
        {
            'name': 'excessive_hold_times',
            'condition': 'hold_hours > 17520',  # > 2 years
            'description': 'Excessive hold times > 2 years - likely matching error'
        },
        {
            'name': 'duplicate_transactions',
            'condition': 'buy_hash = sell_hash',
            'description': 'Same transaction matched as both buy and sell'
        }
    ]
    
    results = {}
    total_deleted = 0
    
    # Get initial count
    initial_count_query = f"""
    SELECT COUNT(*) as initial_count
    FROM `{self.bq_client.dataset_ref}.matched_trades`
    """
    
    try:
        initial_result = await self.bq_client.execute_query(initial_count_query)
        initial_rows = list(initial_result.result())
        initial_count = initial_rows[0]['initial_count'] if initial_rows else 0
        results['initial_match_count'] = initial_count
    except Exception as e:
        logger.error(f"Error getting initial count: {e}")
        results['initial_match_count'] = 0
    
    # Process each cleanup condition
    for condition_info in cleanup_conditions:
        condition_name = condition_info['name']
        condition_sql = condition_info['condition']
        description = condition_info['description']
        
        try:
            # Count matches that would be affected
            count_query = f"""
            SELECT 
              COUNT(*) as affected_count,
              COUNT(DISTINCT wallet_address) as affected_wallets,
              COUNT(DISTINCT token_address) as affected_tokens,
              MIN(created_at) as earliest_match,
              MAX(created_at) as latest_match
            FROM `{self.bq_client.dataset_ref}.matched_trades`
            WHERE {condition_sql}
            """
            
            count_result = await self.bq_client.execute_query(count_query)
            count_rows = list(count_result.result())
            
            if count_rows:
                count_data = dict(count_rows[0])
                affected_count = count_data['affected_count']
                
                # Convert timestamps for JSON serialization
                if count_data.get('earliest_match'):
                    count_data['earliest_match'] = count_data['earliest_match'].isoformat()
                if count_data.get('latest_match'):
                    count_data['latest_match'] = count_data['latest_match'].isoformat()
            else:
                affected_count = 0
                count_data = {'affected_count': 0}
            
            results[condition_name] = {
                'description': description,
                'condition': condition_sql,
                'affected_matches': affected_count,
                'affected_wallets': count_data.get('affected_wallets', 0),
                'affected_tokens': count_data.get('affected_tokens', 0),
                'earliest_match': count_data.get('earliest_match'),
                'latest_match': count_data.get('latest_match')
            }
            
            # If not a dry run and there are matches to clean up
            if not dry_run and affected_count > 0:
                # Get sample of records being deleted for audit trail
                sample_query = f"""
                SELECT 
                  wallet_address,
                  token_symbol,
                  buy_hash,
                  sell_hash,
                  price_change_percent,
                  hold_hours,
                  trade_volume_usd
                FROM `{self.bq_client.dataset_ref}.matched_trades`
                WHERE {condition_sql}
                LIMIT 5
                """
                
                sample_result = await self.bq_client.execute_query(sample_query)
                sample_rows = [dict(row) for row in sample_result.result()]
                results[condition_name]['sample_deleted_records'] = sample_rows
                
                # Perform the cleanup
                cleanup_query = f"""
                DELETE FROM `{self.bq_client.dataset_ref}.matched_trades`
                WHERE {condition_sql}
                """
                
                cleanup_job = await self.bq_client.execute_query(cleanup_query)
                deleted_count = cleanup_job.num_dml_affected_rows or 0
                results[condition_name]['deleted_matches'] = deleted_count
                total_deleted += deleted_count
                
                logger.info(f"Cleaned up {deleted_count} invalid matches for condition: {condition_name}")
            else:
                results[condition_name]['deleted_matches'] = 0
        
        except Exception as e:
            logger.error(f"Error processing cleanup condition '{condition_name}': {e}")
            results[condition_name] = {
                'description': description,
                'condition': condition_sql,
                'error': str(e),
                'affected_matches': 0,
                'deleted_matches': 0
            }
    
    # Get final count if cleanup was performed
    final_count = initial_count
    if not dry_run and total_deleted > 0:
        try:
            final_result = await self.bq_client.execute_query(initial_count_query)  # Reuse same query
            final_rows = list(final_result.result())
            final_count = final_rows[0]['initial_count'] if final_rows else 0
        except Exception as e:
            logger.error(f"Error getting final count: {e}")
    
    # Calculate cleanup statistics
    total_affected = sum(
        r.get('affected_matches', 0) 
        for r in results.values() 
        if isinstance(r, dict) and 'affected_matches' in r
    )
    
    # Create cleanup summary with detailed analytics
    results['cleanup_summary'] = {
        'dry_run': dry_run,
        'initial_match_count': initial_count,
        'final_match_count': final_count,
        'total_affected_matches': total_affected,
        'total_deleted_matches': total_deleted,
        'cleanup_percentage': (total_deleted / initial_count * 100) if initial_count > 0 else 0,
        'cleanup_timestamp': datetime.utcnow().isoformat(),
        'conditions_checked': len(cleanup_conditions),
        'conditions_with_issues': len([
            r for r in results.values() 
            if isinstance(r, dict) and r.get('affected_matches', 0) > 0
        ])
    }
    
    # Add recommendations based on cleanup results
    recommendations = []
    
    if total_affected > 0:
        affected_percentage = (total_affected / initial_count * 100) if initial_count > 0 else 0
        
        if affected_percentage > 10:
            recommendations.append("High percentage of invalid matches detected - review data ingestion process")
        
        if results.get('negative_hold_times', {}).get('affected_matches', 0) > 0:
            recommendations.append("Fix matching algorithm to prevent negative hold times")
        
        if results.get('extreme_price_changes', {}).get('affected_matches', 0) > 0:
            recommendations.append("Implement price change validation in data pipeline")
        
        if results.get('invalid_prices', {}).get('affected_matches', 0) > 0:
            recommendations.append("Improve price data quality and validation")
        
        if results.get('impossible_gas_ratios', {}).get('affected_matches', 0) > 0:
            recommendations.append("Review gas cost calculations and trade volume computations")
    
    if dry_run and total_affected > 0:
        recommendations.append(f"Run cleanup with dry_run=False to remove {total_affected} invalid matches")
    elif not dry_run and total_deleted > 0:
        recommendations.append("Monitor matching quality after cleanup and adjust validation rules if needed")
    
    if len(recommendations) == 0:
        recommendations.append("No significant data quality issues detected")
    
    results['cleanup_summary']['recommendations'] = recommendations
    
    # Add data quality assessment
    if initial_count > 0:
        quality_score = max(0, 100 - (total_affected / initial_count * 100))
        results['cleanup_summary']['data_quality_assessment'] = {
            'quality_score': round(quality_score, 2),
            'quality_grade': (
                'A' if quality_score >= 95 else
                'B' if quality_score >= 90 else
                'C' if quality_score >= 85 else
                'D' if quality_score >= 80 else
                'F'
            ),
            'issues_found': total_affected > 0,
            'critical_issues': any([
                results.get('negative_hold_times', {}).get('affected_matches', 0) > 0,
                results.get('impossible_gas_ratios', {}).get('affected_matches', 0) > 0,
                results.get('invalid_prices', {}).get('affected_matches', 0) > 0
            ])
        }
    
    return results

async def get_matching_statistics(self, days_back: int = 30) -> Dict[str, Any]:
    """Get comprehensive statistics about matching performance over time"""
    
    stats_query = f"""
    WITH daily_matching_stats AS (
      SELECT 
        DATE(created_at) as match_date,
        COUNT(*) as daily_matches,
        COUNT(DISTINCT wallet_address) as unique_wallets,
        COUNT(DISTINCT token_address) as unique_tokens,
        
        -- Quality metrics
        COUNT(CASE WHEN price_data_quality = 'Complete' THEN 1 END) as complete_data_count,
        COUNT(CASE WHEN ABS(price_change_percent) > 100 THEN 1 END) as high_return_trades,
        COUNT(CASE WHEN price_change_percent > 0 THEN 1 END) as profitable_trades,
        
        -- Performance metrics
        AVG(price_change_percent) as avg_daily_performance,
        AVG(hold_hours) as avg_daily_hold_time,
        AVG(trade_volume_usd) as avg_daily_trade_size,
        AVG(gas_efficiency_ratio) as avg_daily_gas_efficiency,
        
        -- Trade type distribution
        COUNT(CASE WHEN trade_type = 'Scalp' THEN 1 END) as scalp_trades,
        COUNT(CASE WHEN trade_type = 'Day Trade' THEN 1 END) as day_trades,
        COUNT(CASE WHEN trade_type = 'Swing Trade' THEN 1 END) as swing_trades,
        COUNT(CASE WHEN trade_type = 'Position Trade' THEN 1 END) as position_trades
        
      FROM `{self.bq_client.dataset_ref}.matched_trades`
      WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
      GROUP BY DATE(created_at)
      ORDER BY match_date DESC
    ),
    
    token_matching_performance AS (
      SELECT 
        token_symbol,
        COUNT(*) as total_matches,
        COUNT(DISTINCT wallet_address) as unique_traders,
        AVG(price_change_percent) as avg_token_performance,
        COUNT(CASE WHEN price_change_percent > 0 THEN 1 END) / COUNT(*) as token_win_rate,
        AVG(hold_hours) as avg_token_hold_time,
        SUM(trade_volume_usd) as total_token_volume,
        COUNT(CASE WHEN price_data_quality = 'Complete' THEN 1 END) / COUNT(*) as data_completeness_rate
        
      FROM `{self.bq_client.dataset_ref}.matched_trades`
      WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
      GROUP BY token_symbol
      HAVING COUNT(*) >= 10  -- Only tokens with significant activity
      ORDER BY total_matches DESC
      LIMIT 20
    ),
    
    wallet_matching_performance AS (
      SELECT 
        wallet_address,
        COUNT(*) as total_matches,
        COUNT(DISTINCT token_address) as unique_tokens,
        AVG(price_change_percent) as avg_wallet_performance,
        COUNT(CASE WHEN price_change_percent > 0 THEN 1 END) / COUNT(*) as wallet_win_rate,
        AVG(hold_hours) as avg_wallet_hold_time,
        SUM(trade_volume_usd) as total_wallet_volume,
        AVG(gas_efficiency_ratio) as avg_wallet_gas_efficiency,
        
        -- Wallet tier
        MAX(wallet_sophistication_score) as sophistication_score,
        CASE 
          WHEN MAX(wallet_sophistication_score) < 180 THEN 'Elite'
          WHEN MAX(wallet_sophistication_score) < 220 THEN 'High'
          WHEN MAX(wallet_sophistication_score) < 260 THEN 'Medium'
          ELSE 'Low'
        END as wallet_tier
        
      FROM `{self.bq_client.dataset_ref}.matched_trades`
      WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
      GROUP BY wallet_address
      HAVING COUNT(*) >= 5  -- Only wallets with significant activity
      ORDER BY total_matches DESC
      LIMIT 50
    ),
    
    platform_analysis AS (
      SELECT 
        CASE 
          WHEN buy_platform = sell_platform THEN buy_platform
          ELSE 'Cross-Platform'
        END as platform_category,
        COUNT(*) as trade_count,
        AVG(price_change_percent) as avg_performance,
        AVG(gas_efficiency_ratio) as avg_gas_efficiency,
        COUNT(CASE WHEN price_change_percent > 0 THEN 1 END) / COUNT(*) as win_rate
        
      FROM `{self.bq_client.dataset_ref}.matched_trades`
      WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
      GROUP BY platform_category
      ORDER BY trade_count DESC
    )
    
    SELECT 
      'daily_stats' as stats_section,
      TO_JSON_STRING(ARRAY_AGG(STRUCT(
        match_date,
        daily_matches,
        unique_wallets,
        unique_tokens,
        complete_data_count,
        high_return_trades,
        profitable_trades,
        avg_daily_performance,
        avg_daily_hold_time,
        avg_daily_trade_size,
        avg_daily_gas_efficiency,
        scalp_trades,
        day_trades,
        swing_trades,
        position_trades
      ) ORDER BY match_date)) as data
    FROM daily_matching_stats
    
    UNION ALL
    
    SELECT 
      'token_performance' as stats_section,
      TO_JSON_STRING(ARRAY_AGG(STRUCT(
        token_symbol,
        total_matches,
        unique_traders,
        avg_token_performance,
        token_win_rate,
        avg_token_hold_time,
        total_token_volume,
        data_completeness_rate
      ))) as data
    FROM token_matching_performance
    
    UNION ALL
    
    SELECT 
      'wallet_performance' as stats_section,
      TO_JSON_STRING(ARRAY_AGG(STRUCT(
        wallet_address,
        total_matches,
        unique_tokens,
        avg_wallet_performance,
        wallet_win_rate,
        avg_wallet_hold_time,
        total_wallet_volume,
        avg_wallet_gas_efficiency,
        sophistication_score,
        wallet_tier
      ))) as data
    FROM wallet_matching_performance
    
    UNION ALL
    
    SELECT 
      'platform_analysis' as stats_section,
      TO_JSON_STRING(ARRAY_AGG(STRUCT(
        platform_category,
        trade_count,
        avg_performance,
        avg_gas_efficiency,
        win_rate
      ))) as data
    FROM platform_analysis
    """
    
    def _execute_query():
        try:
            results = list(self.bq_client.client.query(stats_query).result())
            statistics = {}
            
            for row in results:
                section = row['stats_section']
                data = json.loads(row['data']) if row['data'] else []
                statistics[section] = data
            
            # Calculate summary metrics
            daily_stats = statistics.get('daily_stats', [])
            if daily_stats:
                total_matches = sum(day['daily_matches'] for day in daily_stats)
                avg_daily_matches = total_matches / len(daily_stats) if daily_stats else 0
                
                # Calculate trends (comparing first half vs second half of period)
                mid_point = len(daily_stats) // 2
                if mid_point > 0:
                    recent_avg = sum(day['daily_matches'] for day in daily_stats[:mid_point]) / mid_point
                    older_avg = sum(day['daily_matches'] for day in daily_stats[mid_point:]) / (len(daily_stats) - mid_point)
                    trend = ((recent_avg - older_avg) / older_avg * 100) if older_avg > 0 else 0
                else:
                    trend = 0
                
                statistics['summary'] = {
                    'analysis_period_days': days_back,
                    'total_matches_analyzed': total_matches,
                    'avg_daily_matches': round(avg_daily_matches, 1),
                    'matching_trend_pct': round(trend, 1),
                    'total_unique_wallets': len(set(day.get('unique_wallets', 0) for day in daily_stats)),
                    'total_unique_tokens': len(set(day.get('unique_tokens', 0) for day in daily_stats)),
                    'analysis_timestamp': datetime.utcnow().isoformat()
                }
            
            return statistics
            
        except Exception as e:
            logger.error(f"Error getting matching statistics: {e}")
            return {"error": str(e)}
    
    import asyncio
    from datetime import datetime
    import json
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(self.bq_client.executor, _execute_query)

async def optimize_matching_parameters(self) -> Dict[str, Any]:
    """Analyze current matching performance and suggest optimization parameters"""
    
    optimization_query = f"""
    WITH matching_analysis AS (
      SELECT 
        -- Price data timing analysis
        AVG(CASE WHEN buy_price_source = sell_price_source THEN 1 ELSE 0 END) as same_source_rate,
        COUNT(CASE WHEN price_data_quality = 'Missing' THEN 1 END) as missing_price_count,
        COUNT(*) as total_matches,
        
        -- Hold time analysis
        AVG(hold_hours) as avg_hold_hours,
        COUNT(CASE WHEN hold_hours < 0.1 THEN 1 END) as very_short_holds,
        COUNT(CASE WHEN hold_hours > 8760 THEN 1 END) as very_long_holds,
        
        -- Volume analysis
        AVG(trade_volume_usd) as avg_trade_volume,
        COUNT(CASE WHEN trade_volume_usd < 10 THEN 1 END) as micro_trades,
        COUNT(CASE WHEN trade_volume_usd > 1000000 THEN 1 END) as whale_trades,
        
        -- Gas efficiency analysis
        AVG(gas_efficiency_ratio) as avg_gas_ratio,
        COUNT(CASE WHEN gas_efficiency_ratio > 0.1 THEN 1 END) as high_gas_trades,
        
        -- Performance analysis
        AVG(ABS(price_change_percent)) as avg_abs_return,
        COUNT(CASE WHEN ABS(price_change_percent) > 1000 THEN 1 END) as extreme_returns,
        
        -- Match quality
        AVG(trade_efficiency_score) as avg_efficiency_score
        
      FROM `{self.bq_client.dataset_ref}.matched_trades`
      WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    )
    
    SELECT * FROM matching_analysis
    """
    
    def _execute_optimization():
        try:
            results = list(self.bq_client.client.query(optimization_query).result())
            if not results:
                return {"error": "No matching data available for optimization"}
            
            analysis = dict(results[0])
            optimizations = {
                'current_metrics': analysis,
                'recommended_parameters': {},
                'optimization_suggestions': []
            }
            
            # Analyze price data timing
            missing_rate = (analysis['missing_price_count'] / analysis['total_matches']) * 100
            if missing_rate > 20:
                optimizations['recommended_parameters']['price_window_minutes'] = 60  # Increase from 30
                optimizations['optimization_suggestions'].append(
                    f"Increase price matching window to 60 minutes ({missing_rate:.1f}% missing data)"
                )
            elif missing_rate < 5:
                optimizations['recommended_parameters']['price_window_minutes'] = 15  # Decrease to 15
                optimizations['optimization_suggestions'].append(
                    "Price data quality excellent - can tighten matching window to 15 minutes"
                )
            
            # Analyze hold time filters
            very_short_rate = (analysis['very_short_holds'] / analysis['total_matches']) * 100
            if very_short_rate > 10:
                optimizations['recommended_parameters']['min_hold_minutes'] = 5  # Increase minimum
                optimizations['optimization_suggestions'].append(
                    f"Add minimum hold time filter of 5 minutes ({very_short_rate:.1f}% very short holds)"
                )
            
            very_long_rate = (analysis['very_long_holds'] / analysis['total_matches']) * 100
            if very_long_rate > 5:
                optimizations['recommended_parameters']['max_hold_hours'] = 4380  # 6 months
                optimizations['optimization_suggestions'].append(
                    f"Add maximum hold time filter of 6 months ({very_long_rate:.1f}% very long holds)"
                )
            
            # Analyze trade volume filters
            micro_rate = (analysis['micro_trades'] / analysis['total_matches']) * 100
            if micro_rate > 15:
                optimizations['recommended_parameters']['min_trade_volume_usd'] = 50
                optimizations['optimization_suggestions'].append(
                    f"Filter out micro trades < $50 ({micro_rate:.1f}% of matches)"
                )
            
            # Analyze gas efficiency
            high_gas_rate = (analysis['high_gas_trades'] / analysis['total_matches']) * 100
            if high_gas_rate > 10:
                optimizations['recommended_parameters']['max_gas_ratio'] = 0.05  # 5% max
                optimizations['optimization_suggestions'].append(
                    f"Filter trades with gas > 5% of volume ({high_gas_rate:.1f}% high gas trades)"
                )
            
            # Analyze extreme returns
            extreme_rate = (analysis['extreme_returns'] / analysis['total_matches']) * 100
            if extreme_rate > 2:
                optimizations['recommended_parameters']['max_return_percent'] = 2000  # 20x max
                optimizations['optimization_suggestions'].append(
                    f"Filter extreme returns > 2000% ({extreme_rate:.1f}% extreme returns)"
                )
            
            # Overall quality assessment
            avg_efficiency = analysis.get('avg_efficiency_score', 0)
            if avg_efficiency < 0.5:
                optimizations['optimization_suggestions'].append(
                    "Low average efficiency score - review matching algorithm"
                )
            elif avg_efficiency > 0.8:
                optimizations['optimization_suggestions'].append(
                    "High efficiency score - matching quality is excellent"
                )
            
            # Add performance recommendations
            if len(optimizations['optimization_suggestions']) == 0:
                optimizations['optimization_suggestions'].append(
                    "Current matching parameters appear optimal"
                )
            
            optimizations['optimization_summary'] = {
                'total_matches_analyzed': analysis['total_matches'],
                'current_efficiency_score': round(avg_efficiency, 3),
                'missing_price_data_pct': round(missing_rate, 2),
                'recommendations_count': len(optimizations['optimization_suggestions']),
                'analysis_timestamp': datetime.utcnow().isoformat()
            }
            
            return optimizations
            
        except Exception as e:
            logger.error(f"Error optimizing matching parameters: {e}")
            return {"error": str(e)}
    
    import asyncio
    from datetime import datetime
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(self.bq_client.executor, _execute_optimization)

# Helper method for debugging and development
async def debug_wallet_matching(self, wallet_address: str, days_back: int = 7) -> Dict[str, Any]:
    """Debug matching issues for a specific wallet"""
    
    wallet_address = wallet_address.lower()
    
    debug_query = f"""
    WITH wallet_transactions AS (
      SELECT 
        transaction_hash,
        timestamp,
        transfer_type,
        token_symbol,
        token_amount,
        cost_in_eth,
        platform
      FROM `{self.bq_client.dataset_ref}.transactions`
      WHERE wallet_address = '{wallet_address}'
        AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
      ORDER BY timestamp
    ),
    
    wallet_matches AS (
      SELECT 
        buy_hash,
        sell_hash,
        token_symbol,
        buy_time,
        sell_time,
        hold_hours,
        price_change_percent,
        trade_volume_usd,
        price_data_quality
      FROM `{self.bq_client.dataset_ref}.matched_trades`
      WHERE wallet_address = '{wallet_address}'
        AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
      ORDER BY buy_time
    )
    
    SELECT 
      'transactions' as data_type,
      TO_JSON_STRING(ARRAY_AGG(STRUCT(
        transaction_hash,
        timestamp,
        transfer_type,
        token_symbol,
        token_amount,
        cost_in_eth,
        platform
      ) ORDER BY timestamp)) as data
    FROM wallet_transactions
    
    UNION ALL
    
    SELECT 
      'matches' as data_type,
      TO_JSON_STRING(ARRAY_AGG(STRUCT(
        buy_hash,
        sell_hash,
        token_symbol,
        buy_time,
        sell_time,
        hold_hours,
        price_change_percent,
        trade_volume_usd,
        price_data_quality
      ) ORDER BY buy_time)) as data
    FROM wallet_matches
    """
    
    def _execute_debug():
        try:
            results = list(self.bq_client.client.query(debug_query).result())
            debug_info = {}
            
            for row in results:
                data_type = row['data_type']
                data = json.loads(row['data']) if row['data'] else []
                debug_info[data_type] = data
            
            # Analyze matching gaps
            transactions = debug_info.get('transactions', [])
            matches = debug_info.get('matches', [])
            
            # Convert timestamp strings back to datetime for analysis
            for tx in transactions:
                if tx.get('timestamp'):
                    tx['timestamp_parsed'] = datetime.fromisoformat(tx['timestamp'].replace('Z', '+00:00'))
            
            matched_hashes = set()
            for match in matches:
                matched_hashes.add(match['buy_hash'])
                matched_hashes.add(match['sell_hash'])
            
            unmatched_transactions = [
                tx for tx in transactions 
                if tx['transaction_hash'] not in matched_hashes
            ]
            
            debug_info['analysis'] = {
                'total_transactions': len(transactions),
                'total_matches': len(matches),
                'unmatched_transactions': len(unmatched_transactions),
                'match_rate_pct': (len(matches) * 2 / len(transactions) * 100) if transactions else 0,
                'unmatched_details': unmatched_transactions[:10],  # First 10 for brevity
                'analysis_timestamp': datetime.utcnow().isoformat()
            }
            
            return debug_info
            
        except Exception as e:
            logger.error(f"Error debugging wallet matching: {e}")
            return {"error": str(e)}
    
    import asyncio
    from datetime import datetime
    import json
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(self.bq_client.executor, _execute_debug)


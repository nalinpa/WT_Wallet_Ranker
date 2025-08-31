import logging
from typing import Optional, Dict, List, Any
from .bigquery_client import BigQueryClient

logger = logging.getLogger(__name__)

class ScoreCalculator:
    def __init__(self, bq_client: BigQueryClient):
        self.bq_client = bq_client
    
    async def calculate_score_adjustments(self) -> int:
        """Calculate score adjustments based on matched trades"""
        
        query = f"""
        CREATE OR REPLACE TABLE `{self.bq_client.dataset_ref}.score_adjustments` AS
        SELECT 
          wallet_address,
          buy_hash,
          sell_hash, 
          token_symbol,
          price_change_percent,
          profit_usd,
          hold_hours,
          trade_type,
          trade_volume_usd,
          buy_gas,
          sell_gas,
          buy_amount,
          buy_price,
          
          -- Calculate score delta with all adjustments - MEANINGFUL SCALING
          GREATEST(-300, LEAST(300, 
            ROUND(
              CASE 
                -- Exceptional performance (10x+ gains)
                WHEN price_change_percent >= 1000 THEN -200
                WHEN price_change_percent >= 500 THEN -150  
                WHEN price_change_percent >= 200 THEN -100
                WHEN price_change_percent >= 100 THEN -75
                
                -- Very good performance  
                WHEN price_change_percent >= 50 THEN -50
                WHEN price_change_percent >= 30 THEN -35
                WHEN price_change_percent >= 20 THEN -25
                WHEN price_change_percent >= 10 THEN -15
                WHEN price_change_percent >= 5 THEN -8
                WHEN price_change_percent > 0 THEN -3
                
                -- Losses - increasingly severe penalties
                WHEN price_change_percent >= -5 THEN 5
                WHEN price_change_percent >= -10 THEN 12
                WHEN price_change_percent >= -20 THEN 25
                WHEN price_change_percent >= -30 THEN 40
                WHEN price_change_percent >= -50 THEN 65
                WHEN price_change_percent >= -70 THEN 100
                WHEN price_change_percent >= -90 THEN 150
                ELSE 200  -- Near total loss
              END * 
              
              -- Size multiplier (bigger trades matter more)
              CASE 
                WHEN trade_volume_usd >= 100000 THEN 2.0  -- $100k+ trade
                WHEN trade_volume_usd >= 50000 THEN 1.8   -- $50k+ trade  
                WHEN trade_volume_usd >= 10000 THEN 1.5   -- $10k+ trade
                WHEN trade_volume_usd >= 5000 THEN 1.3    -- $5k+ trade
                WHEN trade_volume_usd >= 1000 THEN 1.1    -- $1k+ trade
                WHEN trade_volume_usd < 100 THEN 0.5      -- Very small trade
                ELSE 1.0
              END *
              
              -- Timing multiplier (trade type specific expectations)
              CASE 
                WHEN trade_type = 'Scalp' AND price_change_percent > 10 THEN 1.5
                WHEN trade_type = 'Scalp' AND price_change_percent > 0 THEN 1.2  
                WHEN trade_type = 'Scalp' AND price_change_percent < 0 THEN 1.3
                WHEN trade_type = 'Day Trade' AND price_change_percent > 20 THEN 1.3
                WHEN trade_type = 'Day Trade' AND price_change_percent < -10 THEN 1.2
                WHEN trade_type = 'Swing Trade' AND price_change_percent > 30 THEN 1.4
                WHEN trade_type = 'Swing Trade' AND price_change_percent < -15 THEN 1.3
                WHEN trade_type = 'Position Trade' AND price_change_percent > 50 THEN 1.6
                WHEN trade_type = 'Position Trade' AND price_change_percent > 20 THEN 1.3
                WHEN trade_type = 'Position Trade' AND price_change_percent < -20 THEN 1.4
                ELSE 1.0
              END *
              
              -- Gas efficiency penalty (more severe for inefficient trading)
              CASE 
                WHEN trade_volume_usd > 0 AND ((buy_gas + sell_gas) * 2500) / trade_volume_usd > 0.05 THEN 1.8  -- Gas > 5%
                WHEN trade_volume_usd > 0 AND ((buy_gas + sell_gas) * 2500) / trade_volume_usd > 0.02 THEN 1.4  -- Gas > 2%
                WHEN trade_volume_usd > 0 AND ((buy_gas + sell_gas) * 2500) / trade_volume_usd > 0.01 THEN 1.2  -- Gas > 1%
                WHEN trade_volume_usd > 0 AND ((buy_gas + sell_gas) * 2500) / trade_volume_usd < 0.001 THEN 0.9 -- Very efficient
                ELSE 1.0
              END
            )
          )) as score_delta,
          
          CURRENT_TIMESTAMP() as calculated_at
          
        FROM `{self.bq_client.dataset_ref}.matched_trades`
        WHERE price_change_percent IS NOT NULL
        """
        
        try:
            await self.bq_client.execute_query(query)
            
            # Get count
            count_query = f"""
            SELECT COUNT(*) as adjustment_count
            FROM `{self.bq_client.dataset_ref}.score_adjustments`
            """
            
            count_result = await self.bq_client.execute_query(count_query)
            count_rows = list(count_result.result())
            adjustment_count = count_rows[0]['adjustment_count'] if count_rows else 0
            
            logger.info(f"Calculated {adjustment_count} score adjustments")
            return adjustment_count
            
        except Exception as e:
            logger.error(f"Error calculating score adjustments: {e}")
            raise e
    
    async def generate_wallet_updates(self, min_trades: int = 3) -> int:
        """Generate wallet score updates based on aggregated adjustments"""
        
        query = f"""
        CREATE OR REPLACE TABLE `{self.bq_client.dataset_ref}.wallet_score_updates` AS
        SELECT 
          sa.wallet_address,
          COUNT(*) as trades_analyzed,
          SUM(sa.score_delta) as total_score_change,
          AVG(sa.score_delta) as avg_score_per_trade,
          AVG(sa.price_change_percent) as avg_profit_pct,
          SUM(CASE WHEN sa.price_change_percent > 0 THEN 1 ELSE 0 END) as winning_trades,
          SUM(CASE WHEN sa.price_change_percent <= 0 THEN 1 ELSE 0 END) as losing_trades,
          
          -- Calculate win rate
          SUM(CASE WHEN sa.price_change_percent > 0 THEN 1 ELSE 0 END) / COUNT(*) as win_rate,
          
          -- Get current score (use most recent from transactions)
          (SELECT wallet_sophistication_score 
           FROM `{self.bq_client.dataset_ref}.transactions` t 
           WHERE t.wallet_address = sa.wallet_address 
           ORDER BY t.timestamp DESC 
           LIMIT 1) as current_score,
          
          -- Total trading volume
          SUM(sa.trade_volume_usd) as total_volume_usd,
          AVG(sa.trade_volume_usd) as avg_trade_size_usd,
          
          CURRENT_TIMESTAMP() as update_calculated_at
          
        FROM `{self.bq_client.dataset_ref}.score_adjustments` sa
        GROUP BY sa.wallet_address
        HAVING COUNT(*) >= {min_trades}  -- Minimum trades for score update
        """
        
        try:
            await self.bq_client.execute_query(query)
            
            # Add suggested new scores
            update_query = f"""
            CREATE OR REPLACE TABLE `{self.bq_client.dataset_ref}.wallet_score_updates` AS
            SELECT *,
              -- Suggested new score (clamped to reasonable bounds)
              GREATEST(25, LEAST(1000, 
                COALESCE(current_score, 250) + total_score_change
              )) as suggested_new_score
            FROM `{self.bq_client.dataset_ref}.wallet_score_updates`
            """
            
            await self.bq_client.execute_query(update_query)
            
            # Get count
            count_query = f"""
            SELECT COUNT(*) as update_count
            FROM `{self.bq_client.dataset_ref}.wallet_score_updates`
            """
            
            count_result = await self.bq_client.execute_query(count_query)
            count_rows = list(count_result.result())
            update_count = count_rows[0]['update_count'] if count_rows else 0
            
            logger.info(f"Generated updates for {update_count} wallets")
            return update_count
            
        except Exception as e:
            logger.error(f"Error generating wallet updates: {e}")
            raise e
    
    async def generate_recommendations(self) -> int:
        """Generate human-readable score update recommendations"""
        
        query = f"""
        CREATE OR REPLACE TABLE `{self.bq_client.dataset_ref}.score_update_recommendations` AS
        SELECT 
          wallet_address,
          current_score,
          suggested_new_score,
          total_score_change,
          trades_analyzed,
          win_rate,
          avg_profit_pct,
          winning_trades,
          losing_trades,
          total_volume_usd,
          avg_trade_size_usd,
          
          -- Create recommendation message
          CASE 
            WHEN total_score_change <= -200 THEN 
              CONCAT('🚀🚀 LEGENDARY UPGRADE: Exceptional trading performance! ', 
                     CAST(winning_trades AS STRING), '/', CAST(trades_analyzed AS STRING), 
                     ' trades profitable (', ROUND(win_rate * 100, 1), '% win rate, avg: ', 
                     ROUND(avg_profit_pct, 1), '%). MAJOR SCORE BOOST: ', 
                     CAST(current_score AS STRING), ' → ', CAST(suggested_new_score AS STRING), 
                     ' (', CAST(total_score_change AS STRING), ')')
            WHEN total_score_change <= -100 THEN 
              CONCAT('🚀 MAJOR UPGRADE: Outstanding performance! Win rate: ', 
                     ROUND(win_rate * 100, 1), '%, avg profit: ', ROUND(avg_profit_pct, 1), 
                     '%. Score boost: ', CAST(current_score AS STRING), ' → ', 
                     CAST(suggested_new_score AS STRING), ' (', CAST(total_score_change AS STRING), ')')
            WHEN total_score_change <= -50 THEN
              CONCAT('✅ SOLID UPGRADE: Strong trading performance. Win rate: ', 
                     ROUND(win_rate * 100, 1), '%, avg profit: ', ROUND(avg_profit_pct, 1), 
                     '%. Improved score: ', CAST(current_score AS STRING), ' → ', 
                     CAST(suggested_new_score AS STRING), ' (', CAST(total_score_change AS STRING), ')')
            WHEN total_score_change <= -25 THEN
              CONCAT('↗️ GOOD PROGRESS: Above average performance. Win rate: ', 
                     ROUND(win_rate * 100, 1), '%. Score improvement: ', 
                     CAST(current_score AS STRING), ' → ', CAST(suggested_new_score AS STRING))
            WHEN total_score_change >= 200 THEN
              CONCAT('💥 MAJOR DOWNGRADE: Severe losses detected! ', 
                     CAST(losing_trades AS STRING), '/', CAST(trades_analyzed AS STRING), 
                     ' trades unprofitable. Avg loss: ', ROUND(avg_profit_pct, 1), '%. ',
                     'SIGNIFICANT PENALTY: ', CAST(current_score AS STRING), ' → ', 
                     CAST(suggested_new_score AS STRING), ' (+', CAST(ABS(total_score_change) AS STRING), ')')
            WHEN total_score_change >= 100 THEN
              CONCAT('⚠️⚠️ SERIOUS DOWNGRADE: Poor performance pattern. Win rate: ', 
                     ROUND(win_rate * 100, 1), '%, avg loss: ', ROUND(avg_profit_pct, 1), 
                     '%. Major penalty: ', CAST(current_score AS STRING), ' → ', 
                     CAST(suggested_new_score AS STRING), ' (+', CAST(ABS(total_score_change) AS STRING), ')')
            WHEN total_score_change >= 50 THEN
              CONCAT('📉 NOTABLE DOWNGRADE: Below average performance. Win rate: ', 
                     ROUND(win_rate * 100, 1), '%. Score penalty: ', 
                     CAST(current_score AS STRING), ' → ', CAST(suggested_new_score AS STRING), 
                     ' (+', CAST(ABS(total_score_change) AS STRING), ')')
            WHEN total_score_change >= 25 THEN
              CONCAT('⚠️ CAUTION: Underperforming trades. Score adjustment: ', 
                     CAST(current_score AS STRING), ' → ', CAST(suggested_new_score AS STRING))
            WHEN ABS(total_score_change) < 25 THEN
              CONCAT('➡️ MAINTAIN: Mixed performance, minor adjustment: ', 
                     CAST(current_score AS STRING), ' → ', CAST(suggested_new_score AS STRING))
            ELSE
              CONCAT('📊 STANDARD UPDATE: Score adjustment based on recent activity: ', 
                     CAST(current_score AS STRING), ' → ', CAST(suggested_new_score AS STRING))
          END as recommendation_message,
          
          -- Priority level  
          CASE 
            WHEN ABS(total_score_change) >= 150 THEN 'CRITICAL'
            WHEN ABS(total_score_change) >= 75 THEN 'HIGH'
            WHEN ABS(total_score_change) >= 35 THEN 'MEDIUM'  
            WHEN ABS(total_score_change) >= 15 THEN 'LOW'
            ELSE 'MINIMAL'
          END as priority,
          
          update_calculated_at
          
        FROM `{self.bq_client.dataset_ref}.wallet_score_updates`
        WHERE ABS(total_score_change) >= 15  -- Only meaningful changes
        ORDER BY ABS(total_score_change) DESC
        """
        
        try:
            await self.bq_client.execute_query(query)
            
            # Get count
            count_query = f"""
            SELECT COUNT(*) as recommendation_count
            FROM `{self.bq_client.dataset_ref}.score_update_recommendations`
            """
            
            count_result = await self.bq_client.execute_query(count_query)
            count_rows = list(count_result.result())
            rec_count = count_rows[0]['recommendation_count'] if count_rows else 0
            
            logger.info(f"Generated {rec_count} score recommendations")
            return rec_count
            
        except Exception as e:
            logger.error(f"Error generating recommendations: {e}")
            raise e
    
    async def calculate_advanced_wallet_features(self, days_back: int = 30) -> int:
        """Calculate advanced wallet features for machine learning models"""
        
        query = f"""
        CREATE OR REPLACE TABLE `{self.bq_client.dataset_ref}.wallet_features` AS
        WITH wallet_transactions AS (
          SELECT 
            t.*,
            p.price_usd,
            -- Calculate trade value in USD when price data is available
            CASE WHEN p.price_usd IS NOT NULL THEN t.token_amount * p.price_usd ELSE NULL END as trade_value_usd
          FROM `{self.bq_client.dataset_ref}.transactions` t
          LEFT JOIN `{self.bq_client.dataset_ref}.token_prices` p
            ON t.token_address = p.token_address
            AND ABS(TIMESTAMP_DIFF(t.timestamp, p.timestamp, MINUTE)) <= 30
          WHERE t.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
        ),
        
        wallet_stats AS (
          SELECT 
            wallet_address,
            wallet_sophistication_score,
            COUNT(*) as total_transactions,
            COUNT(DISTINCT token_address) as unique_tokens,
            COUNT(DISTINCT platform) as unique_platforms,
            
            -- Time-based metrics
            MIN(timestamp) as first_transaction,
            MAX(timestamp) as last_transaction,
            AVG(TIMESTAMP_DIFF(LEAD(timestamp) OVER (PARTITION BY wallet_address ORDER BY timestamp), 
                               timestamp, HOUR)) as avg_time_between_transactions,
            
            -- Volume and value metrics
            SUM(token_amount) as total_token_volume,
            AVG(token_amount) as avg_token_amount,
            SUM(CASE WHEN trade_value_usd IS NOT NULL THEN trade_value_usd ELSE 0 END) as total_volume_usd,
            AVG(CASE WHEN trade_value_usd IS NOT NULL THEN trade_value_usd END) as avg_trade_size_usd,
            MAX(CASE WHEN trade_value_usd IS NOT NULL THEN trade_value_usd END) as max_trade_size_usd,
            
            -- Gas metrics
            SUM(cost_in_eth) as total_gas_cost,
            AVG(cost_in_eth) as avg_gas_cost,
            MAX(cost_in_eth) as max_gas_cost,
            
            -- Trading behavior
            SUM(CASE WHEN transfer_type = 'buy' THEN 1 ELSE 0 END) as buy_count,
            SUM(CASE WHEN transfer_type = 'sell' THEN 1 ELSE 0 END) as sell_count,
            
            -- Time-of-day patterns
            AVG(EXTRACT(HOUR FROM timestamp)) as avg_trading_hour,
            STDDEV(EXTRACT(HOUR FROM timestamp)) as trading_hour_variance,
            
            -- Weekend vs weekday activity
            SUM(CASE WHEN EXTRACT(DAYOFWEEK FROM timestamp) IN (1, 7) THEN 1 ELSE 0 END) as weekend_trades,
            SUM(CASE WHEN EXTRACT(DAYOFWEEK FROM timestamp) NOT IN (1, 7) THEN 1 ELSE 0 END) as weekday_trades
            
          FROM wallet_transactions
          GROUP BY wallet_address, wallet_sophistication_score
        ),
        
        wallet_features_calculated AS (
          SELECT 
            wallet_address,
            wallet_sophistication_score as sophistication_score,
            total_transactions,
            unique_tokens,
            unique_platforms,
            
            -- Activity metrics
            DATE_DIFF(DATE(last_transaction), DATE(first_transaction), DAY) as active_days,
            total_transactions / NULLIF(DATE_DIFF(DATE(last_transaction), DATE(first_transaction), DAY), 0) as avg_transactions_per_day,
            COALESCE(avg_time_between_transactions, 24) as avg_hold_duration_hours,
            
            -- Volume metrics
            total_volume_usd,
            COALESCE(avg_trade_size_usd, 0) as avg_trade_size_usd,
            COALESCE(max_trade_size_usd, 0) as max_trade_size_usd,
            
            -- Gas efficiency
            total_gas_cost as total_volume_eth,
            CASE WHEN total_volume_usd > 0 THEN (total_gas_cost * 2500) / total_volume_usd ELSE 0 END as gas_efficiency_ratio,
            
            -- Trading patterns
            CASE WHEN total_transactions > 0 THEN buy_count / total_transactions ELSE 0 END as buy_ratio,
            CASE WHEN buy_count + sell_count > 0 THEN buy_count / (buy_count + sell_count) ELSE 0 END as profitable_trades_ratio,
            
            -- Diversification
            LEAST(unique_tokens / 20.0, 1.0) as diversification_index,
            
            -- Time patterns
            CASE WHEN total_transactions > 0 THEN weekend_trades / total_transactions ELSE 0 END as weekend_activity_ratio,
            COALESCE(trading_hour_variance, 0) as trading_hour_variance,
            
            -- Activity frequency score (0-1 scale)
            CASE 
              WHEN total_transactions / NULLIF(DATE_DIFF(DATE(last_transaction), DATE(first_transaction), DAY), 0) >= 5 THEN 1.0
              WHEN total_transactions / NULLIF(DATE_DIFF(DATE(last_transaction), DATE(first_transaction), DAY), 0) >= 2 THEN 0.8
              WHEN total_transactions / NULLIF(DATE_DIFF(DATE(last_transaction), DATE(first_transaction), DAY), 0) >= 1 THEN 0.6
              WHEN total_transactions / NULLIF(DATE_DIFF(DATE(last_transaction), DATE(first_transaction), DAY), 0) >= 0.5 THEN 0.4
              ELSE 0.2
            END as activity_frequency_score,
            
            last_transaction as last_activity,
            
            -- Wallet tier
            CASE 
              WHEN wallet_sophistication_score < 180 THEN 'Elite'
              WHEN wallet_sophistication_score < 220 THEN 'High'
              WHEN wallet_sophistication_score < 260 THEN 'Medium'
              ELSE 'Low'
            END as wallet_tier,
            
            CURRENT_TIMESTAMP() as feature_update_time
            
          FROM wallet_stats
          WHERE total_transactions >= 3  -- Minimum activity for meaningful features
        )
        
        SELECT 
          wallet_address,
          sophistication_score,
          total_transactions,
          unique_tokens,
          avg_hold_duration_hours,
          total_volume_eth,
          total_volume_usd,
          profitable_trades_ratio,
          ROUND(1.0 / (1.0 + gas_efficiency_ratio), 3) as gas_efficiency_score,  -- Invert so higher is better
          diversification_index,
          last_activity,
          wallet_tier,
          avg_trade_size_usd,
          max_trade_size_usd,
          activity_frequency_score,
          feature_update_time
        FROM wallet_features_calculated
        """
        
        try:
            await self.bq_client.execute_query(query)
            
            # Get count
            count_query = f"""
            SELECT COUNT(*) as feature_count
            FROM `{self.bq_client.dataset_ref}.wallet_features`
            """
            
            count_result = await self.bq_client.execute_query(count_query)
            count_rows = list(count_result.result())
            feature_count = count_rows[0]['feature_count'] if count_rows else 0
            
            logger.info(f"Calculated features for {feature_count} wallets")
            return feature_count
            
        except Exception as e:
            logger.error(f"Error calculating wallet features: {e}")
            raise e
    
    async def calculate_token_features(self, days_back: int = 7) -> int:
        """Calculate token quality features based on holder analysis"""
        
        query = f"""
        CREATE OR REPLACE TABLE `{self.bq_client.dataset_ref}.token_features` AS
        WITH token_holder_stats AS (
          SELECT 
            t.token_address,
            t.token_symbol,
            COUNT(DISTINCT t.wallet_address) as total_holders,
            COUNT(DISTINCT CASE WHEN t.wallet_sophistication_score < 180 THEN t.wallet_address END) as elite_holders,
            COUNT(DISTINCT CASE WHEN t.wallet_sophistication_score < 220 THEN t.wallet_address END) as high_tier_holders,
            
            -- Sophistication metrics
            AVG(t.wallet_sophistication_score) as avg_holder_sophistication,
            MIN(t.wallet_sophistication_score) as best_holder_score,
            
            -- Volume analysis
            SUM(t.token_amount) as total_volume,
            SUM(CASE WHEN t.wallet_sophistication_score < 180 THEN t.token_amount ELSE 0 END) as elite_volume,
            SUM(CASE WHEN t.wallet_sophistication_score < 220 THEN t.token_amount ELSE 0 END) as high_tier_volume,
            
            -- Buy/sell analysis by wallet quality
            SUM(CASE WHEN t.transfer_type = 'buy' AND t.wallet_sophistication_score < 180 THEN t.token_amount ELSE 0 END) as elite_buy_volume,
            SUM(CASE WHEN t.transfer_type = 'sell' AND t.wallet_sophistication_score < 180 THEN t.token_amount ELSE 0 END) as elite_sell_volume,
            
            -- Recent activity
            COUNT(CASE WHEN t.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN 1 END) as recent_transactions,
            COUNT(DISTINCT CASE WHEN t.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) 
                                    AND t.wallet_sophistication_score < 180 
                               THEN t.wallet_address END) as recent_elite_holders,
            
            -- Holding patterns
            AVG(TIMESTAMP_DIFF(LEAD(t.timestamp) OVER (PARTITION BY t.wallet_address, t.token_address ORDER BY t.timestamp), 
                               t.timestamp, HOUR)) as avg_hold_duration_hours,
            
            -- Whale analysis (top 10% of holders by volume)
            APPROX_QUANTILES(t.token_amount, 10)[OFFSET(9)] as p90_volume,
            COUNT(CASE WHEN t.token_amount >= APPROX_QUANTILES(t.token_amount, 10)[OFFSET(9)] THEN 1 END) as whale_transactions
            
          FROM `{self.bq_client.dataset_ref}.transactions` t
          WHERE t.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
            AND t.token_address != '0x0000000000000000000000000000000000000000'
          GROUP BY t.token_address, t.token_symbol
          HAVING total_holders >= 5  -- Minimum holders for meaningful analysis
        ),
        
        token_features_calculated AS (
          SELECT 
            token_address,
            token_symbol,
            
            -- Smart money metrics
            CASE WHEN total_volume > 0 THEN elite_volume / total_volume ELSE 0 END as smart_money_ratio,
            elite_holders as elite_wallet_holders,
            avg_holder_sophistication,
            
            -- Volume trends (simplified - would need historical data for true trends)
            CASE WHEN recent_transactions > 0 THEN recent_transactions / 24.0 ELSE 0 END as volume_trend_7d,
            
            -- Accumulation analysis
            CASE 
              WHEN elite_buy_volume + elite_sell_volume > 0 
              THEN (elite_buy_volume - elite_sell_volume) / (elite_buy_volume + elite_sell_volume)
              ELSE 0 
            END as accumulation_score,
            
            -- Holder quality
            CASE WHEN total_holders > 0 THEN elite_holders / total_holders ELSE 0 END as holder_stability_score,
            
            -- Price momentum placeholder (would integrate with price data)
            0.0 as price_momentum,
            
            total_holders,
            COALESCE(avg_hold_duration_hours, 24) as avg_hold_duration_hours,
            
            -- Whale concentration
            CASE WHEN total_volume > 0 THEN whale_transactions / total_holders ELSE 0 END as whale_concentration_ratio,
            
            CURRENT_TIMESTAMP() as feature_update_time
            
          FROM token_holder_stats
        )
        
        SELECT * FROM token_features_calculated
        ORDER BY smart_money_ratio DESC, elite_wallet_holders DESC
        """
        
        try:
            await self.bq_client.execute_query(query)
            
            # Get count
            count_query = f"""
            SELECT COUNT(*) as token_feature_count
            FROM `{self.bq_client.dataset_ref}.token_features`
            """
            
            count_result = await self.bq_client.execute_query(count_query)
            count_rows = list(count_result.result())
            feature_count = count_rows[0]['token_feature_count'] if count_rows else 0
            
            logger.info(f"Calculated features for {feature_count} tokens")
            return feature_count
            
        except Exception as e:
            logger.error(f"Error calculating token features: {e}")
            raise e
    
    async def run_full_scoring_pipeline(self, days_back: int = 7, min_trades: int = 3) -> Dict[str, int]:
        """Run the complete scoring pipeline in sequence"""
        try:
            logger.info("Starting full scoring pipeline")
            
            results = {}
            
            # Step 1: Calculate score adjustments
            logger.info("Step 1: Calculating score adjustments...")
            results['score_adjustments'] = await self.calculate_score_adjustments()
            
            # Step 2: Generate wallet updates
            logger.info("Step 2: Generating wallet updates...")
            results['wallet_updates'] = await self.generate_wallet_updates(min_trades)
            
            # Step 3: Generate recommendations
            logger.info("Step 3: Generating recommendations...")
            results['recommendations'] = await self.generate_recommendations()
            
            # Step 4: Calculate advanced features
            logger.info("Step 4: Calculating wallet features...")
            results['wallet_features'] = await self.calculate_advanced_wallet_features(days_back * 4)  # Longer period for features
            
            # Step 5: Calculate token features
            logger.info("Step 5: Calculating token features...")
            results['token_features'] = await self.calculate_token_features(days_back)
            
            logger.info(f"Scoring pipeline completed successfully: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error in scoring pipeline: {e}")
            raise e
    
    async def get_scoring_summary(self) -> Dict[str, Any]:
        """Get a summary of recent scoring activity and results"""
        
        summary_query = f"""
        WITH recent_recommendations AS (
          SELECT 
            priority,
            COUNT(*) as count,
            AVG(ABS(total_score_change)) as avg_change,
            MAX(ABS(total_score_change)) as max_change,
            MIN(ABS(total_score_change)) as min_change
          FROM `{self.bq_client.dataset_ref}.score_update_recommendations`
          WHERE update_calculated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
          GROUP BY priority
        ),
        
        wallet_tier_changes AS (
          SELECT 
            CASE 
              WHEN current_score < 180 THEN 'Elite'
              WHEN current_score < 220 THEN 'High' 
              WHEN current_score < 260 THEN 'Medium'
              ELSE 'Low'
            END as current_tier,
            CASE 
              WHEN suggested_new_score < 180 THEN 'Elite'
              WHEN suggested_new_score < 220 THEN 'High'
              WHEN suggested_new_score < 260 THEN 'Medium' 
              ELSE 'Low'
            END as suggested_tier,
            COUNT(*) as wallet_count
          FROM `{self.bq_client.dataset_ref}.wallet_score_updates`
          WHERE update_calculated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
          GROUP BY current_tier, suggested_tier
        ),
        
        top_performers AS (
          SELECT 
            wallet_address,
            current_score,
            suggested_new_score,
            total_score_change,
            win_rate,
            avg_profit_pct
          FROM `{self.bq_client.dataset_ref}.wallet_score_updates`
          WHERE update_calculated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            AND total_score_change < 0  -- Improvements only
          ORDER BY total_score_change ASC
          LIMIT 5
        ),
        
        worst_performers AS (
          SELECT 
            wallet_address,
            current_score, 
            suggested_new_score,
            total_score_change,
            win_rate,
            avg_profit_pct
          FROM `{self.bq_client.dataset_ref}.wallet_score_updates`
          WHERE update_calculated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            AND total_score_change > 0  -- Degradations only
          ORDER BY total_score_change DESC
          LIMIT 5
        )
        
        SELECT 
          'recommendations' as summary_type,
          TO_JSON_STRING(ARRAY_AGG(STRUCT(priority, count, avg_change, max_change, min_change))) as data
        FROM recent_recommendations
        
        UNION ALL
        
        SELECT 
          'tier_changes' as summary_type,
          TO_JSON_STRING(ARRAY_AGG(STRUCT(current_tier, suggested_tier, wallet_count))) as data
        FROM wallet_tier_changes
        
        UNION ALL
        
        SELECT 
          'top_performers' as summary_type,
          TO_JSON_STRING(ARRAY_AGG(STRUCT(wallet_address, current_score, suggested_new_score, total_score_change, win_rate, avg_profit_pct))) as data
        FROM top_performers
        
        UNION ALL
        
        SELECT 
          'worst_performers' as summary_type,
          TO_JSON_STRING(ARRAY_AGG(STRUCT(wallet_address, current_score, suggested_new_score, total_score_change, win_rate, avg_profit_pct))) as data
        FROM worst_performers
        """
        
        def _execute_query():
            try:
                results = list(self.bq_client.client.query(summary_query).result())
                summary = {}
                
                for row in results:
                    summary_type = row['summary_type']
                    data = json.loads(row['data']) if row['data'] else []
                    summary[summary_type] = data
                
                # Add metadata
                summary['generated_at'] = datetime.utcnow().isoformat()
                summary['period'] = '24 hours'
                
                return summary
                
            except Exception as e:
                logger.error(f"Error getting scoring summary: {e}")
                return {"error": str(e)}
        
        import asyncio
        from datetime import datetime
        import json
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.bq_client.executor, _execute_query)
    
    async def apply_score_updates(self, wallet_addresses: List[str] = None, auto_approve_threshold: int = 50) -> Dict[str, int]:
        """Apply approved score updates to the transactions table"""
        
        # Build WHERE clause for specific wallets or auto-approval
        where_clause = ""
        if wallet_addresses:
            wallet_list = "', '".join([addr.lower() for addr in wallet_addresses])
            where_clause = f"WHERE wallet_address IN ('{wallet_list}')"
        else:
            # Auto-approve changes below threshold
            where_clause = f"WHERE ABS(total_score_change) <= {auto_approve_threshold}"
        
        apply_query = f"""
        UPDATE `{self.bq_client.dataset_ref}.transactions` t
        SET wallet_sophistication_score = u.suggested_new_score
        FROM `{self.bq_client.dataset_ref}.wallet_score_updates` u
        WHERE t.wallet_address = u.wallet_address
          AND u.wallet_address IN (
            SELECT wallet_address 
            FROM `{self.bq_client.dataset_ref}.wallet_score_updates` 
            {where_clause}
          )
        """
        
        try:
            job = await self.bq_client.execute_query(apply_query)
            rows_updated = job.num_dml_affected_rows or 0
            
            # Mark applied updates
            mark_applied_query = f"""
            UPDATE `{self.bq_client.dataset_ref}.wallet_score_updates`
            SET applied_at = CURRENT_TIMESTAMP()
            {where_clause}
            """
            
            await self.bq_client.execute_query(mark_applied_query)
            
            # Get summary of applied updates
            summary_query = f"""
            SELECT 
              COUNT(*) as wallets_updated,
              SUM(ABS(total_score_change)) as total_change_applied,
              AVG(total_score_change) as avg_change_applied,
              COUNT(CASE WHEN total_score_change < 0 THEN 1 END) as upgrades,
              COUNT(CASE WHEN total_score_change > 0 THEN 1 END) as downgrades
            FROM `{self.bq_client.dataset_ref}.wallet_score_updates`
            WHERE applied_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)
            """
            
            summary_result = await self.bq_client.execute_query(summary_query)
            summary_rows = list(summary_result.result())
            summary = dict(summary_rows[0]) if summary_rows else {}
            
            logger.info(f"Applied score updates: {rows_updated} transaction records updated, {summary.get('wallets_updated', 0)} wallets affected")
            
            return {
                'transaction_records_updated': rows_updated,
                'wallets_affected': summary.get('wallets_updated', 0),
                'total_change_applied': summary.get('total_change_applied', 0),
                'upgrades': summary.get('upgrades', 0),
                'downgrades': summary.get('downgrades', 0)
            }
            
        except Exception as e:
            logger.error(f"Error applying score updates: {e}")
            raise e
    
    async def validate_scoring_results(self) -> Dict[str, Any]:
        """Validate the results of the scoring process for quality assurance"""
        
        validation_query = f"""
        WITH validation_checks AS (
          -- Check 1: Score adjustments distribution
          SELECT 
            'score_adjustment_distribution' as check_name,
            COUNT(*) as total_adjustments,
            AVG(score_delta) as avg_adjustment,
            MIN(score_delta) as min_adjustment,
            MAX(score_delta) as max_adjustment,
            STDDEV(score_delta) as stddev_adjustment,
            COUNT(CASE WHEN score_delta < -100 THEN 1 END) as extreme_positive_adjustments,
            COUNT(CASE WHEN score_delta > 100 THEN 1 END) as extreme_negative_adjustments
          FROM `{self.bq_client.dataset_ref}.score_adjustments`
          WHERE calculated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
          
          UNION ALL
          
          -- Check 2: Wallet update distribution
          SELECT 
            'wallet_update_distribution' as check_name,
            COUNT(*) as total_updates,
            AVG(total_score_change) as avg_change,
            MIN(total_score_change) as min_change,
            MAX(total_score_change) as max_change,
            STDDEV(total_score_change) as stddev_change,
            COUNT(CASE WHEN ABS(total_score_change) > 200 THEN 1 END) as extreme_changes,
            COUNT(CASE WHEN win_rate > 0.9 THEN 1 END) as suspicious_high_win_rates
          FROM `{self.bq_client.dataset_ref}.wallet_score_updates`
          WHERE update_calculated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        ),
        
        data_quality_checks AS (
          -- Check 3: Data consistency
          SELECT 
            'data_consistency' as check_name,
            COUNT(CASE WHEN price_change_percent IS NULL THEN 1 END) as missing_price_data,
            COUNT(CASE WHEN trade_volume_usd <= 0 THEN 1 END) as zero_volume_trades,
            COUNT(CASE WHEN hold_hours < 0 THEN 1 END) as negative_hold_times,
            COUNT(CASE WHEN ABS(price_change_percent) > 10000 THEN 1 END) as extreme_price_changes,
            COUNT(*) as total_matched_trades,
            0 as placeholder1,
            0 as placeholder2,
            0 as placeholder3
          FROM `{self.bq_client.dataset_ref}.matched_trades`
          WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        ),
        
        recommendation_quality AS (
          -- Check 4: Recommendation quality
          SELECT 
            'recommendation_quality' as check_name,
            COUNT(*) as total_recommendations,
            COUNT(CASE WHEN priority = 'CRITICAL' THEN 1 END) as critical_recommendations,
            COUNT(CASE WHEN priority = 'HIGH' THEN 1 END) as high_recommendations,
            COUNT(CASE WHEN ABS(total_score_change) < 15 THEN 1 END) as low_impact_recommendations,
            AVG(trades_analyzed) as avg_trades_per_recommendation,
            COUNT(DISTINCT wallet_address) as unique_wallets_with_recommendations,
            0 as placeholder7,
            0 as placeholder8
          FROM `{self.bq_client.dataset_ref}.score_update_recommendations`
          WHERE update_calculated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        )
        
        SELECT * FROM validation_checks
        UNION ALL
        SELECT * FROM data_quality_checks  
        UNION ALL
        SELECT * FROM recommendation_quality
        """
        
        def _execute_validation():
            try:
                results = list(self.bq_client.client.query(validation_query).result())
                validation_results = {}
                
                for row in results:
                    check_name = row['check_name']
                    check_data = dict(row)
                    del check_data['check_name']  # Remove the check name from the data
                    validation_results[check_name] = check_data
                
                # Calculate quality scores
                quality_score = 100  # Start with perfect score
                issues = []
                
                # Check for issues and deduct points
                if validation_results.get('score_adjustment_distribution', {}).get('extreme_positive_adjustments', 0) > 10:
                    quality_score -= 15
                    issues.append("High number of extreme positive score adjustments")
                
                if validation_results.get('score_adjustment_distribution', {}).get('extreme_negative_adjustments', 0) > 10:
                    quality_score -= 15
                    issues.append("High number of extreme negative score adjustments")
                
                if validation_results.get('data_consistency', {}).get('missing_price_data', 0) > 100:
                    quality_score -= 10
                    issues.append("High number of trades with missing price data")
                
                if validation_results.get('data_consistency', {}).get('extreme_price_changes', 0) > 5:
                    quality_score -= 10
                    issues.append("Extreme price changes detected")
                
                if validation_results.get('wallet_update_distribution', {}).get('suspicious_high_win_rates', 0) > 3:
                    quality_score -= 20
                    issues.append("Suspicious high win rates detected")
                
                validation_results['quality_assessment'] = {
                    'overall_quality_score': max(0, quality_score),
                    'quality_grade': (
                        'A' if quality_score >= 90 else
                        'B' if quality_score >= 80 else  
                        'C' if quality_score >= 70 else
                        'D' if quality_score >= 60 else 'F'
                    ),
                    'issues_found': issues,
                    'validation_timestamp': datetime.utcnow().isoformat()
                }
                
                return validation_results
                
            except Exception as e:
                logger.error(f"Error validating scoring results: {e}")
                return {"error": str(e)}
        
        import asyncio
        from datetime import datetime
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.bq_client.executor, _execute_validation)
    
    async def get_wallet_score_history(self, wallet_address: str, days_back: int = 30) -> List[Dict[str, Any]]:
        """Get the score change history for a specific wallet"""
        wallet_address = wallet_address.lower()
        
        history_query = f"""
        SELECT 
          update_calculated_at,
          current_score,
          suggested_new_score,
          total_score_change,
          trades_analyzed,
          win_rate,
          avg_profit_pct,
          recommendation_message,
          priority
        FROM `{self.bq_client.dataset_ref}.score_update_recommendations`
        WHERE wallet_address = '{wallet_address}'
          AND update_calculated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
        ORDER BY update_calculated_at DESC
        LIMIT 20
        """
        
        def _execute_query():
            try:
                results = self.bq_client.client.query(history_query).result()
                history = []
                
                for row in results:
                    record = dict(row)
                    # Convert timestamp to ISO string
                    if record.get('update_calculated_at'):
                        record['update_calculated_at'] = record['update_calculated_at'].isoformat()
                    history.append(record)
                
                return history
                
            except Exception as e:
                logger.error(f"Error getting wallet score history for {wallet_address}: {e}")
                return []
        
        import asyncio
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.bq_client.executor, _execute_query)
    
    async def generate_performance_report(self, days_back: int = 7) -> Dict[str, Any]:
        """Generate a comprehensive performance report of the scoring system"""
        
        report_query = f"""
        WITH scoring_metrics AS (
          SELECT 
            COUNT(DISTINCT sa.wallet_address) as wallets_analyzed,
            COUNT(*) as total_score_adjustments,
            AVG(ABS(sa.score_delta)) as avg_adjustment_magnitude,
            
            -- Performance distribution
            COUNT(CASE WHEN sa.price_change_percent > 0 THEN 1 END) as profitable_trades,
            COUNT(CASE WHEN sa.price_change_percent <= 0 THEN 1 END) as unprofitable_trades,
            AVG(sa.price_change_percent) as avg_trade_performance,
            
            -- Score change distribution
            COUNT(CASE WHEN sa.score_delta < 0 THEN 1 END) as score_improvements,
            COUNT(CASE WHEN sa.score_delta > 0 THEN 1 END) as score_degradations,
            
            -- Trade type analysis
            COUNT(CASE WHEN sa.trade_type = 'Scalp' THEN 1 END) as scalp_trades,
            COUNT(CASE WHEN sa.trade_type = 'Day Trade' THEN 1 END) as day_trades,
            COUNT(CASE WHEN sa.trade_type = 'Swing Trade' THEN 1 END) as swing_trades,
            COUNT(CASE WHEN sa.trade_type = 'Position Trade' THEN 1 END) as position_trades
            
          FROM `{self.bq_client.dataset_ref}.score_adjustments` sa
          WHERE sa.calculated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
        ),
        
        wallet_tier_performance AS (
          SELECT 
            CASE 
              WHEN current_score < 180 THEN 'Elite'
              WHEN current_score < 220 THEN 'High'
              WHEN current_score < 260 THEN 'Medium'
              ELSE 'Low'
            END as tier,
            COUNT(*) as wallet_count,
            AVG(win_rate) as avg_win_rate,
            AVG(avg_profit_pct) as avg_profit_pct,
            AVG(ABS(total_score_change)) as avg_score_change,
            SUM(CASE WHEN total_score_change < 0 THEN 1 ELSE 0 END) as improving_wallets,
            SUM(CASE WHEN total_score_change > 0 THEN 1 ELSE 0 END) as declining_wallets
          FROM `{self.bq_client.dataset_ref}.wallet_score_updates`
          WHERE update_calculated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
          GROUP BY tier
        ),
        
        system_health AS (
          SELECT 
            COUNT(*) as total_recommendations,
            COUNT(CASE WHEN priority = 'CRITICAL' THEN 1 END) as critical_alerts,
            COUNT(CASE WHEN priority = 'HIGH' THEN 1 END) as high_priority_alerts,
            AVG(trades_analyzed) as avg_trades_per_wallet,
            COUNT(DISTINCT wallet_address) as unique_wallets_with_recommendations
          FROM `{self.bq_client.dataset_ref}.score_update_recommendations`
          WHERE update_calculated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
        )
        
        SELECT 
          'scoring_metrics' as report_section,
          TO_JSON_STRING(STRUCT(
            wallets_analyzed,
            total_score_adjustments,
            avg_adjustment_magnitude,
            profitable_trades,
            unprofitable_trades,
            avg_trade_performance,
            score_improvements,
            score_degradations,
            scalp_trades,
            day_trades,
            swing_trades,
            position_trades
          )) as data
        FROM scoring_metrics
        
        UNION ALL
        
        SELECT 
          'tier_performance' as report_section,
          TO_JSON_STRING(ARRAY_AGG(STRUCT(
            tier,
            wallet_count,
            avg_win_rate,
            avg_profit_pct,
            avg_score_change,
            improving_wallets,
            declining_wallets
          ))) as data
        FROM wallet_tier_performance
        
        UNION ALL
        
        SELECT 
          'system_health' as report_section,
          TO_JSON_STRING(STRUCT(
            total_recommendations,
            critical_alerts,
            high_priority_alerts,
            avg_trades_per_wallet,
            unique_wallets_with_recommendations
          )) as data
        FROM system_health
        """
        
        def _execute_query():
            try:
                results = list(self.bq_client.client.query(report_query).result())
                report = {}
                
                for row in results:
                    section = row['report_section']
                    data = json.loads(row['data']) if row['data'] else {}
                    report[section] = data
                
                # Add summary and metadata
                report['summary'] = {
                    'report_period_days': days_back,
                    'generated_at': datetime.utcnow().isoformat(),
                    'total_wallets_analyzed': report.get('scoring_metrics', {}).get('wallets_analyzed', 0),
                    'total_recommendations': report.get('system_health', {}).get('total_recommendations', 0),
                    'system_status': 'healthy' if report.get('system_health', {}).get('critical_alerts', 0) < 10 else 'attention_needed'
                }
                
                return report
                
            except Exception as e:
                logger.error(f"Error generating performance report: {e}")
                return {"error": str(e)}
        
        import asyncio
        from datetime import datetime
        import json
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.bq_client.executor, _execute_query)
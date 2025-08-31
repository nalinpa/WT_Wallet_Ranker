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
          
          -- Calculate score delta with meaningful scaling
          GREATEST(-300, LEAST(300, 
            ROUND(
              CASE 
                -- Exceptional performance
                WHEN price_change_percent >= 1000 THEN -200
                WHEN price_change_percent >= 500 THEN -150  
                WHEN price_change_percent >= 200 THEN -100
                WHEN price_change_percent >= 100 THEN -75
                
                -- Good performance  
                WHEN price_change_percent >= 50 THEN -50
                WHEN price_change_percent >= 30 THEN -35
                WHEN price_change_percent >= 20 THEN -25
                WHEN price_change_percent >= 10 THEN -15
                WHEN price_change_percent >= 5 THEN -8
                WHEN price_change_percent > 0 THEN -3
                
                -- Losses
                WHEN price_change_percent >= -5 THEN 5
                WHEN price_change_percent >= -10 THEN 12
                WHEN price_change_percent >= -20 THEN 25
                WHEN price_change_percent >= -30 THEN 40
                WHEN price_change_percent >= -50 THEN 65
                WHEN price_change_percent >= -70 THEN 100
                WHEN price_change_percent >= -90 THEN 150
                ELSE 200  -- Near total loss
              END * 
              
              -- Trade type multiplier
              CASE 
                WHEN trade_type = 'Scalp' AND price_change_percent > 10 THEN 1.5
                WHEN trade_type = 'Scalp' AND price_change_percent > 0 THEN 1.2  
                WHEN trade_type = 'Day Trade' AND price_change_percent > 20 THEN 1.3
                WHEN trade_type = 'Swing Trade' AND price_change_percent > 30 THEN 1.4
                WHEN trade_type = 'Position Trade' AND price_change_percent > 50 THEN 1.6
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
          AVG(sa.price_change_percent) as avg_profit_pct,
          SUM(CASE WHEN sa.price_change_percent > 0 THEN 1 ELSE 0 END) as winning_trades,
          SUM(CASE WHEN sa.price_change_percent <= 0 THEN 1 ELSE 0 END) as losing_trades,
          
          -- Calculate win rate
          SUM(CASE WHEN sa.price_change_percent > 0 THEN 1 ELSE 0 END) / COUNT(*) as win_rate,
          
          -- Get current score
          (SELECT wallet_sophistication_score 
           FROM `{self.bq_client.dataset_ref}.transactions` t 
           WHERE t.wallet_address = sa.wallet_address 
           ORDER BY t.timestamp DESC 
           LIMIT 1) as current_score,
          
          CURRENT_TIMESTAMP() as update_calculated_at
          
        FROM `{self.bq_client.dataset_ref}.score_adjustments` sa
        GROUP BY sa.wallet_address
        HAVING COUNT(*) >= {min_trades}
        """
        
        try:
            await self.bq_client.execute_query(query)
            
            # Add suggested new scores
            update_query = f"""
            CREATE OR REPLACE TABLE `{self.bq_client.dataset_ref}.wallet_score_updates` AS
            SELECT *,
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
          
          -- Create recommendation message
          CASE 
            WHEN total_score_change <= -200 THEN 
              CONCAT('🚀🚀 LEGENDARY UPGRADE: Exceptional trading! Win rate: ', 
                     ROUND(win_rate * 100, 1), '%, avg: ', ROUND(avg_profit_pct, 1), 
                     '%. Score: ', CAST(current_score AS STRING), ' → ', 
                     CAST(suggested_new_score AS STRING))
            WHEN total_score_change <= -100 THEN 
              CONCAT('🚀 MAJOR UPGRADE: Outstanding performance! Win rate: ', 
                     ROUND(win_rate * 100, 1), '%. Score: ', CAST(current_score AS STRING), 
                     ' → ', CAST(suggested_new_score AS STRING))
            WHEN total_score_change <= -50 THEN
              CONCAT('✅ SOLID UPGRADE: Strong performance. Win rate: ', 
                     ROUND(win_rate * 100, 1), '%. Score: ', CAST(current_score AS STRING), 
                     ' → ', CAST(suggested_new_score AS STRING))
            WHEN total_score_change <= -25 THEN
              CONCAT('↗️ GOOD PROGRESS: Above average performance. Score: ', 
                     CAST(current_score AS STRING), ' → ', CAST(suggested_new_score AS STRING))
            WHEN total_score_change >= 200 THEN
              CONCAT('💥 MAJOR DOWNGRADE: Severe losses! Avg loss: ', 
                     ROUND(avg_profit_pct, 1), '%. Score: ', CAST(current_score AS STRING), 
                     ' → ', CAST(suggested_new_score AS STRING))
            WHEN total_score_change >= 100 THEN
              CONCAT('⚠️ SERIOUS DOWNGRADE: Poor performance. Win rate: ', 
                     ROUND(win_rate * 100, 1), '%. Score: ', CAST(current_score AS STRING), 
                     ' → ', CAST(suggested_new_score AS STRING))
            WHEN total_score_change >= 50 THEN
              CONCAT('📉 NOTABLE DOWNGRADE: Below average performance. Score: ', 
                     CAST(current_score AS STRING), ' → ', CAST(suggested_new_score AS STRING))
            WHEN total_score_change >= 25 THEN
              CONCAT('⚠️ CAUTION: Underperforming trades. Score: ', 
                     CAST(current_score AS STRING), ' → ', CAST(suggested_new_score AS STRING))
            WHEN ABS(total_score_change) < 25 THEN
              CONCAT('➡️ MAINTAIN: Mixed performance. Score: ', 
                     CAST(current_score AS STRING), ' → ', CAST(suggested_new_score AS STRING))
            ELSE
              CONCAT('📊 STANDARD UPDATE: Score adjustment: ', 
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
import os
import asyncio
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
import logging

from models.requests import (
    TransactionBatch, 
    ScoreCalculationRequest, 
    PriceUpdateRequest
)
from models.responses import (
    HealthResponse,
    PriceUpdateResponse, 
    ScoreCalculationResponse,
    RecommendationsResponse,
    WalletAnalysisResponse,
    TransactionProcessingResponse
)
from services.price_fetcher import PriceFetcher
from services.trade_matcher import TradeMatcher
from services.score_calculator import ScoreCalculator
from services.bigquery_client import BigQueryClient
from utils.helpers import ResponseFormatter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global service instances
bq_client: BigQueryClient = None
price_fetcher: PriceFetcher = None
trade_matcher: TradeMatcher = None
score_calculator: ScoreCalculator = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global bq_client, price_fetcher, trade_matcher, score_calculator
    logger.info("Starting up wallet scoring service...")
    
    try:
        bq_client = BigQueryClient()
        price_fetcher = PriceFetcher()
        trade_matcher = TradeMatcher(bq_client)
        score_calculator = ScoreCalculator(bq_client)
        logger.info("All services initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        raise e
    
    yield
    
    # Shutdown
    logger.info("Shutting down wallet scoring service...")
    if price_fetcher:
        await price_fetcher.close()

# Initialize FastAPI app
app = FastAPI(
    title="Wallet Scoring Service",
    description="Dynamic wallet performance scoring based on actual trading results",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Global exception: {exc}")
    return JSONResponse(
        status_code=500,
        content=ResponseFormatter.error(
            message=f"Internal server error: {str(exc)}",
            error_code="INTERNAL_ERROR"
        )
    )

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow(),
        services={
            "bigquery": "connected" if bq_client else "disconnected",
            "price_fetcher": "ready" if price_fetcher else "not_ready",
            "trade_matcher": "ready" if trade_matcher else "not_ready",
            "score_calculator": "ready" if score_calculator else "not_ready"
        }
    )

@app.post("/update-prices", response_model=PriceUpdateResponse)
async def update_token_prices(
    background_tasks: BackgroundTasks,
    request: Optional[PriceUpdateRequest] = None
):
    """Fetch and update token prices for active tokens"""
    try:
        logger.info("Starting price update process")
        
        # Get active tokens from recent transactions
        hours_back = request.hours_back if request else 24
        active_tokens = await bq_client.get_active_tokens(hours_back)
        logger.info(f"Found {len(active_tokens)} active tokens")
        
        if not active_tokens:
            return PriceUpdateResponse(
                status="success",
                message="No active tokens found",
                prices_updated=0,
                tokens_processed=0,
                failed_tokens=[],
                timestamp=datetime.utcnow()
            )
        
        # Fetch prices for all active tokens
        price_updates = []
        failed_tokens = []
        
        for token in active_tokens:
            try:
                price_data = await price_fetcher.get_token_price(
                    token['address'], 
                    token['symbol']
                )
                if price_data:
                    price_updates.append({
                        'token_address': token['address'],
                        'token_symbol': token['symbol'],
                        'price_usd': price_data['usd'],
                        'price_eth': price_data['eth'],
                        'timestamp': datetime.utcnow(),
                        'volume_24h': price_data.get('volume_24h', 0),
                        'market_cap': price_data.get('market_cap', 0),
                        'source': price_data['source']
                    })
                else:
                    failed_tokens.append(token['symbol'])
            except Exception as e:
                logger.error(f"Error fetching price for {token['symbol']}: {e}")
                failed_tokens.append(token['symbol'])
        
        # Insert price data into BigQuery
        success_count = 0
        if price_updates:
            success = await bq_client.insert_price_data(price_updates)
            if success:
                success_count = len(price_updates)
                logger.info(f"Successfully updated {success_count} token prices")
        
        return PriceUpdateResponse(
            status="success" if success_count > 0 else "partial",
            message=f"Updated {success_count} of {len(active_tokens)} token prices",
            prices_updated=success_count,
            tokens_processed=len(active_tokens),
            failed_tokens=failed_tokens,
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        logger.error(f"Error in price update: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/calculate-scores", response_model=ScoreCalculationResponse)
async def calculate_wallet_scores(request: Optional[ScoreCalculationRequest] = None):
    """Calculate wallet score updates based on recent trades"""
    try:
        logger.info("Starting score calculation process")
        
        # Get request parameters with defaults
        days_back = request.days_back if request else 7
        min_trades = request.min_trades if request else 3
        
        # Step 1: Match trades with price data
        logger.info("Matching trades with price data...")
        matched_trades = await trade_matcher.create_matched_trades_table(days_back)
        logger.info(f"Matched {matched_trades} trade pairs")
        
        # Step 2: Calculate score adjustments
        logger.info("Calculating score adjustments...")
        score_adjustments = await score_calculator.calculate_score_adjustments()
        logger.info(f"Calculated {score_adjustments} score adjustments")
        
        # Step 3: Generate wallet score updates
        logger.info("Generating wallet score updates...")
        wallet_updates = await score_calculator.generate_wallet_updates(min_trades)
        logger.info(f"Generated updates for {wallet_updates} wallets")
        
        # Step 4: Create recommendations
        logger.info("Creating score update recommendations...")
        recommendations = await score_calculator.generate_recommendations()
        logger.info(f"Generated {recommendations} recommendations")
        
        # Get summary of recommendations
        rec_summary = await bq_client.get_recent_recommendations()
        
        return ScoreCalculationResponse(
            status="success",
            message="Score calculation completed successfully",
            matched_trades=matched_trades,
            score_adjustments=score_adjustments,
            wallet_updates=wallet_updates,
            recommendations_generated=recommendations,
            summary=rec_summary,
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        logger.error(f"Error in score calculation: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/recommendations", response_model=RecommendationsResponse)
async def get_score_recommendations(
    priority: str = Query(
        default="HIGH,MEDIUM,CRITICAL",
        description="Comma-separated priority levels"
    ),
    hours_back: int = Query(
        default=24,
        ge=1,
        le=168,
        description="Hours to look back for recommendations"
    ),
    limit: int = Query(
        default=50,
        ge=1,
        le=200,
        description="Maximum number of recommendations to return"
    )
):
    """Get recent score update recommendations"""
    try:
        priority_list = [p.strip().upper() for p in priority.split(',')]
        valid_priorities = {"CRITICAL", "HIGH", "MEDIUM", "LOW", "MINIMAL"}
        priority_list = [p for p in priority_list if p in valid_priorities]
        
        if not priority_list:
            priority_list = ["HIGH", "MEDIUM", "CRITICAL"]
        
        recommendations = await bq_client.get_score_recommendations(
            priority_list, 
            hours_back, 
            limit
        )
        
        return RecommendationsResponse(
            status="success",
            message=f"Retrieved {len(recommendations)} recommendations",
            recommendations=recommendations,
            count=len(recommendations),
            filters={
                "priority": priority_list,
                "hours_back": hours_back,
                "limit": limit
            },
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        logger.error(f"Error getting recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/process-transactions", response_model=TransactionProcessingResponse)
async def process_new_transactions(
    transaction_batch: TransactionBatch,
    background_tasks: BackgroundTasks,
    auto_update_prices: bool = Query(
        default=True,
        description="Automatically update prices for involved tokens"
    )
):
    """Process new transaction data and optionally trigger score updates"""
    try:
        transactions = transaction_batch.transactions
        logger.info(f"Processing {len(transactions)} new transactions")
        
        # Convert Pydantic models to dicts for BigQuery
        transaction_dicts = [tx.dict() for tx in transactions]
        
        # Insert transactions into BigQuery
        success = await bq_client.insert_transactions(transaction_dicts)
        
        if not success:
            raise HTTPException(
                status_code=500, 
                detail="Failed to insert transactions into BigQuery"
            )
        
        # Extract unique tokens and wallets
        unique_tokens = list(set([
            tx.token_address for tx in transactions 
            if tx.token_address and tx.token_address != '0x0000000000000000000000000000000000000000'
        ]))
        unique_wallets = list(set([tx.wallet_address for tx in transactions]))
        
        logger.info(f"Found {len(unique_tokens)} unique tokens and {len(unique_wallets)} unique wallets")
        
        # Automatically update prices for involved tokens if requested
        price_updates_count = 0
        if auto_update_prices and unique_tokens:
            background_tasks.add_task(
                update_prices_for_tokens, 
                unique_tokens, 
                transactions
            )
        
        return TransactionProcessingResponse(
            status="success",
            message=f"Successfully processed {len(transactions)} transactions",
            transactions_processed=len(transactions),
            unique_wallets=len(unique_wallets),
            unique_tokens=len(unique_tokens),
            price_update_triggered=auto_update_prices and len(unique_tokens) > 0,
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        logger.error(f"Error processing transactions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/wallet-analysis/{wallet_address}", response_model=WalletAnalysisResponse)
async def analyze_wallet(
    wallet_address: str = Path(..., description="Ethereum wallet address"),
    days_back: int = Query(
        default=30,
        ge=1,
        le=365,
        description="Number of days to analyze"
    )
):
    """Get detailed analysis for a specific wallet"""
    try:
        # Validate wallet address format
        if not wallet_address.startswith('0x') or len(wallet_address) != 42:
            raise HTTPException(
                status_code=400,
                detail="Invalid wallet address format"
            )
        
        analysis = await bq_client.get_wallet_analysis(wallet_address, days_back)
        
        if "error" in analysis:
            return WalletAnalysisResponse(
                status="error",
                message=analysis["error"],
                wallet_address=wallet_address,
                analysis={},
                timestamp=datetime.utcnow()
            )
        
        return WalletAnalysisResponse(
            status="success",
            message="Wallet analysis completed",
            wallet_address=wallet_address,
            analysis=analysis,
            days_analyzed=days_back,
            timestamp=datetime.utcnow()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing wallet {wallet_address}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_system_stats():
    """Get system statistics and health metrics"""
    try:
        stats = await bq_client.get_system_stats()
        return {
            "status": "success",
            "stats": stats,
            "timestamp": datetime.utcnow()
        }
    except Exception as e:
        logger.error(f"Error getting system stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Background task functions
async def update_prices_for_tokens(token_addresses: List[str], transactions: List):
    """Background task to update prices for specific tokens"""
    try:
        logger.info(f"Background price update for {len(token_addresses)} tokens")
        
        price_updates = []
        for token_addr in token_addresses:
            # Get token symbol from transactions
            token_symbol = next(
                (tx.token_symbol for tx in transactions 
                 if tx.token_address == token_addr), 
                None
            )
            
            if token_symbol:
                try:
                    price_data = await price_fetcher.get_token_price(token_addr, token_symbol)
                    if price_data:
                        price_updates.append({
                            'token_address': token_addr,
                            'token_symbol': token_symbol,
                            'price_usd': price_data['usd'],
                            'price_eth': price_data['eth'],
                            'timestamp': datetime.utcnow(),
                            'volume_24h': price_data.get('volume_24h', 0),
                            'market_cap': price_data.get('market_cap', 0),
                            'source': price_data['source']
                        })
                except Exception as e:
                    logger.error(f"Background price fetch error for {token_symbol}: {e}")
        
        if price_updates:
            await bq_client.insert_price_data(price_updates)
            logger.info(f"Background task updated prices for {len(price_updates)} tokens")
        
    except Exception as e:
        logger.error(f"Background price update task failed: {e}")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        log_level="info",
        access_log=True
    )
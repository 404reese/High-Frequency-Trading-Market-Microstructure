from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import duckdb
from pathlib import Path
import math

app = FastAPI(title="HFT Terminal Backend")

# Enable CORS for the frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For dev, restrict in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
ENRICHED_TRADES_PARQUET = DATA_DIR / "enriched_trades_parquet"
HFT_SIGNALS = DATA_DIR / "HFT_Signals"

# DuckDB connections are instantiated locally per request to be thread-safe in FastAPI

@app.get("/")
def read_root():
    return {"message": "HFT Terminal Backend is running"}

@app.get("/api/health")
def health_check():
    return {"status": "ok"}

@app.get("/api/tickers")
def get_tickers():
    try:
        # Get distinct stocks directly from the parquet partition structures or doing a rapid query
        query = f"SELECT DISTINCT stock FROM read_parquet('{ENRICHED_TRADES_PARQUET.as_posix()}/**/*.parquet', hive_partitioning=1) ORDER BY stock"
        with duckdb.connect() as con:
            df = con.query(query).to_df()
        return {"tickers": df['stock'].tolist()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/summary")
def get_dashboard_summary():
    try:
        # Fetch high-level stock summary
        query = f"""
            SELECT 
                stock, 
                market_cat, 
                SUM(shares) as total_volume, 
                AVG(price) as avg_price, 
                COUNT(*) as trade_count
            FROM read_parquet('{ENRICHED_TRADES_PARQUET.as_posix()}/**/*.parquet', hive_partitioning=1)
            GROUP BY stock, market_cat
            ORDER BY total_volume DESC
        """
        with duckdb.connect() as con:
            df = con.query(query).to_df()
        # Convert to records
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/historical-prices/{stock}")
def get_historical_prices(stock: str):
    try:
        query = f"""
            SELECT 
                CAST(timestamp / 60000000000 AS BIGINT) as minute_bucket,
                MIN(price) as low,
                MAX(price) as high,
                FIRST(price) as open,
                LAST(price) as close,
                SUM(price * shares) / SUM(shares) as vwap,
                SUM(shares) as volume
            FROM read_parquet('{ENRICHED_TRADES_PARQUET.as_posix()}/stock={stock}/**/*.parquet')
            GROUP BY minute_bucket
            ORDER BY minute_bucket
        """
        with duckdb.connect() as con:
            df = con.query(query).to_df()
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/obi-data/{stock}")
def get_obi_data(stock: str):
    try:
        obi_path = HFT_SIGNALS / "obi" / "*.parquet"
        query = f"""
            SELECT minute_bucket, OBI, Buy_Vol, Sell_Vol
            FROM read_parquet('{obi_path.as_posix()}')
            WHERE stock = '{stock}'
            ORDER BY minute_bucket
        """
        with duckdb.connect() as con:
            df = con.query(query).to_df()
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sentiment/{stock}")
def get_sentiment(stock: str):
    # Retrieve the latest OBI, latest VWAP change, and Volatility to infer Bullish/Bearish.
    try:
        obi_path = HFT_SIGNALS / "obi" / "*.parquet"
        vol_path = HFT_SIGNALS / "volatility" / "*.parquet"
        
        obi_query = f"""
            SELECT OBI 
            FROM read_parquet('{obi_path.as_posix()}') 
            WHERE stock = '{stock}' 
            ORDER BY minute_bucket DESC LIMIT 1
        """
        with duckdb.connect() as con:
            obi_df = con.query(obi_query).to_df()
        current_obi = float(obi_df.iloc[0]['OBI']) if not obi_df.empty else 0.0
        
        # We can approximate the predictive ML logic
        # Positive OBI means more buys over sells
        score = 50.0 + (current_obi * 50.0)
        
        # Ensure between 0 and 100
        score = max(0.0, min(100.0, score))
        
        status = "NEUTRAL"
        if score > 65:
            status = "BULLISH"
        elif score < 35:
            status = "BEARISH"
            
        return {
            "stock": stock,
            "sentiment_score": score,
            "status": status,
            "obi": current_obi
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

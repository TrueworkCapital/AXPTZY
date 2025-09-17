from fastapi import FastAPI, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import os

from app.services import DataManager, ZerodhaService, ZerodhaCredentials
from app.schemas import OHLCVOut

from dotenv import load_dotenv

load_dotenv()

def get_data_manager() -> DataManager:
    return DataManager({'data_validation_enabled': True, 'export_formats': ['csv', 'json', 'parquet']})


def get_zerodha_service() -> Optional[ZerodhaService]:
    api_key = os.getenv('ZERODHA_API_KEY')
    api_secret = os.getenv('ZERODHA_API_SECRET')
    if not api_key or not api_secret:
        return None
    return ZerodhaService(ZerodhaCredentials(api_key=api_key, api_secret=api_secret, access_token=os.getenv('ZERODHA_ACCESS_TOKEN')))


app = FastAPI(title="Nifty50 Data API")


@app.get("/health")
def health(dm: DataManager = Depends(get_data_manager)):
    return dm.health_check()


@app.get("/symbols")
def symbols(dm: DataManager = Depends(get_data_manager)):
    return dm.symbols_list


@app.get("/latest/{symbol}", response_model=List[OHLCVOut])
def latest(symbol: str, count: int = Query(100, ge=1, le=5000), dm: DataManager = Depends(get_data_manager)):
    df = dm.get_latest_bars(symbol, count)
    return df.assign(symbol=symbol, data_source='db').to_dict(orient='records')


@app.get("/historical/{symbol}", response_model=List[OHLCVOut])
def historical(symbol: str, start: str, end: str, dm: DataManager = Depends(get_data_manager)):

    try:
        start_dt = datetime.fromisoformat(start)
        end_dt = datetime.fromisoformat(end)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date format. Use ISO 8601.")

    df = dm.get_historical_data(symbol, start_dt, end_dt)
    return df.assign(symbol=symbol, data_source='db').to_dict(orient='records')


@app.post("/export")
def export(symbols: List[str], start: str, end: str, fmt: str = 'csv', dm: DataManager = Depends(get_data_manager)):
    try:
        start_dt = datetime.fromisoformat(start)
        end_dt = datetime.fromisoformat(end)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date format. Use ISO 8601.")
    path = dm.export_data(symbols, start_dt, end_dt, fmt)
    return {"file_path": path}


@app.post("/live/update")
def live_update(symbols: Optional[List[str]] = None, dm: DataManager = Depends(get_data_manager), zs: Optional[ZerodhaService] = Depends(get_zerodha_service)):
    if zs is None:
        raise HTTPException(status_code=400, detail="Zerodha credentials not configured")
    results = dm.fetch_and_store_live_data(zs, symbols)
    return {"updated": sum(1 for v in results.values() if v), "details": results}


class HistoricalIngestBody(BaseModel):
    symbols: List[str]
    start: str
    end: str
    interval: str = "minute"
    validate_only: bool = False  # New parameter for validation-only mode


@app.post("/ingest/historical")
def ingest_historical(payload: HistoricalIngestBody, dm: DataManager = Depends(get_data_manager), zs: Optional[ZerodhaService] = Depends(get_zerodha_service)):
    print(f"🔍 API RECEIVED REQUEST: validate_only={payload.validate_only}")
    if zs is None:
        raise HTTPException(status_code=400, detail="Zerodha credentials not configured")
    try:
        start_dt = datetime.strptime(payload.start, "%Y-%m-%d")
        end_dt = datetime.strptime(payload.end, "%Y-%m-%d")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
    if end_dt < start_dt:
        raise HTTPException(status_code=400, detail="end must be >= start")

    details = {}
    total_rows = 0
    validation_errors = []
    validation_warnings = []
    cumulative_quality_scores = []  # Store all quality scores for cumulative calculation
    total_weighted_score = 0.0  # For weighted average calculation
    all_timestamp_details = []  # Store timestamp details for Excel export
    
    for symbol in payload.symbols:
        df = zs.fetch_historical_data(symbol, start_dt, end_dt, interval=payload.interval)
        if df.empty:
            details[symbol] = {"rows": 0, "stored": False, "validation_passed": True}
            continue
            
        # Validate data before storing
        print(f"🔍 API: About to validate {symbol}, validate_only={payload.validate_only}")
        is_valid, issues, quality_score, timestamp_details = dm.validate_data_quality(df, symbol, skip_logging=payload.validate_only)
        
        if not is_valid:
            details[symbol] = {
                "rows": len(df), 
                "stored": False, 
                "validation_passed": False,
                "validation_issues": issues,
                "quality_score": quality_score
            }
            validation_errors.append(f"{symbol}: {', '.join(issues)}")
            cumulative_quality_scores.append(quality_score)  # Add to cumulative scores
            # Add to weighted calculation
            total_weighted_score += quality_score * len(df)
            # Collect timestamp details
            if timestamp_details:
                timestamp_details['symbol'] = symbol
                all_timestamp_details.append(timestamp_details)
            continue
            
        # Check if this is validation-only mode
        if payload.validate_only:
            # VALIDATION-ONLY MODE: NO STORAGE AT ALL
            print(f"🔍 VALIDATION-ONLY MODE: Skipping storage for {symbol}")
            details[symbol] = {
                "rows": len(df), 
                "stored": False,  # NEVER stored in validation-only mode
                "validation_passed": True,
                "quality_score": quality_score,
                "validation_only": True
            }
            
            # Add validation warnings even if validation passed (quality score >= threshold)
            if issues:
                details[symbol]["validation_warnings"] = issues
                validation_warnings.append(f"{symbol}: {', '.join(issues)}")
            
            # Add to cumulative quality scores
            cumulative_quality_scores.append(quality_score)
            # Add to weighted calculation
            total_weighted_score += quality_score * len(df)
            
            print(f"🔍 DEBUG VALIDATION-ONLY: {symbol} - quality_score={quality_score}, rows={len(df)}, weighted_contribution={quality_score * len(df)}")
            
            # Update total rows for weighted calculation
            total_rows += len(df)
            
            # Collect timestamp details
            if timestamp_details:
                timestamp_details['symbol'] = symbol
                all_timestamp_details.append(timestamp_details)
            
            # Skip storage completely - just continue to next symbol
            continue
        else:
            # Normal mode - store the data
            ok = dm.store_ohlcv_data(df, symbol, data_source='zerodha_kite', skip_validation=True)
            details[symbol] = {
                "rows": len(df), 
                "stored": bool(ok), 
                "validation_passed": True,
                "quality_score": quality_score
            }
            
            # Add validation warnings even if validation passed (quality score >= threshold)
            if issues:
                details[symbol]["validation_warnings"] = issues
                validation_warnings.append(f"{symbol}: {', '.join(issues)}")
            
            # Add to cumulative quality scores
            cumulative_quality_scores.append(quality_score)
            # Add to weighted calculation
            total_weighted_score += quality_score * len(df)
            # Collect timestamp details
            if timestamp_details:
                timestamp_details['symbol'] = symbol
                all_timestamp_details.append(timestamp_details)
        
        total_rows += len(df)
    
    # Calculate both simple average and weighted average quality scores
    simple_average_score = 0.0
    weighted_average_score = 0.0
    
    print(f"🔍 DEBUG: cumulative_quality_scores = {cumulative_quality_scores}")
    print(f"🔍 DEBUG: total_weighted_score = {total_weighted_score}")
    print(f"🔍 DEBUG: total_rows = {total_rows}")
    
    if cumulative_quality_scores:
        # Simple average (equal weight for each symbol)
        simple_average_score = sum(cumulative_quality_scores) / len(cumulative_quality_scores)
        
        # Weighted average (weighted by data volume)
        weighted_average_score = total_weighted_score / total_rows if total_rows > 0 else 0.0
        
        print(f"🔍 DEBUG: simple_average_score = {simple_average_score}")
        print(f"🔍 DEBUG: weighted_average_score = {weighted_average_score}")
    
    response = {
        "symbols": len(payload.symbols), 
        "total_rows": total_rows, 
        "details": details,
        "validation_only": payload.validate_only,
        "cumulative_quality_score": round(weighted_average_score, 6),  # Use weighted average as primary
        "simple_average_quality_score": round(simple_average_score, 6),  # Include simple average for comparison
        "quality_score_method": "weighted_by_data_volume"
    }
    
    if validation_errors:
        response["validation_errors"] = validation_errors
    if validation_warnings:
        response["validation_warnings"] = validation_warnings
    
    # Export timestamp details to Excel if there are any issues
    excel_file_path = None
    if all_timestamp_details:
        excel_file_path = dm.export_timestamp_details_to_excel(all_timestamp_details, payload.start, payload.end)
        response["timestamp_details_excel"] = excel_file_path
        
    return response


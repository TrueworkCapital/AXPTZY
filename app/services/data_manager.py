import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from sqlalchemy.dialects.mysql import insert as mysql_insert

from app.database import SessionLocal, engine
from app.models.ohlcv_data import OHLCV
from app.models.constituents_metadata import ConstituentsMetadata
from app.models.performance_metrics import PerformanceMetrics
from app.models.data_quality_log import DataQualityLog
from app.models.live_data_cache import LiveDataCache
from app.models.export_history import ExportHistory
from app.services.nifty_constituents import NiftyConstituentsManager
from app.validation import DataValidator
from app.services.zerodha_service import ZerodhaService


class DataManager:
    """MySQL-backed Data Manager using SQLAlchemy models."""

    def __init__(self, config: Dict[str, Any]):
        self.logger = logging.getLogger(__name__ + ".DataManager")
        self.config = {
            'data_validation_enabled': True,
            'batch_size': 5000,  # Reduced batch size for better reliability
            'export_formats': ['csv', 'json', 'parquet']
        }
        if config:
            self.config.update(config)

        self.constituents = NiftyConstituentsManager.get_constituents()
        self.symbols_list = NiftyConstituentsManager.get_symbols_list()
        self.sectors = NiftyConstituentsManager.get_sectors()

        self._validation_rules = {
            'price_range': {'min': 0.1, 'max': 200000},
            'volume_min': 0,
            'ohlc_logic': True,
            'time_sequence': True,
            'duplicate_check': True,
            'quality_threshold': 0.95,
            'trading_hours': {
                'start': '09:15:00',
                'end': '15:30:00'  # Extended to 15:30 to include 15:29:xx timestamps
            },
            'check_holidays': True  # Enable holiday and weekend validation
        }

        self._initialize_database_metadata()

        self.validator = DataValidator(self.config, self._validation_rules, self.logger)

        # caching
        self._cache: Dict[str, Tuple[datetime, pd.DataFrame]] = {}
        self._cache_timestamps: Dict[str, datetime] = {}
        self._cache_stats = {'hits': 0, 'misses': 0}
        self._cache_lock = threading.Lock()
        self._max_cache_age = timedelta(minutes=5)
        self._start_cache_cleanup()

    def _initialize_database_metadata(self):
        session: Session = SessionLocal()
        try:
            for symbol, info in self.constituents.items():
                existing = session.get(ConstituentsMetadata, symbol)
                if not existing:
                    session.add(ConstituentsMetadata(
                        symbol=symbol,
                        company_name=info['name'],
                        sector=info['sector'],
                        is_active=True
                    ))
            session.commit()
        except Exception as exc:
            session.rollback()
            self.logger.error(f"Failed to initialize metadata: {exc}")
            raise
        finally:
            session.close()

    def validate_data_quality(self, data: pd.DataFrame, symbol: Optional[str] = None, skip_logging: bool = False) -> Tuple[bool, List[str], float, dict]:
        print(f"🔍 DataManager.validate_data_quality called for {symbol}, skip_logging={skip_logging}")
        is_valid, issues, score, timestamp_details = self.validator.validate_data_quality(data, symbol, skip_logging)
        if issues and symbol and not skip_logging:
            print(f"🔍 About to call _log_quality for {symbol}")
            self._log_quality(symbol, issues, score)
        elif issues and symbol and skip_logging:
            print(f"🔍 Skipping _log_quality for {symbol} (validation-only mode)")
        return is_valid, issues, score, timestamp_details

    def store_ohlcv_data(self, data: pd.DataFrame, symbol: str, data_source: str = 'zerodha_kite', skip_validation: bool = False) -> bool:
        print(f"🚨 STORAGE METHOD CALLED for {symbol} with {len(data)} rows")
        if data.empty:
            return True

        data_copy = data.copy()
        data_copy['symbol'] = symbol
        data_copy['data_source'] = data_source
        data_copy['timestamp'] = pd.to_datetime(data_copy['timestamp'])

        if symbol in self.constituents:
            data_copy['sector'] = self.constituents[symbol]['sector']

        # Only validate if not explicitly skipped (for cases where validation was already done)
        if not skip_validation:
            is_valid, issues, quality_score = self.validate_data_quality(data_copy, symbol)
            if not is_valid:
                self.logger.warning(f"Data validation failed for {symbol}: {issues}")
                return False
        else:
            # If validation was skipped, assume quality score of 1.0
            quality_score = 1.0
            
        data_copy['quality_score'] = quality_score

        # Process data in batches to avoid memory and database limits
        batch_size = self.config.get('batch_size', 10000)
        total_rows = len(data_copy)
        successful_batches = 0
        failed_batches = 0
        
        self.logger.info(f"Storing {total_rows} rows for {symbol} in batches of {batch_size}")
        
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch_data = data_copy.iloc[start_idx:end_idx]
            
            # Convert batch to rows
            rows: List[Dict[str, Any]] = []
            for _, r in batch_data.iterrows():
                rows.append({
                    'timestamp': r['timestamp'].to_pydatetime() if isinstance(r['timestamp'], pd.Timestamp) else r['timestamp'],
                    'symbol': r['symbol'],
                    'open': float(r['open']),
                    'high': float(r['high']),
                    'low': float(r['low']),
                    'close': float(r['close']),
                    'volume': int(r['volume']),
                    'data_source': r.get('data_source', data_source),
                    'quality_score': float(r.get('quality_score', 1.0)),
                    'sector': r.get('sector')
                })

            # Retry logic for database operations
            max_retries = 3
            batch_success = False
            
            for retry_attempt in range(max_retries):
                session: Session = SessionLocal()
                try:
                    stmt = mysql_insert(OHLCV.__table__).values(rows)
                    update_cols = {col: stmt.inserted[col] for col in ['open', 'high', 'low', 'close', 'volume', 'data_source', 'quality_score', 'sector']}
                    ondup = stmt.on_duplicate_key_update(**update_cols)
                    session.execute(ondup)
                    session.commit()
                    successful_batches += 1
                    batch_success = True
                    self.logger.debug(f"Successfully stored batch {start_idx}-{end_idx-1} for {symbol}")
                    break
                except Exception as exc:
                    session.rollback()
                    if retry_attempt < max_retries - 1:
                        self.logger.warning(f"Batch {start_idx}-{end_idx-1} for {symbol} failed (attempt {retry_attempt + 1}/{max_retries}): {exc}. Retrying...")
                        # Wait before retry (exponential backoff)
                        import time
                        time.sleep(2 ** retry_attempt)
                    else:
                        failed_batches += 1
                        self.logger.error(f"Failed to store batch {start_idx}-{end_idx-1} for {symbol} after {max_retries} attempts: {exc}")
                finally:
                    session.close()
        
        # Invalidate caches for symbol after all batches are processed
        if successful_batches > 0:
            self._invalidate_cache(symbol)
        
        # Return True if at least some batches were successful
        if successful_batches > 0:
            if failed_batches > 0:
                self.logger.warning(f"Partial success for {symbol}: {successful_batches} batches succeeded, {failed_batches} failed")
            return True
        else:
            self.logger.error(f"All batches failed for {symbol}")
            return False

    def get_latest_bars(self, symbol: str, count: int = 100) -> pd.DataFrame:
        cache_key = f"latest_{symbol}_{count}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return cached

        session: Session = SessionLocal()
        try:
            q = (
                session.query(OHLCV)
                .filter(OHLCV.symbol == symbol)
                .order_by(OHLCV.timestamp.desc())
                .limit(count)
            )
            rows = list(q)
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame([
                {
                    'timestamp': r.timestamp,
                    'open': r.open,
                    'high': r.high,
                    'low': r.low,
                    'close': r.close,
                    'volume': r.volume,
                    'quality_score': r.quality_score,
                    'sector': r.sector,
                }
                for r in rows
            ]).sort_values('timestamp')
            df = df.reset_index(drop=True)
            self._store_in_cache(cache_key, df)
            return df
        finally:
            session.close()

    def get_historical_data(self, symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        session: Session = SessionLocal()
        try:
            rows = (
                session.query(OHLCV)
                .filter(OHLCV.symbol == symbol)
                .filter(OHLCV.timestamp >= start_date)
                .filter(OHLCV.timestamp <= end_date)
                .order_by(OHLCV.timestamp.asc())
                .all()
            )
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame([
                {
                    'timestamp': r.timestamp,
                    'open': r.open,
                    'high': r.high,
                    'low': r.low,
                    'close': r.close,
                    'volume': r.volume,
                    'quality_score': r.quality_score,
                    'sector': r.sector,
                }
                for r in rows
            ])
            return df
        finally:
            session.close()

    def export_data(self, symbols: List[str], start_date: datetime, end_date: datetime, fmt: str = 'csv', file_path: Optional[str] = None) -> str:
        if fmt not in self.config['export_formats']:
            raise ValueError(f"Unsupported format: {fmt}")

        all_data: List[pd.DataFrame] = []
        for s in symbols:
            df = self.get_historical_data(s, start_date, end_date)
            if not df.empty:
                df['symbol'] = s
                all_data.append(df)
        if not all_data:
            raise ValueError("No data to export")

        combined = pd.concat(all_data, ignore_index=True).sort_values(['symbol', 'timestamp'])

        if not file_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            symbols_str = '_'.join(symbols[:3]) + ('_etc' if len(symbols) > 3 else '')
            file_path = f"exports/nifty50_data_{symbols_str}_{timestamp}.{fmt}"

        if fmt == 'csv':
            combined.to_csv(file_path, index=False)
        elif fmt == 'json':
            combined.to_json(file_path, orient='records', date_format='iso')
        elif fmt == 'parquet':
            combined.to_parquet(file_path, index=False)

        size_bytes = 0
        try:
            import os
            size_bytes = os.path.getsize(file_path)
        except Exception:
            pass

        session: Session = SessionLocal()
        try:
            session.add(ExportHistory(
                export_type='historical_data',
                symbols=','.join(symbols),
                date_range_start=start_date.date(),
                date_range_end=end_date.date(),
                format=fmt,
                file_path=file_path,
                file_size_mb=(size_bytes / (1024 * 1024)) if size_bytes else None
            ))
            session.commit()
        finally:
            session.close()

        return file_path

    def export_timestamp_details_to_excel(self, timestamp_details: List[dict], start_date: str, end_date: str) -> str:
        """Export timestamp details (non-trading days and missing intervals) to Excel file."""
        import pandas as pd
        from datetime import datetime
        import os
        
        # Create exports directory if it doesn't exist
        exports_dir = "exports"
        os.makedirs(exports_dir, exist_ok=True)
        
        # Generate filename
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"timestamp_details_{start_date}_to_{end_date}_{timestamp_str}.xlsx"
        file_path = os.path.join(exports_dir, filename)
        
        # Prepare data for Excel export
        non_trading_data = []
        non_trading_ohlcv_data = []
        gap_data = []
        missing_consecutive_data = []
        
        for symbol_data in timestamp_details:
            symbol = symbol_data.get('symbol', 'Unknown')
            
            # Process non-trading timestamps
            for item in symbol_data.get('non_trading_timestamps', []):
                timestamp = item['timestamp']
                # Convert to timezone-naive if needed
                if timestamp.tzinfo is not None:
                    timestamp = timestamp.replace(tzinfo=None)
                
                non_trading_data.append({
                    'Symbol': symbol,
                    'Timestamp': timestamp,
                    'Date': timestamp.date(),
                    'Time': timestamp.time(),
                    'Reason': item['reason'],
                    'Day': item.get('day', ''),
                    'Holiday_Name': item.get('holiday_name', '')
                })
            
            # Process non-trading OHLCV data
            for item in symbol_data.get('non_trading_ohlcv_data', []):
                timestamp = item['timestamp']
                # Convert to timezone-naive if needed
                if timestamp.tzinfo is not None:
                    timestamp = timestamp.replace(tzinfo=None)
                
                non_trading_ohlcv_data.append({
                    'Symbol': symbol,
                    'Timestamp': timestamp,
                    'Date': timestamp.date(),
                    'Time': timestamp.time(),
                    'Reason': item['reason'],
                    'Day': item.get('day', ''),
                    'Holiday_Name': item.get('holiday_name', ''),
                    'Open': item.get('open', None),
                    'High': item.get('high', None),
                    'Low': item.get('low', None),
                    'Close': item.get('close', None),
                    'Volume': item.get('volume', None)
                })
            
            # Process gap details
            for item in symbol_data.get('gap_details', []):
                gap_start = item['gap_start']
                gap_end = item['gap_end']
                
                # Convert to timezone-naive if needed
                if gap_start.tzinfo is not None:
                    gap_start = gap_start.replace(tzinfo=None)
                if gap_end.tzinfo is not None:
                    gap_end = gap_end.replace(tzinfo=None)
                
                gap_data.append({
                    'Symbol': symbol,
                    'Gap_Start': gap_start,
                    'Gap_End': gap_end,
                    'Gap_Duration_Minutes': item['gap_duration_minutes'],
                    'Missing_Intervals': item['missing_intervals'],
                    'Expected_Interval_Minutes': item['expected_interval_minutes']
                })
            
            # Process missing consecutive minutes
            for item in symbol_data.get('missing_consecutive_minutes', []):
                missing_timestamp = item['missing_timestamp']
                prev_timestamp = item['prev_timestamp']
                next_timestamp = item['next_timestamp']
                
                # Convert to timezone-naive if needed
                if missing_timestamp.tzinfo is not None:
                    missing_timestamp = missing_timestamp.replace(tzinfo=None)
                if prev_timestamp.tzinfo is not None:
                    prev_timestamp = prev_timestamp.replace(tzinfo=None)
                if next_timestamp.tzinfo is not None:
                    next_timestamp = next_timestamp.replace(tzinfo=None)
                
                missing_consecutive_data.append({
                    'Symbol': symbol,
                    'Missing_Timestamp': missing_timestamp,
                    'Previous_Timestamp': prev_timestamp,
                    'Next_Timestamp': next_timestamp,
                    'Gap_Duration_Minutes': item['gap_duration_minutes']
                })
        
        # Create Excel file with multiple sheets
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            if non_trading_data:
                non_trading_df = pd.DataFrame(non_trading_data)
                non_trading_df.to_excel(writer, sheet_name='Non_Trading_Days', index=False)
            
            if non_trading_ohlcv_data:
                non_trading_ohlcv_df = pd.DataFrame(non_trading_ohlcv_data)
                non_trading_ohlcv_df.to_excel(writer, sheet_name='Non_Trading_OHLCV', index=False)
            
            if gap_data:
                gap_df = pd.DataFrame(gap_data)
                gap_df.to_excel(writer, sheet_name='Missing_Intervals', index=False)
            
            if missing_consecutive_data:
                missing_consecutive_df = pd.DataFrame(missing_consecutive_data)
                missing_consecutive_df.to_excel(writer, sheet_name='Missing_Consecutive_Minutes', index=False)
            
            # Create summary sheet
            summary_data = {
                'Metric': [
                    'Total Non-Trading Timestamps', 
                    'Total Non-Trading OHLCV Records',
                    'Total Missing Intervals', 
                    'Total Missing Consecutive Minutes',
                    'Date Range'
                ],
                'Value': [
                    len(non_trading_data),
                    len(non_trading_ohlcv_data),
                    len(gap_data),
                    len(missing_consecutive_data),
                    f"{start_date} to {end_date}"
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        self.logger.info(f"Timestamp details exported to: {file_path}")
        return file_path

    def _log_performance(self, operation: str, symbol: Optional[str], duration_ms: float, success: bool, records: int = 0):
        session: Session = SessionLocal()
        try:
            session.add(PerformanceMetrics(
                operation=operation,
                symbol=symbol,
                duration_ms=duration_ms,
                records_affected=records,
                success=success
            ))
            session.commit()
        except Exception:
            session.rollback()
        finally:
            session.close()

    def _log_quality(self, symbol: str, issues: List[str], quality_score: float):
        session: Session = SessionLocal()
        try:
            session.add(DataQualityLog(
                symbol=symbol,
                quality_score=quality_score,
                issues_found='; '.join(issues),
                severity=1 if quality_score > 0.8 else 2 if quality_score > 0.5 else 3
            ))
            session.commit()
        except Exception:
            session.rollback()
        finally:
            session.close()


    # caching helpers
    def _get_from_cache(self, key: str) -> Optional[pd.DataFrame]:
        with self._cache_lock:
            if key in self._cache:
                cache_time, data = self._cache[key]
                if datetime.now() - cache_time < self._max_cache_age:
                    self._cache_stats['hits'] += 1
                    return data.copy()
                else:
                    del self._cache[key]
                    self._cache_timestamps.pop(key, None)
            self._cache_stats['misses'] += 1
            return None

    def _store_in_cache(self, key: str, data: pd.DataFrame):
        with self._cache_lock:
            self._cache[key] = (datetime.now(), data.copy())
            self._cache_timestamps[key] = datetime.now()
            if len(self._cache) > 500:
                oldest_key = min(self._cache_timestamps.keys(), key=lambda k: self._cache_timestamps[k])
                self._cache.pop(oldest_key, None)
                self._cache_timestamps.pop(oldest_key, None)

    def _invalidate_cache(self, symbol: str):
        with self._cache_lock:
            keys_to_remove = [k for k in list(self._cache.keys()) if symbol in k]
            for k in keys_to_remove:
                self._cache.pop(k, None)
                self._cache_timestamps.pop(k, None)

    def _start_cache_cleanup(self):
        def cleanup_cache():
            while True:
                try:
                    time.sleep(300)
                    now = datetime.now()
                    with self._cache_lock:
                        expired = [k for k, (t, _) in self._cache.items() if now - t > self._max_cache_age]
                        for k in expired:
                            self._cache.pop(k, None)
                            self._cache_timestamps.pop(k, None)
                        if expired:
                            self.logger.info(f"Cleaned {len(expired)} expired cache entries")
                except Exception as exc:
                    self.logger.error(f"Cache cleanup error: {exc}")
        threading.Thread(target=cleanup_cache, daemon=True).start()

    # additional features migrated from original module
    def get_latest_bars_all_symbols(self, count: int = 100) -> Dict[str, pd.DataFrame]:
        result: Dict[str, pd.DataFrame] = {}
        for symbol in self.symbols_list:
            result[symbol] = self.get_latest_bars(symbol, count)
        return result

    def get_sector_data(self, sector: str, count: int = 100) -> Dict[str, pd.DataFrame]:
        if sector not in self.sectors:
            raise ValueError(f"Unknown sector: {sector}")
        data: Dict[str, pd.DataFrame] = {}
        for symbol in self.sectors[sector]:
            data[symbol] = self.get_latest_bars(symbol, count)
        return data

    def fetch_and_store_live_data(self, zs: Optional[ZerodhaService], symbols: Optional[List[str]] = None) -> Dict[str, bool]:
        if not zs or not zs.credentials.access_token:
            self.logger.error("Zerodha session not established")
            return {}
        symbols = symbols or self.symbols_list
        live = zs.fetch_live_quotes(symbols)
        if not live:
            return {}
        session: Session = SessionLocal()
        results: Dict[str, bool] = {}
        try:
            for symbol, data in live.items():
                try:
                    session.merge(LiveDataCache(
                        symbol=symbol,
                        timestamp=datetime.now(),
                        open=data.get('open', 0.0),
                        high=data.get('high', 0.0),
                        low=data.get('low', 0.0),
                        close=data.get('close', 0.0),
                        volume=data.get('volume', 0)
                    ))
                    results[symbol] = True
                except Exception as exc:
                    self.logger.error(f"Error storing live data for {symbol}: {exc}")
                    results[symbol] = False
            session.commit()
        finally:
            session.close()
        return results

    def get_performance_stats(self) -> Dict[str, Any]:
        cache_hit_rate = 0.0
        if (self._cache_stats['hits'] + self._cache_stats['misses']) > 0:
            cache_hit_rate = (self._cache_stats['hits'] / (self._cache_stats['hits'] + self._cache_stats['misses'])) * 100.0
        return {
            'cache_hit_rate_pct': round(cache_hit_rate, 2),
            'cache_entries': len(self._cache),
            'total_symbols': len(self.symbols_list),
        }

    def health_check(self) -> Dict[str, Any]:
        health = {
            'overall_status': 'healthy',
            'database_connection': False,
            'cache_status': 'ok',
            'issues': []
        }
        try:
            session: Session = SessionLocal()
            try:
                session.query(func.count(OHLCV.id)).first()
                health['database_connection'] = True
            finally:
                session.close()

            stats = self.get_performance_stats()
            if stats['cache_hit_rate_pct'] < 60:
                health['cache_status'] = 'warning'
                health['issues'].append('Low cache hit rate')
            if health['issues']:
                health['overall_status'] = 'warning' if len(health['issues']) < 3 else 'unhealthy'
        except Exception as exc:
            health['overall_status'] = 'unhealthy'
            health['issues'].append(str(exc))
        return health

    def close(self):
        with self._cache_lock:
            self._cache.clear()
            self._cache_timestamps.clear()



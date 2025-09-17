import pandas as pd
import numpy as np
from datetime import timedelta
from typing import List, Tuple
import holidays

class DataValidator:
    def __init__(self, config, validation_rules, logger):
        self.config = config
        self._validation_rules = validation_rules
        self.logger = logger

    def _log_data_quality_issues(self, symbol: str, issues: List[str], quality_score: float):
        # Implement logging to database or console
        print(f"🚨 LOGGING METHOD CALLED for {symbol} - THIS SHOULD NOT HAPPEN IN VALIDATION-ONLY MODE!")
        print(f"🔍 Data quality issues for {symbol}: {issues}, Score: {quality_score}")
        if self.logger:
            self.logger.warning(f"Data quality issues for {symbol}: {issues}, Score: {quality_score}")

    def _is_trading_time(self, timestamp, trading_start, trading_end, ind_holidays):
        """Check if a timestamp is within trading hours and trading days."""
        if pd.isna(timestamp):
            return False
        
        # Check if it's a weekend (Saturday=5, Sunday=6)
        if timestamp.weekday() >= 5:
            return False
        
        # Check if it's a holiday
        if timestamp.date() in ind_holidays:
            return False
        
        # Check if it's within trading hours
        time_only = timestamp.time()
        return trading_start <= time_only <= trading_end

    def _get_trading_data_only(self, data, trading_start, trading_end, ind_holidays):
        """Filter data to only include trading hours and trading days."""
        if 'timestamp' not in data.columns:
            return data
        
        # Create mask for trading times
        trading_mask = data['timestamp'].apply(
            lambda ts: self._is_trading_time(ts, trading_start, trading_end, ind_holidays)
        )
        
        return data[trading_mask].copy()

    def _calculate_expected_trading_intervals(self, data_sorted, trading_start, trading_end, ind_holidays):
        """Calculate expected intervals only for trading hours and trading days."""
        if len(data_sorted) < 2:
            return None, 0
        
        # Filter to only trading data
        trading_data = self._get_trading_data_only(data_sorted, trading_start, trading_end, ind_holidays)
        
        if len(trading_data) < 2:
            return None, 0
        
        # Calculate time differences for trading data only
        time_diffs = trading_data['timestamp'].diff().dropna()
        
        if len(time_diffs) == 0:
            return None, 0
        
        # Get the most common interval (mode)
        most_common_interval = time_diffs.mode()
        if len(most_common_interval) > 0:
            expected_interval = most_common_interval.iloc[0]
            return expected_interval, len(trading_data)
        
        return None, 0

    def validate_data_quality(self, data: pd.DataFrame, symbol: str = None, skip_logging: bool = False) -> Tuple[bool, List[str], float, dict]:
        """Comprehensive data validation with quality scoring."""
        
        if not self.config.get('data_validation_enabled', True):
            return True, [], 1.0, {}

        print(f"🔍 Validating data for {symbol}: {len(data)} rows")
        issues = []
        quality_scores = []
        timestamp_details = {
            'non_trading_timestamps': [],
            'non_trading_ohlcv_data': [],
            'missing_interval_timestamps': [],
            'gap_details': [],
            'missing_consecutive_minutes': []
        }

        try:
            # Basic structure validation
            required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            missing_columns = [col for col in required_columns if col not in data.columns]
            if missing_columns:
                issues.append(f"Missing columns: {missing_columns}")
                quality_scores.append(0.0)
            else:
                quality_scores.append(1.0)

            if data.empty:
                issues.append("Empty dataset")
                return False, issues, 0.0

            # OHLC logic validation
            if self._validation_rules['ohlc_logic']:
                ohlc_violations = 0

                # High >= max(Open, Close, Low)
                high_violations = (
                    (data['high'] < data['open']) |
                    (data['high'] < data['close']) |
                    (data['high'] < data['low'])
                ).sum()
                ohlc_violations += high_violations

                # Low <= min(Open, Close, High)
                low_violations = (
                    (data['low'] > data['open']) |
                    (data['low'] > data['close']) |
                    (data['low'] > data['high'])
                ).sum()
                ohlc_violations += low_violations

                if ohlc_violations > 0:
                    issues.append(f"OHLC logic violations: {ohlc_violations}")
                    quality_scores.append(max(0, 1 - (ohlc_violations / len(data))))
                else:
                    quality_scores.append(1.0)

            # Price range validation
            price_cols = ['open', 'high', 'low', 'close']
            price_quality = 1.0
            for col in price_cols:
                out_of_range = (
                    (data[col] < self._validation_rules['price_range']['min']) |
                    (data[col] > self._validation_rules['price_range']['max'])
                ).sum()

                if out_of_range > 0:
                    issues.append(f"{col} out of range: {out_of_range} bars")
                    price_quality *= max(0, 1 - (out_of_range / len(data)))

            quality_scores.append(price_quality)

            # Volume validation
            negative_volume = (data['volume'] < self._validation_rules['volume_min']).sum()
            if negative_volume > 0:
                issues.append(f"Invalid volume: {negative_volume} bars")
                quality_scores.append(max(0, 1 - (negative_volume / len(data))))
            else:
                quality_scores.append(1.0)

            # Trading hours validation
            if 'timestamp' in data.columns:
                # Get trading hours from validation rules
                trading_start = pd.Timestamp(self._validation_rules.get('trading_hours', {}).get('start', '09:15:00')).time()
                trading_end = pd.Timestamp(self._validation_rules.get('trading_hours', {}).get('end', '15:29:00')).time()
                
                # Check if timestamps are within valid trading hours
                invalid_hours = 0
                for timestamp in data['timestamp']:
                    if pd.notna(timestamp):
                        # Convert to time only (ignore date)
                        time_only = timestamp.time()
                        # Check if outside trading hours
                        if time_only < trading_start or time_only > trading_end:
                            invalid_hours += 1
                
                if invalid_hours > 0:
                    issues.append(f"Timestamps outside trading hours ({trading_start.strftime('%H:%M')}-{trading_end.strftime('%H:%M')}): {invalid_hours}")
                    quality_scores.append(max(0, 1 - (invalid_hours / len(data))))
                    print(f"🔍 Found {invalid_hours} timestamps outside trading hours in {symbol}")
                else:
                    quality_scores.append(1.0)
                
                # Holiday and weekend validation
                if self._validation_rules.get('check_holidays', True):
                    # Get years from the data to initialize holidays
                    years = set()
                    for timestamp in data['timestamp']:
                        if pd.notna(timestamp):
                            years.add(timestamp.year)
                    
                    if years:
                        # Initialize India holidays for the required years
                        ind_holidays = holidays.India(years=list(years))
                        
                        # Check for holidays and weekends
                        non_trading_days = 0
                        for idx, row in data.iterrows():
                            timestamp = row['timestamp']
                            if pd.notna(timestamp):
                                # Check if it's a weekend (Saturday=5, Sunday=6)
                                if timestamp.weekday() >= 5:  # Weekend
                                    non_trading_days += 1
                                    timestamp_details['non_trading_timestamps'].append({
                                        'timestamp': timestamp,
                                        'reason': 'weekend',
                                        'day': timestamp.strftime('%A')
                                    })
                                    # Capture the actual OHLCV data
                                    timestamp_details['non_trading_ohlcv_data'].append({
                                        'timestamp': timestamp,
                                        'reason': 'weekend',
                                        'day': timestamp.strftime('%A'),
                                        'open': row.get('open'),
                                        'high': row.get('high'),
                                        'low': row.get('low'),
                                        'close': row.get('close'),
                                        'volume': row.get('volume')
                                    })
                                # Check if it's a holiday
                                elif timestamp.date() in ind_holidays:
                                    non_trading_days += 1
                                    timestamp_details['non_trading_timestamps'].append({
                                        'timestamp': timestamp,
                                        'reason': 'holiday',
                                        'holiday_name': ind_holidays.get(timestamp.date(), 'Unknown Holiday')
                                    })
                                    # Capture the actual OHLCV data
                                    timestamp_details['non_trading_ohlcv_data'].append({
                                        'timestamp': timestamp,
                                        'reason': 'holiday',
                                        'holiday_name': ind_holidays.get(timestamp.date(), 'Unknown Holiday'),
                                        'open': row.get('open'),
                                        'high': row.get('high'),
                                        'low': row.get('low'),
                                        'close': row.get('close'),
                                        'volume': row.get('volume')
                                    })
                        
                        if non_trading_days > 0:
                            issues.append(f"Data on non-trading days (weekends/holidays): {non_trading_days}")
                            quality_scores.append(max(0, 1 - (non_trading_days / len(data))))
                            print(f"🔍 Found {non_trading_days} timestamps on non-trading days in {symbol}")
                        else:
                            quality_scores.append(1.0)

            # Time sequence validation and missing timestamp check
            if self._validation_rules['time_sequence'] and len(data) > 1:
                if 'timestamp' in data.columns:
                    # Check for missing/null timestamps
                    null_timestamps = data['timestamp'].isnull().sum()
                    # Also check for NaT (Not a Time) values
                    nat_timestamps = pd.isna(data['timestamp']).sum()
                    total_missing = max(null_timestamps, nat_timestamps)
                    
                    if total_missing > 0:
                        issues.append(f"Missing timestamps: {total_missing}")
                        quality_scores.append(max(0, 1 - (total_missing / len(data))))
                    else:
                        quality_scores.append(1.0)
                    
                    # Check for time sequence errors (non-increasing timestamps)
                    data_sorted = data.sort_values('timestamp')
                    time_errors = (data_sorted['timestamp'].diff() <= timedelta(0)).sum()
                    if time_errors > 0:
                        issues.append(f"Time sequence errors: {time_errors}")
                        quality_scores.append(max(0, 1 - (time_errors / len(data))))
                    else:
                        quality_scores.append(1.0)
                    
                    # Check for missing time intervals (gaps in sequence) - ONLY for trading hours and trading days
                    if len(data_sorted) > 1:
                        # Get trading hours and holidays for validation
                        trading_start = pd.Timestamp(self._validation_rules.get('trading_hours', {}).get('start', '09:15:00')).time()
                        trading_end = pd.Timestamp(self._validation_rules.get('trading_hours', {}).get('end', '15:29:00')).time()
                        
                        # Get years from the data to initialize holidays
                        years = set()
                        for timestamp in data_sorted['timestamp']:
                            if pd.notna(timestamp):
                                years.add(timestamp.year)
                        
                        ind_holidays = holidays.India(years=list(years)) if years else set()
                        
                        # Calculate expected interval based on trading data only
                        expected_interval, trading_data_count = self._calculate_expected_trading_intervals(
                            data_sorted, trading_start, trading_end, ind_holidays
                        )
                        
                        if expected_interval and trading_data_count > 1:
                            # Filter to only trading data for gap analysis
                            trading_data = self._get_trading_data_only(data_sorted, trading_start, trading_end, ind_holidays)
                            trading_data = trading_data.sort_values('timestamp')
                            
                            # Calculate time differences for trading data only
                            time_diffs = trading_data['timestamp'].diff().dropna()
                            
                            if len(time_diffs) > 0:
                                # Check for gaps larger than expected interval
                                # For minute data, be more strict - only allow 10% tolerance
                                tolerance = 1.1 if expected_interval <= timedelta(minutes=1) else 1.5
                                large_gaps = time_diffs > (expected_interval * tolerance)
                                gap_count = large_gaps.sum()
                                
                                if gap_count > 0:
                                    # Calculate how many missing intervals this represents
                                    missing_intervals = 0
                                    actual_gap_count = 0
                                    # Get the actual indices where gaps occur
                                    gap_indices = time_diffs[large_gaps].index
                                    gap_values = time_diffs[large_gaps]
                                    
                                    for i, (gap_idx, gap) in enumerate(zip(gap_indices, gap_values)):
                                        if expected_interval.total_seconds() > 0:
                                            # Calculate missing intervals for this gap
                                            gap_missing = int((gap.total_seconds() / expected_interval.total_seconds()) - 1)
                                            # Only count reasonable gaps (not huge gaps that span days/weeks)
                                            if gap_missing > 0 and gap_missing <= 1000:  # Reasonable limit per gap
                                                missing_intervals += gap_missing
                                                actual_gap_count += 1
                                                
                                                # Find the actual timestamps for this gap
                                                gap_start_time = trading_data.iloc[gap_idx]['timestamp']
                                                gap_end_time = trading_data.iloc[gap_idx + 1]['timestamp']
                                                
                                                # Show only actual gaps
                                                print(f"🔍 GAP {actual_gap_count}: {gap_start_time} → {gap_end_time} (Duration: {int(gap.total_seconds()/60)} min, Missing: {gap_missing} intervals)")
                                                
                                                timestamp_details['gap_details'].append({
                                                    'gap_start': gap_start_time,
                                                    'gap_end': gap_end_time,
                                                    'gap_duration_minutes': int(gap.total_seconds() / 60),
                                                    'missing_intervals': gap_missing,
                                                    'expected_interval_minutes': int(expected_interval.total_seconds() / 60)
                                                })
                                    
                                    # Cap total missing intervals to a reasonable maximum relative to dataset size
                                    max_reasonable_missing = min(trading_data_count // 2, 10000)  # Max 50% of data or 10k
                                    missing_intervals = min(missing_intervals, max_reasonable_missing)
                                    
                                    if missing_intervals > 0:
                                        issues.append(f"Missing time intervals (trading hours only): {actual_gap_count} gaps detected ({missing_intervals} missing data points)")
                                        quality_scores.append(max(0, 1 - (actual_gap_count / trading_data_count)))
                                        print(f"🔍 Found {actual_gap_count} time gaps in {symbol} with {missing_intervals} missing data points (trading hours only)")
                                    else:
                                        # If calculated missing intervals are unreasonable, just report gap count
                                        issues.append(f"Missing time intervals (trading hours only): {actual_gap_count} gaps detected")
                                        quality_scores.append(max(0, 1 - (actual_gap_count / trading_data_count)))
                                        print(f"🔍 Found {actual_gap_count} time gaps in {symbol} (missing intervals calculation was unreasonable)")
                                else:
                                    quality_scores.append(1.0)
                            else:
                                quality_scores.append(1.0)
                        else:
                            quality_scores.append(1.0)
                    
                    # Additional check: For minute-level data, ensure we have consecutive minutes within trading hours and trading days
                    if len(data_sorted) > 1:
                        # Check if this looks like minute-level data
                        sample_diffs = data_sorted['timestamp'].diff().dropna().head(10)
                        if len(sample_diffs) > 0 and sample_diffs.min() <= timedelta(minutes=2):
                            # This appears to be minute-level data
                            # Get trading hours and holidays for validation
                            trading_start = pd.Timestamp(self._validation_rules.get('trading_hours', {}).get('start', '09:15:00')).time()
                            trading_end = pd.Timestamp(self._validation_rules.get('trading_hours', {}).get('end', '15:29:00')).time()
                            
                            # Get years from the data to initialize holidays
                            years = set()
                            for timestamp in data_sorted['timestamp']:
                                if pd.notna(timestamp):
                                    years.add(timestamp.year)
                            
                            ind_holidays = holidays.India(years=list(years)) if years else set()
                            
                            # Filter to only trading data for consecutive minute analysis
                            trading_data = self._get_trading_data_only(data_sorted, trading_start, trading_end, ind_holidays)
                            trading_data = trading_data.sort_values('timestamp')
                            
                            if len(trading_data) > 1:
                                consecutive_minutes = 0
                                for i in range(1, len(trading_data)):
                                    prev_time = trading_data.iloc[i-1]['timestamp']
                                    curr_time = trading_data.iloc[i]['timestamp']
                                    
                                    # Check if both timestamps are on the same trading day
                                    if prev_time.date() == curr_time.date():
                                        # For minute-level data, check if we're missing any minute intervals
                                        # Allow for seconds within the same minute
                                        prev_minute = prev_time.replace(second=0, microsecond=0)
                                        curr_minute = curr_time.replace(second=0, microsecond=0)
                                        
                                        expected_next_minute = prev_minute + timedelta(minutes=1)
                                        
                                        # If current minute is more than 1 minute ahead, count missing minutes
                                        if curr_minute > expected_next_minute:
                                            missing_minutes = int((curr_minute - expected_next_minute).total_seconds() / 60)
                                            consecutive_minutes += missing_minutes
                                            
                                            # Capture the specific missing minute intervals
                                            for j in range(1, missing_minutes + 1):
                                                missing_timestamp = expected_next_minute + timedelta(minutes=j-1)
                                                timestamp_details['missing_consecutive_minutes'].append({
                                                    'missing_timestamp': missing_timestamp,
                                                    'prev_timestamp': prev_time,
                                                    'next_timestamp': curr_time,
                                                    'gap_duration_minutes': missing_minutes
                                                })
                                
                                if consecutive_minutes > 0:
                                    issues.append(f"Missing consecutive minutes within trading hours: {consecutive_minutes} missing minute intervals")
                                    quality_scores.append(max(0, 1 - (consecutive_minutes / len(trading_data))))
                                    print(f"🔍 Found {consecutive_minutes} missing consecutive minute intervals within trading hours in {symbol}")
                                else:
                                    quality_scores.append(1.0)
                            else:
                                quality_scores.append(1.0)

            # Duplicate check
            if self._validation_rules['duplicate_check']:
                if 'timestamp' in data.columns:
                    duplicates = data.duplicated(subset=['timestamp']).sum()
                    if duplicates > 0:
                        issues.append(f"Duplicate timestamps: {duplicates}")
                        quality_scores.append(max(0, 1 - (duplicates / len(data))))
                    else:
                        quality_scores.append(1.0)

            # Calculate overall quality score
            overall_quality = np.mean(quality_scores) if quality_scores else 0.0

            # Log quality issues if any (skip logging in validation-only mode)
            if issues and symbol and not skip_logging:
                print(f"🔍 About to log quality issues for {symbol}, skip_logging={skip_logging}")
                self._log_data_quality_issues(symbol, issues, overall_quality)
            elif issues and symbol and skip_logging:
                print(f"🔍 Skipping quality logging for {symbol} (validation-only mode)")

            is_valid = overall_quality >= self._validation_rules['quality_threshold']

            return is_valid, issues, overall_quality, timestamp_details

        except Exception as e:
            self.logger.error(f"❌ Data validation error: {e}")
            return False, [f"Validation failed: {str(e)}"], 0.0, {}

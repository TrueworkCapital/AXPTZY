import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

import pandas as pd

try:
    from kiteconnect import KiteConnect
    KITE_AVAILABLE = True
except Exception:
    KITE_AVAILABLE = False


class ZerodhaCredentials:
    def __init__(self, api_key: str, api_secret: str, access_token: Optional[str] = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.access_token = access_token


class ZerodhaService:
    def __init__(self, credentials: ZerodhaCredentials):
        if not KITE_AVAILABLE:
            raise ImportError('kiteconnect is not installed. Install with: pip install kiteconnect')
        self.logger = logging.getLogger(__name__ + '.ZerodhaService')
        self.credentials = credentials
        self.kite: Optional[KiteConnect] = KiteConnect(api_key=self.credentials.api_key)
        if self.credentials.access_token:
            self.kite.set_access_token(self.credentials.access_token)

        self._instruments_cache: Dict[str, List[dict]] = {}

    def login_url(self) -> str:
        return self.kite.login_url()

    def set_access_token_from_request(self, request_token: str) -> str:
        session = self.kite.generate_session(request_token, api_secret=self.credentials.api_secret)
        self.credentials.access_token = session['access_token']
        self.kite.set_access_token(self.credentials.access_token)
        return self.credentials.access_token

    def get_instruments(self, exchange: str = 'NSE') -> List[Dict]:
        cache_key = f'instruments_{exchange}'
        now = datetime.now()
        cached = self._instruments_cache.get(cache_key)
        if cached and (now - cached[0]) < timedelta(hours=1):
            return cached[1]
        instruments = self.kite.instruments(exchange)
        self._instruments_cache[cache_key] = (now, instruments)
        return instruments

    def get_instrument_token(self, symbol: str, exchange: str = 'NSE') -> Optional[int]:
        instruments = self.get_instruments(exchange)
        for inst in instruments:
            if inst.get('tradingsymbol') == symbol:
                return inst.get('instrument_token')
        self.logger.warning(f'Instrument token not found for {symbol}')
        return None

    def fetch_historical_data(self, symbol: str, start_date: datetime, end_date: datetime, interval: str = 'minute') -> pd.DataFrame:
        token = self.get_instrument_token(symbol)

        if not token:
            return pd.DataFrame()
        chunk = timedelta(days=50)
        cur = start_date
        all_rows: List[dict] = []
        while cur < end_date:
            nxt = min(cur + chunk, end_date)
            rows = self.kite.historical_data(instrument_token=token, from_date=cur, to_date=nxt, interval=interval)
            if rows:
                all_rows.extend(rows)
            time.sleep(0.25)
            cur = nxt + timedelta(days=1)
        if not all_rows:
            return pd.DataFrame()
        df = pd.DataFrame(all_rows).rename(columns={'date': 'timestamp'})
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
        return df

    def fetch_live_quotes(self, symbols: List[str]) -> Dict[str, Dict]:
        if not self.credentials.access_token:
            return {}
        instrument_keys: List[str] = []
        sym_map: Dict[str, str] = {}
        for s in symbols:
            instrument_keys.append(f"NSE:{s}")
            sym_map[f"NSE:{s}"] = s
        quotes = self.kite.quote(instrument_keys)
        live: Dict[str, Dict] = {}
        for key, q in quotes.items():
            s = sym_map.get(key)
            if not s:
                continue
            live[s] = {
                'open': q.get('ohlc', {}).get('open', 0.0),
                'high': q.get('ohlc', {}).get('high', 0.0),
                'low': q.get('ohlc', {}).get('low', 0.0),
                'close': q.get('last_price', 0.0),
                'volume': q.get('volume', 0)
            }
        return live



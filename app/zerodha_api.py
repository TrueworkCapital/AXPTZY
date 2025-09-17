import os
import json
from datetime import datetime, timedelta
from typing import Optional
from kiteconnect import KiteConnect, KiteTicker
from dotenv import load_dotenv

# Load environment variables from a .env file at project root, if present
load_dotenv()

class ZerodhaCredentials:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret

class ZerodhaConnection:
    def __init__(self, credentials: ZerodhaCredentials):
        self.credentials = credentials
        self.kite = KiteConnect(api_key=self.credentials.api_key)
        self.access_token = os.getenv('ZERODHA_ACCESS_TOKEN')
        if self.access_token:
            self.kite.set_access_token(self.access_token)

    
    def get_login_url(self, redirect_url: str = None) -> str:
        self.kite.set_session_expiry_hook(self.handle_session_expiry)
        login_url = self.kite.login_url()  # redirect_url not needed in code
        return login_url


    def handle_session_expiry(self):
        """
        Handle session expiry.
        You can implement logic to refresh or notify the user.
        """
        print("Session expired! Please re-authenticate.")

    def generate_session(self, request_token: str) -> dict:
        """
        Exchange request token for access token and store it securely.
        """
        try:
            data = self.kite.generate_session(request_token, self.credentials.api_secret)
            self.access_token = data["access_token"]
            # Store the access token securely (e.g., database or environment)
            self.save_access_token(self.access_token)
            self.kite.set_access_token(self.access_token)
            return data
        except Exception as e:
            raise Exception(f"Failed to generate session: {e}")

    def save_access_token(self, token: str):
        """
        Save access token securely.
        This example uses environment variables, but a database is recommended.
        """
        # WARNING: In production, store this in a secure database or vault
        os.environ['ZERODHA_ACCESS_TOKEN'] = token
        print("Access token saved securely.")

    def test_connection(self) -> bool:
        """
        Test if the connection is working by fetching the user profile.
        """
        try:
            profile = self.kite.profile()
            print("Connected as:", profile['user_name'])
            return True
        except Exception as e:
            print("Connection failed:", e)
            return False

    def get_historical_data(self, symbol: str, from_date: str, to_date: str, interval: str = "day"):
        """
        Fetch historical data for a stock symbol within a date range.
        """
        from datetime import datetime
        try:
            instrument_token = self.get_instrument_token(symbol)
            data = self.kite.historical_data(
                instrument_token=instrument_token,
                from_date=datetime.strptime(from_date, "%Y-%m-%d"),
                to_date=datetime.strptime(to_date, "%Y-%m-%d"),
                interval=interval
            )
            return data
        except Exception as e:
            raise Exception(f"Error fetching historical data: {e}")

    def get_instrument_token(self, symbol: str) -> int:
        """
        Retrieve instrument token from the exchange instruments list.
        """
        try:
            instruments = self.kite.instruments("NSE")
            for instrument in instruments:
                if instrument['tradingsymbol'] == symbol:
                    return instrument['instrument_token']
            raise Exception(f"Instrument token not found for {symbol}")
        except Exception as e:
            raise Exception(f"Error retrieving instrument token: {e}")

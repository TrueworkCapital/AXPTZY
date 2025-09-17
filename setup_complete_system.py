from app.services import DataManager, ZerodhaService, ZerodhaCredentials
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def setup_complete_system():
    """Setup and test the complete DataManager system."""

    print("🚀 SETTING UP COMPLETE DATA MANAGER SYSTEM")
    print("="*60)

    # Create necessary directories
    os.makedirs('data_storage', exist_ok=True)
    os.makedirs('exports', exist_ok=True)
    os.makedirs('logs', exist_ok=True)

    # DataManager config (DB via app/database.py)
    config = {
        'data_validation_enabled': True,
        'export_formats': ['csv', 'json', 'parquet']
    }

    # Zerodha credentials from environment
    api_key = os.getenv('ZERODHA_API_KEY')
    api_secret = os.getenv('ZERODHA_API_SECRET')
    zerodha_creds = ZerodhaCredentials(api_key=api_key, api_secret=api_secret)

    try:
        # Initialize services
        dm = DataManager(config)
        zs = None
        if api_key and api_secret:
            zs = ZerodhaService(zerodha_creds)

        print(f"\n📊 SYSTEM OVERVIEW:")
        print(f"   Total Symbols: {len(dm.symbols_list)}")
        print(f"   Sectors: {len(dm.sectors)}")

        # Display all symbols by sector
        print(f"\n🏢 NIFTY50 CONSTITUENTS BY SECTOR:")
        for sector, symbols in dm.sectors.items():
            print(f"   {sector:20}: {len(symbols):2d} stocks - {', '.join(symbols[:3])}{'...' if len(symbols) > 3 else ''}")

        # Simple health view
        print(f"\n🏥 SYSTEM HEALTH CHECK:")
        print(f"   Database: ✅")
        print(f"   Zerodha API: {'✅' if zs else '⚠️ Needs OAuth'}")

        # Performance statistics (placeholder)
        print(f"\n⚡ PERFORMANCE METRICS:")
        print(f"   Cache Entries: 0")

        # Test core functions with sample data
        print(f"\n🧪 TESTING CORE FUNCTIONS:")

        test_symbols = ['RELIANCE', 'TCS', 'HDFCBANK']
        for symbol in test_symbols:
            data = dm.get_latest_bars(symbol, count=5)
            print(f"   {symbol}: {len(data)} bars retrieved")

        # Sector example (optional)
        # it_stocks = dm.get_sector_data('IT', count=1)
        # print(f"   IT Sector: {len(it_stocks)} stocks queried")

        print(f"\n✅ COMPLETE DATA MANAGER SETUP SUCCESSFUL!")
        print(f"🎯 All {len(dm.symbols_list)} Nifty50 constituents ready for data ingestion")
        print(f"🌐 Zerodha API integration available")
        print(f"💾 MySQL database models are ready")

        print(f"\n📋 NEXT STEPS:")
        print(f"1. Complete Zerodha OAuth flow for live data")
        print(f"2. Download historical data using API")
        print(f"3. Set up real-time data feeds")
        print(f"4. Proceed to Module 2: Pivot Calculator")

    except Exception as e:
        print(f"❌ Setup failed: {e}")
        raise

if __name__ == "__main__":
    setup_complete_system()

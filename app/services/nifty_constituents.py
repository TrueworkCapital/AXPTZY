from typing import Dict, List


class NiftyConstituentsManager:
    NIFTY50_CONSTITUENTS = {
        'ADANIENT': {'name': 'Adani Enterprises Ltd.', 'sector': 'Diversified'},
        'ADANIPORTS': {'name': 'Adani Ports and Special Economic Zone Ltd.', 'sector': 'Infrastructure'},
        'APOLLOHOSP': {'name': 'Apollo Hospitals Enterprise Ltd.', 'sector': 'Healthcare'},
        'ASIANPAINT': {'name': 'Asian Paints Ltd.', 'sector': 'Consumer Goods'},
        'AXISBANK': {'name': 'Axis Bank Ltd.', 'sector': 'Banking'},
        'BAJAJ-AUTO': {'name': 'Bajaj Auto Ltd.', 'sector': 'Automobile'},
        'BAJFINANCE': {'name': 'Bajaj Finance Ltd.', 'sector': 'Financial Services'},
        'BAJAJFINSV': {'name': 'Bajaj Finserv Ltd.', 'sector': 'Financial Services'},
        'BPCL': {'name': 'Bharat Petroleum Corporation Ltd.', 'sector': 'Oil & Gas'},
        'BHARTIARTL': {'name': 'Bharti Airtel Ltd.', 'sector': 'Telecom'},
        'BRITANNIA': {'name': 'Britannia Industries Ltd.', 'sector': 'FMCG'},
        'CIPLA': {'name': 'Cipla Ltd.', 'sector': 'Pharma'},
        'COALINDIA': {'name': 'Coal India Ltd.', 'sector': 'Mining'},
        'DIVISLAB': {'name': "Divi's Laboratories Ltd.", 'sector': 'Pharma'},
        'DRREDDY': {'name': "Dr. Reddy's Laboratories Ltd.", 'sector': 'Pharma'},
        'EICHERMOT': {'name': 'Eicher Motors Ltd.', 'sector': 'Automobile'},
        'GRASIM': {'name': 'Grasim Industries Ltd.', 'sector': 'Cement'},
        'HCLTECH': {'name': 'HCL Technologies Ltd.', 'sector': 'IT'},
        'HDFCBANK': {'name': 'HDFC Bank Ltd.', 'sector': 'Banking'},
        'HDFCLIFE': {'name': 'HDFC Life Insurance Company Ltd.', 'sector': 'Insurance'},
        'HEROMOTOCO': {'name': 'Hero MotoCorp Ltd.', 'sector': 'Automobile'},
        'HINDALCO': {'name': 'Hindalco Industries Ltd.', 'sector': 'Metals'},
        'HINDUNILVR': {'name': 'Hindustan Unilever Ltd.', 'sector': 'FMCG'},
        'ICICIBANK': {'name': 'ICICI Bank Ltd.', 'sector': 'Banking'},
        'ITC': {'name': 'ITC Ltd.', 'sector': 'FMCG'},
        'INDUSINDBK': {'name': 'IndusInd Bank Ltd.', 'sector': 'Banking'},
        'INFY': {'name': 'Infosys Ltd.', 'sector': 'IT'},
        'JSWSTEEL': {'name': 'JSW Steel Ltd.', 'sector': 'Metals'},
        'KOTAKBANK': {'name': 'Kotak Mahindra Bank Ltd.', 'sector': 'Banking'},
        'LT': {'name': 'Larsen & Toubro Ltd.', 'sector': 'Infrastructure'},
        'M&M': {'name': 'Mahindra & Mahindra Ltd.', 'sector': 'Automobile'},
        'MARUTI': {'name': 'Maruti Suzuki India Ltd.', 'sector': 'Automobile'},
        'NTPC': {'name': 'NTPC Ltd.', 'sector': 'Power'},
        'NESTLEIND': {'name': 'Nestle India Ltd.', 'sector': 'FMCG'},
        'ONGC': {'name': 'Oil and Natural Gas Corporation Ltd.', 'sector': 'Oil & Gas'},
        'POWERGRID': {'name': 'Power Grid Corporation of India Ltd.', 'sector': 'Power'},
        'RELIANCE': {'name': 'Reliance Industries Ltd.', 'sector': 'Oil & Gas'},
        'SBILIFE': {'name': 'SBI Life Insurance Company Ltd.', 'sector': 'Insurance'},
        'SHRIRAMFIN': {'name': 'Shriram Finance Ltd.', 'sector': 'Financial Services'},
        'SBIN': {'name': 'State Bank of India', 'sector': 'Banking'},
        'SUNPHARMA': {'name': 'Sun Pharmaceutical Industries Ltd.', 'sector': 'Pharma'},
        'TCS': {'name': 'Tata Consultancy Services Ltd.', 'sector': 'IT'},
        'TATACONSUM': {'name': 'Tata Consumer Products Ltd.', 'sector': 'FMCG'},
        'TATAMOTORS': {'name': 'Tata Motors Ltd.', 'sector': 'Automobile'},
        'TATASTEEL': {'name': 'Tata Steel Ltd.', 'sector': 'Metals'},
        'TECHM': {'name': 'Tech Mahindra Ltd.', 'sector': 'IT'},
        'TITAN': {'name': 'Titan Company Ltd.', 'sector': 'Consumer Goods'},
        'ULTRACEMCO': {'name': 'UltraTech Cement Ltd.', 'sector': 'Cement'},
        'UPL': {'name': 'UPL Ltd.', 'sector': 'Chemicals'},
        'WIPRO': {'name': 'Wipro Ltd.', 'sector': 'IT'}
    }

    @classmethod
    def get_constituents(cls) -> Dict[str, Dict[str, str]]:
        return cls.NIFTY50_CONSTITUENTS.copy()

    @classmethod
    def get_symbols_list(cls) -> List[str]:
        return list(cls.NIFTY50_CONSTITUENTS.keys())

    @classmethod
    def get_sectors(cls) -> Dict[str, List[str]]:
        sectors: Dict[str, List[str]] = {}
        for symbol, info in cls.NIFTY50_CONSTITUENTS.items():
            sector = info['sector']
            sectors.setdefault(sector, []).append(symbol)
        return sectors



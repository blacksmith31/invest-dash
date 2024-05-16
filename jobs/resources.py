from abc import ABC, abstractmethod
import json
import requests
from typing import List


HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}


class ResourceBase(ABC):

    @abstractmethod
    def get(self, *args) -> List[dict]:
        pass


class YFiChart(ResourceBase):
    """
    This is a narrow wrapper around the Yahoo Finance API `charts` endpoint 
    """

    @property
    def url(self):
        return f"https://query2.finance.yahoo.com/v8/finance/chart/"

    def __init__(self, headers:dict=HEADERS, timeout:int=10000):
        """
        Args:
            period `range`(str):
                Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        """
        self.headers = headers
        self.timeout = timeout

    def get(self, ticker: str, period:int) -> List[dict]:
        daystr = str(period) + 'd'
        params = {
            "interval": '1d',
            "range": daystr
        }
        url = self.url + ticker
        # Getting data from json
        response = requests.get(
            url=url,
            headers=self.headers,
            params=params,
            timeout=self.timeout
        )

        if response.status_code != 200:
            print(f"request failed for ticker {ticker} with status code {response.status_code}")
            return []
        rj = response.json()

        try:
            timestamps = rj["chart"]["result"][0]["timestamp"]
        except KeyError:
            print(f'KeyError for timestamp at path `rj["chart"]["result"][0]["timestamp"]`: {json.dumps(rj, indent=2)}')
            return []
        try:
            closes = rj["chart"]["result"][0]["indicators"]["adjclose"][0]["adjclose"]
        except KeyError:
            print(f'KeyError for closes at path `rj["chart"]["result"][0]["indicators"]["adjclose"][0]["adjclose"]`: {json.dumps(rj, indent=2)}')
            return []

        days_tup = zip([ticker] * len(timestamps), timestamps, closes)
        ticker_days = [
            {"ticker": ticker, "timestamp": timestamp, "close": close} 
            for ticker, timestamp, close in days_tup
        ]
        return ticker_days


class SymbolsHeader(ResourceBase):
    """
    fetches json data collected by a github action
    """

    @property
    def url(self):
        return "https://raw.githubusercontent.com/jamesonhm/US-Stock-Symbols/main/all/all_full_tickers.json"
    
    def __init__(self, headers:dict=HEADERS, timeout:int=10000):
        self.headers = headers
        self.timeout = timeout
        
    def get(self) -> List[dict]:
        response = requests.get(
            url=self.url,
            headers=HEADERS,
            timeout=self.timeout
        )

        if response.status_code != 200:
            print(f"request failed")
            return []
        print(f"length: {len(response.json())} for {self.url.split('/')[-1]}")
        return response.json()



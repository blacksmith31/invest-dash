import requests

HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}


def get_symbols(timeout:int=10000):
    url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_full_tickers.json"
    #params = {
    #    "tableonly": True,
    #    "limit": 25,
    #    "offset": 0, 
    #    "exchange": "nasdaq",
    #    "download": True
    #}

    # Getting data from json
    response = requests.get(
        url=url,
        headers=HEADERS,
        timeout=timeout
    )

    if response.status_code != 200:
        print(f"request failed")
        return []
    rj = response.json()
    return rj

def main():
    for sym in get_symbols():
        print(sym)


if __name__ == "__main__":
    main()


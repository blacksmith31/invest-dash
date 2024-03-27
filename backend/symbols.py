import requests
from db import insert_update_sym_hdr

HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}


def get_symbols(timeout:int=10000):
    url = "https://raw.githubusercontent.com/jamesonhm/US-Stock-Symbols/main/amex/amex_full_tickers.json"    

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
    print(f"before .json: {type(response)}")
    rj = response.json()
    print(f"after .json: {type(rj)}")
    return rj

def prep_syms(syms: list[dict]):
    updated = []
    keys = ["symbol", "name", "marketCap", "country", "industry", "sector"]
    for sym in syms: 
        new = {key: sym[key] for key in keys}
        new["marketCap"] = int(float(new["marketCap"])) 
        updated.append(new)
    return updated

def main():
    syms = get_symbols()
    print(syms[0])
    # print(syms[0]["symbol"], int(float(syms[0]["marketCap"])))
    """
                    :symbol,
                    :name,
                    :marketCap,
                    :country,
                    :industry,
                    :sector
    """
    toins = syms[:5]
    toins = prep_syms(toins)
    for sym in toins:
        print(sym)

if __name__ == "__main__":
    main()


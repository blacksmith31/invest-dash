import requests
from typing import List
from backend.db import (
    insert_update_sym_hdr, 
    update_symbol_own, 
    update_prev_pos, 
    update_symbols_autotrack
)
from schemas.schemas import Symbol

HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}


def get_symbols(timeout:int=10000) -> List[dict]:
    urls = [
        "https://raw.githubusercontent.com/jamesonhm/US-Stock-Symbols/main/amex/amex_full_tickers.json",
        "https://raw.githubusercontent.com/jamesonhm/US-Stock-Symbols/main/nasdaq/nasdaq_full_tickers.json",
        "https://raw.githubusercontent.com/jamesonhm/US-Stock-Symbols/main/nyse/nyse_full_tickers.json"
    ]
    response_data = []
    for url in urls:
        # Getting data from json
        response = requests.get(
            url=url,
            headers=HEADERS,
            timeout=timeout
        )

        if response.status_code != 200:
            print(f"request for {url} failed")
            return []
        print(f"length: {len(response.json())} for {url.split('/')[-1]}")
        response_data += response.json()
    return response_data

def prep_syms(syms: List[dict]) -> List[Symbol]:
    updated = []
    # keys = ["symbol", "name", "marketCap", "country", "industry", "sector"]
    for sym in syms: 
        # new = {key: sym[key] for key in keys}
        try:
            # new["marketCap"] = int(float(new["marketCap"])) 
            new = Symbol.model_validate(sym)
            updated.append(new)
        except:
            print(f"Symbol Update| input symbol: {sym['symbol']}, mktcap: {sym['marketCap']}")
            raise
    return updated

def symbol_update():
    # update previous position
    update_prev_pos()
    # update new mktcap
    syms = get_symbols()
    toins = prep_syms(syms)
    insert_update_sym_hdr(toins)
    # update tracked status
    update_symbols_autotrack(1000)

def test():
    # update previous position
    # update_prev_pos()
    # update new mktcap
    syms = get_symbols()
    print(f"type: {type(syms)}")
    print(f"type of item: {type(syms[0])}")
    print(f"item: {syms[0]}")
    # toins = prep_syms(syms[:5])
    # print(f"sym models: ")
    # for model in toins:
    #     print(model)
    # toins[0]["marketCap"] = 999999999
    # insert_update_sym_hdr(toins)
    # #toggle_own("AE")
    # # update tracked status
    # update_symbols_autotrack(3)

if __name__ == "__main__":
    test()
    # symbol_update()

from typing import List
import sqlite3

from schemas.schemas import Symbol, TickerDayClose

con = sqlite3.connect("./data/scraper.db", check_same_thread=False)

def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}

con.row_factory = dict_factory

with con:
    con.execute("""
                CREATE TABLE IF NOT EXISTS ticker_history (
                    timestamp INTEGER,
                    ticker TEXT,
                    close NUMERIC, 
                    sroc NUMERIC,
                    UNIQUE(timestamp, ticker)
                )
            """
    )

    con.execute("""
                CREATE TABLE IF NOT EXISTS symbol_hdr (
                    symbol TEXT,
                    name TEXT,
                    mktcap NUMERIC,
                    country TEXT,
                    industry TEXT,
                    sector TEXT,
                    track INTEGER,
                    own INTEGER,
                    prev_position INTEGER,
                    UNIQUE(SYMBOL)
                )
            """
    ) 

def drop_table(name: str):
    try:
        with con:
            con.execute(f"DROP TABLE {name}")
    except sqlite3.OperationalError:
        print("failed to drop table")


def select_sorted_closes():
    try:
        with con:
            result = con.execute("""
            SELECT timestamp,
                   ticker,
                   close
              FROM ticker_history
          ORDER BY ticker
                  ,timestamp
            """).fetchall()
            return result
    except sqlite3.DatabaseError:
        raise


def select_ticker_history(ticker: str):
    try:
        with con:
            result = con.execute("""
            SELECT timestamp,
                   close,
                   sroc
              FROM ticker_history
             WHERE ticker = ?
               AND sroc IS NOT NULL
          ORDER BY timestamp
            """, [ticker]).fetchall()
            return result
    except sqlite3.DatabaseError:
        raise


def select_ticker_closes(ticker: str):
    try:
        with con:
            result = con.execute("""
            SELECT ticker,
                   timestamp,
                   close
              FROM ticker_history
             WHERE ticker = ?
          ORDER BY timestamp
            """, [ticker]).fetchall()
            return result
    except sqlite3.DatabaseError:
        raise


def select_ticker_scores(ticker: str) -> list[dict]:
    try:
        with con:
            result = con.execute("""
            SELECT ticker,
                   timestamp,
                   sroc
              FROM ticker_history
             WHERE ticker = ?
          ORDER BY timestamp
            """, [ticker]).fetchall()
            return result
    except sqlite3.DatabaseError:
        raise


def select_prev_days_scores(limit: int, max_ts:int, min_ts:int) -> list[dict]:
    try:
        with con:
            result = con.execute("""
            SELECT max(timestamp) timestamp
                   ,ticker
                   ,sroc
              FROM ticker_history
             WHERE timestamp < ?
               AND timestamp > ?
               AND sroc is not null
          GROUP BY ticker
          ORDER BY sroc DESC
             LIMIT ?
             """, [max_ts, min_ts, limit]).fetchall()
            return result
    except sqlite3.DatabaseError:
        raise
        

def select_prev_days_scores_owned(limit: int, max_ts:int, min_ts:int) -> list[dict]:
    # try:
    with con:
        result = con.execute("""
            SELECT max(th.timestamp) timestamp
                   ,th.ticker
                   ,th.sroc
                   ,s.own
              FROM ticker_history th
        INNER JOIN symbol_hdr s
                ON s.symbol = th.ticker
             WHERE th.timestamp < ?
               AND th.timestamp > ?
               AND th.sroc is not null
          GROUP BY th.ticker
          ORDER BY th.sroc DESC
             LIMIT ?
         """, [max_ts, min_ts, limit]).fetchall()
        return result
    # except sqlite3.DatabaseError:
    #     raise

def select_ticker_own(ticker: str):
    with con:
        result = con.execute("""
            SELECT max(th.timestamp) timestamp
                   ,th.ticker
                   ,th.sroc
                   ,s.own
              FROM ticker_history th 
        INNER JOIN symbol_hdr s 
                ON s.symbol = th.ticker
            WHERE  th.ticker = ?
        """, [ticker]).fetchone()
        return result

def select_max_ticker_ts(ticker: str) -> dict[str, int]:
    with con:
        result = con.execute("""
        SELECT max(timestamp) latest,
                count(timestamp) daycount
            FROM ticker_history
            WHERE ticker = ?
        """, [ticker]).fetchall()
        return result[0]

def insert_closes_many(history: List[TickerDayClose]) -> int:
    dumped = [day.model_dump() for day in history]
    with con:
        rows = con.executemany(f"""
            INSERT INTO ticker_history (
                        timestamp,
                        ticker,
                        close)
                 VALUES (:timestamp
                        ,:ticker
                        ,:close)
            ON CONFLICT (timestamp, ticker) DO NOTHING
        """, dumped).rowcount
        return rows

def update_sroc_many(data: list[dict]) -> int:
    with con:
        rows = con.executemany(""" 
            UPDATE ticker_history 
               SET sroc = :sroc
             WHERE timestamp = :timestamp
               AND ticker = :ticker
        """, data).rowcount
        return rows

def view_daily_scores(days:int=7):
    sql_gen = f"""
       with days as (
           select distinct timestamp as day 
           from ticker_history 
           where sroc is not null
           and timestamp > strftime('%s','now', 'start of day', '-{days} days')
           ),
       lines as (
           select 'select ticker ' as part
           union all
           select ', ifnull(sum(sroc) filter (where timestamp = ' || day || '), -999) as "' || date(day, 'unixepoch') || '" '
           from days
           union all
           select 'from ticker_history group by ticker;'
       )
       select group_concat(part, '')
       from lines;
    """
    with con:
        sql = list(con.execute(sql_gen).fetchall()[0].values())[0]
    # with con:
        data = con.execute(sql).fetchall()
    return data

def select_tickers_scores(min_ts: int):
    with con:
        result = con.execute("""
            SELECT timestamp,
                   ticker,
                   sroc
              FROM ticker_history
             WHERE sroc is not null
               AND timestamp > ?
        """, [min_ts]).fetchall()
        return result

def prune_ticker_history(min_ts: int) -> None:
    with con:
        con.execute("""
                    DELETE FROM ticker_history
                    WHERE timestamp < ?
                    """, [min_ts])
        return None

def insert_update_sym_hdr(data: List[Symbol]) -> None:
    dumped = [sym.model_dump() for sym in data]
    with con:
        con.executemany("""
            INSERT INTO symbol_hdr (
                    symbol,
                    name,
                    mktcap,
                    country,
                    industry,
                    sector
            ) VALUES (
                    :symbol,
                    :name,
                    :mktcap,
                    :country,
                    :industry,
                    :sector
            ) ON CONFLICT(symbol) DO UPDATE
            SET mktcap = excluded.mktcap
        """, dumped)
        return None

def select_top_symbols_mcap(n:int=1000):
    try:
        with con:
            result = con.execute("""
                 SELECT symbol
                 FROM symbol_hdr
                 ORDER BY mktcap DESC
                 LIMIT ?
                 """, [n]).fetchall()
            return result
    except:
        raise

def view_symbol_hdr(limit:int=9999):
    with con:
        result = con.execute("""
            SELECT symbol,
                   name,
                   mktcap,
                   country,
                   industry,
                   sector,
                   row_number() over(order by mktcap desc) mktcap_rank
            FROM symbol_hdr
            ORDER BY mktcap desc
            LIMIT ?
            """, [limit]).fetchall()
        return result

def update_prev_pos():
    try:
        with con:
            con.execute("""
                UPDATE symbol_hdr
                   SET prev_position = curr.top
                  FROM (
                       SELECT symbol, 
                           row_number() OVER(ORDER BY mktcap DESC) as top 
                       FROM symbol_hdr) as curr
                 WHERE symbol_hdr.symbol = curr.symbol;
                """)
    except:
        raise

def update_symbols_autotrack(n: int):
    try:
        with con:
            con.execute("""
                        update symbol_hdr
                        set track = 0
                        where own = 0 or own is null
                        """)
            con.execute("""
                        update symbol_hdr
                        set track = 1
                            from (select symbol, row_number() over(order by mktcap desc) as top from symbol_hdr) as curr
                        where symbol_hdr.symbol = curr.symbol and (
                            curr.top <= ? or symbol_hdr.own = 1)
                       """, [n])
    except:
        raise

def update_symbol_own(symbol: str):
    with con:
        con.execute("""
                    update symbol_hdr
                    set own = CASE own
                        WHEN 1 THEN 0
                        ELSE 1
                        END
                    WHERE symbol = ?;
                    """, [symbol])

if __name__ == "__main__":
    # old = select_latest_scores(20)
    # print(select_ticker_scores("MSFT"))
    # now = datetime.now()
    # current_ts = dt_day_shift_ts(now, 0)
    # curr_min_ts = dt_day_shift_ts(now, -8)
    # new = select_prev_days_scores(20, curr_min_ts, current_ts)
    print(select_max_ticker_ts("IRT"))
    # print(f"old: ")
    # # for row in old:
    # #     print(row)
    # print("=====================================")
    # print("new: ")
    # for row in new:
    #     print(row)


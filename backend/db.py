import sqlite3

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


# def get_recent_eods():
#     try:
#         with con:
#             result = con.execute("""
#                 SELECT timestamp, 
#                        ticker, 
#                        close,
#                        max(sroc)
#                   FROM ticker_history
#                  WHERE sroc > 60
#                  GROUP BY ticker
#             """).fetchall()
#             return result
#     except sqlite3.DatabaseError:
#         raise


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


def select_ticker_closes(ticker: str) -> list[dict]:
    try:
        with con:
            result = con.execute("""
            SELECT timestamp,
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
            SELECT timestamp x,
                   IFNULL(sroc, 'null') y
              FROM ticker_history
             WHERE ticker = ?
          ORDER BY timestamp
            """, [ticker]).fetchall()
            return result
    except sqlite3.DatabaseError:
        raise


def select_latest_scores(limit: int) -> list[dict]:
    try:
        with con:
            result = con.execute("""
            SELECT max(timestamp) ts
                   ,ticker
                   ,sroc
              FROM ticker_history
             WHERE timestamp > strftime('%s', 'now', 'start of day', '-7 days')
          GROUP BY ticker
          ORDER BY sroc DESC
             LIMIT ?
             """, [limit]).fetchall()
            return result
    except sqlite3.DatabaseError:
        raise


def select_prev_days_scores(limit: int, min_ts:int, max_ts:int) -> list[dict]:
    try:
        with con:
            result = con.execute("""
            SELECT max(timestamp) ts
                   ,ticker
                   ,sroc
              FROM ticker_history
             WHERE timestamp < ?
               AND timestamp > ?
          GROUP BY ticker
          ORDER BY sroc DESC
             LIMIT ?
             """, [max_ts, min_ts, limit]).fetchall()
            return result
    except sqlite3.DatabaseError:
        raise
        

def select_max_ticker_ts(ticker: str) -> list[dict]:
    with con:
        result = con.execute("""
        SELECT max(timestamp) latest,
                count(timestamp) daycount
            FROM ticker_history
            WHERE ticker = ?
        """, [ticker]).fetchall()
        return result


def insert_ticker_close(ticker: str, timestamp: int, close: float) -> None:
    with con:
        con.execute("""
            INSERT INTO ticker_history (
                        timestamp,
                        ticker,
                        close)
                 VALUES (?, ?, ?)
            ON CONFLICT (timestamp, ticker) DO NOTHING
        """, [timestamp, ticker, close])
        return None


def insert_closes_many(data: list[tuple]) -> None:
    if data:
        with con:
            con.executemany("""
                INSERT INTO ticker_history (
                            timestamp,
                            ticker,
                            close)
                     VALUES (?, ?, ?)
                ON CONFLICT (timestamp, ticker) DO NOTHING
            """, data)
        return None


def update_ticker_sroc(ticker: str, ts: int, sroc: float) -> None:
    with con:
        con.execute("""
            UPDATE ticker_history
               SET sroc = ?
             WHERE timestamp = ?
               AND ticker = ?
        """, [sroc, ts, ticker])
        return None


def update_sroc_many(data: list[dict]) -> None:
    with con:
        con.executemany(""" 
            UPDATE ticker_history 
               SET sroc = :sroc
             WHERE timestamp = :timestamp
               AND ticker = :ticker
        """, data)
        return None

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

def prune_ticker_history(min_ts: int) -> None:
    with con:
        con.execute("""
                    DELETE FROM ticker_history
                    WHERE timestamp < ?
                    """, [min_ts])
        return None

def insert_update_sym_hdr(data: list[dict]) -> None:
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
                    :marketCap,
                    :country,
                    :industry,
                    :sector
            ) ON CONFLICT(symbol) DO UPDATE
            SET mktcap = excluded.mktcap
        """, data)
        return None

def select_top_symbols_mcap(n: int = 1000):
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
    # drop_eod_table()
    # create_eod_table()

    sql = view_daily_scores()
    print(f"generated sql: {sql}")
    # tablenames = con.execute("SELECT name FROM sqlite_master").fetchall()
    # print(tablenames)
    # if len(tablenames):
    #     assert f"ticker_eod" in [table["name"] for table in tablenames]
    # else:
    #     print("No tables exist")
    # data = select_latest_scores(10)
    # print(data)
    # eods = get_ticker_eods()
    # for d in eods:
    #     print(d)

#   aapl = get_ticker("AAPL")
#   print(aapl)

    # # print(today_eod[0][0])
    # for row in today_eod[0]:
    #     print(row)

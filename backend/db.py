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


def get_recent_eods():
    try:
        with con:
            result = con.execute("""
                SELECT date, 
                       ticker, 
                       close 
                  FROM ticker_history
                 WHERE date > DATE() - 1
            """).fetchall()
            return result
    except sqlite3.DatabaseError:
        raise


def get_ticker_eods():
    try:
        with con:
            result = con.execute("""
            SELECT date,
                   ticker,
                   close
              FROM ticker_history
          ORDER BY ticker
                  ,date
            """).fetchall()
            return result
    except sqlite3.DatabaseError:
        raise


def get_history(ticker: str):
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


def get_ticker(ticker: str) -> list[dict]:
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


def get_ticker_sroc(ticker: str) -> list[dict]:
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


def get_latest_scores(limit: int) -> list[dict]:
    try:
        with con:
            result = con.execute("""
            SELECT max(timestamp) ts
                   ,ticker
                   ,sroc
              FROM ticker_history
          GROUP BY ticker
          ORDER BY sroc DESC
             LIMIT ?
             """, [limit]).fetchall()
            return result
    except sqlite3.DatabaseError:
        raise


def get_ticker_latest(ticker: str) -> list[dict]:
    with con:
        result = con.execute("""
        SELECT max(timestamp) latest,
                count(timestamp) daycount
            FROM ticker_history
            WHERE ticker = ?
        """, [ticker]).fetchall()
        return result


def update_history(ticker: str, timestamp: int, close: float) -> None:
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


def update_close_many(data: list[tuple]) -> None:
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


def prune_data(min_ts: int) -> None:
    with con:
        con.execute("""
                    DELETE FROM ticker_history
                    WHERE timestamp < ?
                    """, [min_ts])
        return None

def insert_update_sym_hdr(data: list[dict]) -> None:
    with con:
        con.executemany("""
            INSERT OR REPLACE INTO symbol_hdr (
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
            )
        """, data)
        return None

def top_n_symbols(n: int = 1000):
    try:
        with con:
            result = con.execute("""
                                 SELECT symbol
                                 FROM symbol_hdr
                                 ORDER BY mktcap
                                 LIMIT ?
                                 """, [n]).fetchall()
            return result
    except:
        raise

if __name__ == "__main__":
    # drop_eod_table()
    # create_eod_table()

    # tablenames = con.execute("SELECT name FROM sqlite_master").fetchall()
    # print(tablenames)
    # if len(tablenames):
    #     assert f"ticker_eod" in [table["name"] for table in tablenames]
    # else:
    #     print("No tables exist")
    data = get_latest_scores(10)
    print(data)
    # eods = get_ticker_eods()
    # for d in eods:
    #     print(d)

#   aapl = get_ticker("AAPL")
#   print(aapl)

    # # print(today_eod[0][0])
    # for row in today_eod[0]:
    #     print(row)

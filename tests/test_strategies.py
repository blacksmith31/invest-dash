import pytest
from pytest import approx

from backend.db import select_ticker_closes
from schemas.strategy import SROCStrategy

result = select_ticker_closes('META')
limited_set = [day for day in result if day["timestamp"] <= 1715347800]

strats = [
    SROCStrategy(scored_tickers=10, top_tickers=5, ema_window=13, roc_window=90)
]
expected_scores = [
    34.3
]

@pytest.mark.parametrize("strat, expected_score", list(zip(strats, expected_scores)))
def test_score(strat, expected_score):
    scores = strat.score(limited_set)
    # print(f"scores: {scores[-20:]}")
    assert expected_score == approx(scores[-1]["sroc"], rel=0.5)
     

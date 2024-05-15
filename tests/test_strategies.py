import pytest
from pytest import approx

from backend.db import select_ticker_closes
from schemas.strategy import SROCStrategy

result = select_ticker_closes('META')
limited_set = [day for day in result if day["timestamp"] <= 1715347800]

sroc_strats = [
    SROCStrategy(scored_tickers=10, top_tickers=5, ema_window=13, roc_window=90)
]
expected_scores = [
    34.18
]
expected_qdays = [
    126
]

@pytest.mark.parametrize("strat, expected_score", list(zip(sroc_strats, expected_scores)))
def test_sroc_score(strat, expected_score):
    scores = strat.score(limited_set)
    # print(f"scores: {scores[-20:]}")
    assert expected_score == approx(scores[-1]["sroc"], abs=0.5)
     
@pytest.mark.parametrize("strat, expected_qday", list(zip(sroc_strats, expected_qdays)))
def test_post_init(strat, expected_qday):
    assert strat.query_days == expected_qday

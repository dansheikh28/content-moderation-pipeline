import pytest
from moderation.pipeline import score_toxicity


def test_score_toxicity_returns_valid_probabilities():
    texts = ["I love this!", "You are stupid."]
    scores = score_toxicity(texts)

    # Length matches input
    assert len(scores) == 2

    # All scores should be between 0 and 1
    assert all(0 <= s <= 1 for s in scores)


@pytest.mark.parametrize("text", ["I love this product!", "This is awful!!!!," ""])
def test_score_toxicity_handles_various_inputs(text):
    score = score_toxicity([text])[0]
    assert 0.0 <= score <= 1.0

import pytest
from moderation.pipeline import MULTI_LABELS, score_multilabel, score_toxicity


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


def test_multilabel_basic_shape_and_types():
    texts = ["You are nice", "You are awful", "Neutral statement"]
    df = score_multilabel(texts)

    # Columns present
    for col in MULTI_LABELS:
        assert col in df.columns
    assert "flagged" in df.columns

    # Ranges
    for col in MULTI_LABELS:
        assert df[col].between(0.0, 1.0, inclusive="both").all()

    # Boolean type
    assert str(df["flagged"].dtype) == "bool"

    # Row count matches
    assert len(df) == len(texts)


@pytest.mark.parametrize("label", MULTI_LABELS)
def test_each_label_probability_range(label):
    df = score_multilabel(["sample text"])
    assert label in df.columns
    assert df[label].between(0.0, 1.0, inclusive="both").all()

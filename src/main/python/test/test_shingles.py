from text_utils import to_shingles


def check_unique(xs: [str]):
    print(", ".join(xs))
    assert len(xs) == len(set(xs))


def check_invariants(xs: [str]):
    check_unique(xs)
    for x in xs:
        assert isinstance(x, str)


def test_1gram():
    text = "1 2 3"
    ngrams = {1}
    results = to_shingles(text, ngrams)
    assert set(results) == {"1", "2", "3"}
    check_invariants(results)


def test_2gram():
    text = "1 2 3"
    ngrams = {2}
    results = to_shingles(text, ngrams)
    assert set(results) == {"1 2", "2 3"}
    check_invariants(results)


def test_1gram_2gram():
    text = "1 2 3"
    ngrams = {1, 2}
    results = to_shingles(text, ngrams)
    assert set(results) == {"1", "2", "3", "1 2", "2 3"}
    check_invariants(results)


def test_2gram_3gram():
    text = "1 2 3"
    ngrams = {2, 3}
    results = to_shingles(text, ngrams)
    assert set(results) == {"1 2", "2 3", "1 2 3"}
    check_invariants(results)


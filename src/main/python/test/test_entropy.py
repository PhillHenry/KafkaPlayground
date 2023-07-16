from text_utils import entropy_of, to_shingles


def test_no_matching_words_means_0_entropy():
    assert entropy_of(["a document"], {}, {1}) == [0]


def test_entropy_of_a_single_word():
    word = "contiguous"
    n_grams = {2}
    freqs = {w: f for f, w in enumerate(to_shingles(word, n_grams, None))}
    entropies = entropy_of([word] * (len(word) + 1), freqs, n_grams, None)
    assert len(entropies) > 0
    for entropy in entropies:
        assert entropy > 0

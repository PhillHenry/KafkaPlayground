from text_utils import entropy_of


def test_no_matching_words_means_0_entropy():
    assert entropy_of(["a document"], {}, {1}) == [0]

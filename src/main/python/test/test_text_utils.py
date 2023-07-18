from text_utils import frequencies, camel_case_split, remove_timings


def test_frequency_of_chars():
    docs = ['registered',
            'kafka:type=kafka.log4jcontroller',
            'mbean',
            '(kafka.utils.log4jcontrollerregistration$)',
            'setting',
            'd',
            'jdk.tls.rejectclientinitiatedrenegotiation=true',
            'to',
            'disable',
            'client']
    n_grams = {2}
    histo = frequencies(docs, n_grams, None)
    assert len(histo) > 0
    for k, v in histo.items():
        assert len(k) in n_grams
    assert histo["re"] > 0


def test_remove_timings():
    assert len(remove_timings(["0ms", "15ms", "137009ms", "not a duration"])) == 1


def test_no_camel_case():
    text = "this is text"
    assert set(camel_case_split(text)) == {text}


def test_camel_case():
    assert set(camel_case_split("transactionId")) == {"transaction", "Id"}


def _test_camel_case_split_does_not_split_uuids():
    assert len(camel_case_split("A2b3C4FFF2fF")) == 1


def _test_camel_case_split_no_number_split():
    assert len(camel_case_split("transactionId1688479976915")) == 1

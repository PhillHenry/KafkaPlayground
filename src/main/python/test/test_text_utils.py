from text_utils import frequencies


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

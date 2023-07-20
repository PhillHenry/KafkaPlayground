from text_utils import frequencies, camel_case_split, remove_timings, clean_line


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
    assert len(remove_timings(["8 milliseconds", "not a duration"])) == 1


def test_no_camel_case():
    text = "this is text"
    assert set(camel_case_split(text)) == {text}


def test_camel_case():
    assert set(camel_case_split("transactionId")) == {"transaction", "Id"}


def test_clean():
    number = "1688480059114"
    text = f"kafka1: onNext: RAW: [2023-07-04 14:14:19,116] INFO [RaftManager id=1] High watermark set to LogOffsetMetadata(offset=3, metadata=Optional[(segmentBaseOffset=0,relativePositionInSegment=209)]) for the first time for epoch 1 based on indexOfHw 1 and voters [ReplicaState(nodeId=1, endOffset=Optional[LogOffsetMetadata(offset=5, metadata=Optional[(segmentBaseOffset=0,relativePositionInSegment=519)])], lastFetchTimestamp=-1, lastCaughtUpTimestamp=-1, hasAcknowledgedLeader=true), ReplicaState(nodeId=2, endOffset=Optional[LogOffsetMetadata(offset=3, metadata=Optional[(segmentBaseOffset=0,relativePositionInSegment=209)])], lastFetchTimestamp={number}, lastCaughtUpTimestamp=1688480059043, hasAcknowledgedLeader=true), ReplicaState(nodeId=3, endOffset=Optional[LogOffsetMetadata(offset=0, metadata=Optional[(segmentBaseOffset=0,relativePositionInSegment=0)])], lastFetchTimestamp=1688480059066, lastCaughtUpTimestamp=-1, hasAcknowledgedLeader=true)] (org.apache.kafka.raft.LeaderState)"
    cleaned = clean_line(text)
    assert number not in cleaned


def test_camel_case_happy_path():
    assert camel_case_split("GroupMetadataManager") == ["Group", "Metadata", "Manager"]


def test_no_split_entropic():
    assert "gOinsCZ0RZ-bGFcdVUlzAg" in camel_case_split("gOinsCZ0RZ-bGFcdVUlzAg")


def test_camel_case_split_does_not_split_uuids():
    assert len(camel_case_split("A2b3C4FFF2fF")) == 1


def test_camel_case_split_no_number_split():
    assert len(camel_case_split("transactionId1688479976915")) == 1

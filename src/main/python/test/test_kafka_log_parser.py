from kafka_log_parser import LogLine


def test_parse_info():
    line = "kafka3: onNext: RAW: [2023-07-04 14:17:32,032] INFO [GroupCoordinator 3]: Resigned as the group coordinator for partition 8 in epoch OptionalInt[1] (kafka.coordinator.group.GroupCoordinator)"
    log = LogLine(line)
    assert log.machine == "kafka3:"
    assert log.timestamp_str == "[2023-07-04 14:17:32,032]"

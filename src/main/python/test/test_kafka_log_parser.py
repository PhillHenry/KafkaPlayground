from kafka_log_parser import LogLine, DATETIME_FORMAT


def test_parse_info():
    timestamp = "2023-07-04 14:17:32,032"
    log_level = "INFO"
    thread = "GroupCoordinator 3"
    line = f"kafka3: onNext: RAW: [{timestamp}] {log_level} [{thread}]: Resigned as the group coordinator for partition 8 in epoch OptionalInt[1] (kafka.coordinator.group.GroupCoordinator)"
    log = LogLine(line)
    assert log.machine == "kafka3:"
    assert log.timestamp_str == timestamp
    assert log.timestamp.strftime(DATETIME_FORMAT)[0:len(timestamp)] == timestamp
    assert log.log_level == log_level
    assert log.thread == thread

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


def test_parse_thread():
    log = LogLine("kafka2: onNext: RAW: [2023-07-04 14:13:23,467] INFO [controller-2-ThrottledChannelReaper-ControllerMutation]: Stopped (kafka.server.ClientQuotaManager$ThrottledChannelReaper)\n")
    assert log.thread == "controller-2-ThrottledChannelReaper-ControllerMutation"
    log = LogLine("kafka3: onNext: RAW: [2023-07-04 14:13:05,481] INFO [BrokerLifecycleManager id=3] The broker has caught up. Transitioning from STARTING to RECOVERY. (kafka.server.BrokerLifecycleManager)\n")
    assert log.thread == "BrokerLifecycleManager id=3"

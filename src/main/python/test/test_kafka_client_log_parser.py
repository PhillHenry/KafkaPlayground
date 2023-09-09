from kafka_log_parser import ClientLogLine, DATETIME_FORMAT


def test_parse_client_log():
    timestamp = "2023-07-04 15:13:17,946"
    log_level = "INFO"
    payload = "o.a.kafka.common.utils.AppInfoParser - App info kafka.consumer for consumer-group_PH-1 unregistered"
    line = f"{timestamp} | {log_level} | {payload}"
    log = ClientLogLine(line)
    assert log.machine == ""
    assert log.timestamp_str == timestamp
    assert log.timestamp.strftime(DATETIME_FORMAT)[0:len(timestamp)] == timestamp
    assert log.log_level == log_level
    assert " ".join(log.payload) == payload

from kafka_log_parser import LogLine


def human_readable(x: LogLine) -> str:
    return f"{x.machine} {x.timestamp_str} {' '.join(x.payload)}".replace('\n', '')

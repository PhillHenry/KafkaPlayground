from kafka_log_parser import LogLine


class BColors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNBOLD = '\033[0m'
    RED = "\033[1;31m"
    UNDERLINE = '\033[4m'
    LIGHTGRAY = "\x1b[37m"
    DARKGRAY = "\x1b[90m"


def human_readable(x: LogLine) -> str:
    return f"{x.machine} {x.timestamp_str} {' '.join(x.payload)}".strip()

from datetime import datetime
import re

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S,%f"


class LogLine:
    def __init__(self, line: str):
        elements = line.split(" ")
        self.machine = elements[0]
        self.timestamp_str = (elements[3] + " " + elements[4]).replace("[", "").replace("]", "")
        self.timestamp = datetime.strptime(self.timestamp_str, DATETIME_FORMAT)
        self.log_level = elements[5]
        self.payload = elements[6:]
        self.payload_str = " ".join(self.payload)
        matches = re.fullmatch("^\\[([a-zA-Z0-9\ \-=_]+)\\].*", self.payload_str.strip())
        try:
            self.thread = matches.group(1)
        except:
            self.thread = "None"

    def __str__(self):
        return f"{self.machine} {self.timestamp_str} {self.log_level} {self.payload}"

    def set_bin(self, bin: int):
        self.bin = bin
        return self


def parse_logs_per_host(filename: str) -> dict:
    machine_to_logs = {}
    log_lines = read_file(filename)
    for log in log_lines:
        logs = machine_to_logs.get(log.machine, [])
        machine_to_logs[log.machine] = logs + [log]
    return machine_to_logs


def read_plain_file(filename) -> [str]:
    lines = []
    with open(filename, 'r') as f:
        for line in f:
            lines.append(line.strip())
    return lines


def read_file(filename) -> [LogLine]:
    log_lines = []
    failures = 0
    with open(filename, 'r') as f:
        for line in f:
            try:
                log = LogLine(line)
                log_lines.append(log)
            except Exception:
                print(f"Could not parse line:\n{line}")
                failures += 1
    print(f"Was unable to process a total of {failures} lines out of {len(log_lines)}")
    return sorted(log_lines, key=lambda log: (log.timestamp, log.payload_str))  # logs can be out of sequence. payload_str for tie breakers


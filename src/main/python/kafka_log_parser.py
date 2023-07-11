from datetime import datetime

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S,%f"


class LogLine:
    def __init__(self, line: str):
        elements = line.split(" ")
        self.machine = elements[0]
        self.timestamp_str = (elements[3] + " " + elements[4]).replace("[", "").replace("]", "")
        self.timestamp = datetime.strptime(self.timestamp_str, DATETIME_FORMAT)


def parse_logs(filename: str) -> dict:
    machine_to_logs = {}
    with open(filename, 'r') as f:
        for line in f:
            try:
                log = LogLine(line)
                logs = machine_to_logs.get(log.machine, [])
                machine_to_logs[log.machine] = logs + [log]
            except Exception:
                print(f"Could not parse line:\n{line}")
    return machine_to_logs

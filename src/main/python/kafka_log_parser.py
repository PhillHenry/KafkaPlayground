import re as re
from datetime import datetime, timedelta

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S,%f"


class LogLine():
    def __init__(self, line: str):
        print(line)
        elements = line.split(" ")
        print(elements)
        self.machine = elements[0]
        self.timestamp_str = (elements[3] + " " + elements[4]).replace("[", "").replace("]", "")
        self.timestamp = datetime.strptime(self.timestamp_str, DATETIME_FORMAT)


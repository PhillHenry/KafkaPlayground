import re as re


class LogLine():
    def __init__(self, line: str):
        print(line)
        elements = line.split(" ")
        print(elements)
        self.machine = elements[0]
        self.timestamp_str = elements[3] + " " + elements[4]

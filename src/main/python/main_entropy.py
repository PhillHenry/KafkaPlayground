import sys
from collections import defaultdict

import math

from kafka_log_parser import read_file, LogLine
from rendering import human_readable


def clean(log: LogLine) -> str:
    return log.payload_str.lower().replace("_", " ").replace("-", " ").strip()


def information(filename: str):
    log_lines = read_file(filename)
    n = len(log_lines)
    docs = list(map(clean, log_lines))
    doc_freq = defaultdict(int)
    for doc in docs:
        for word in doc.split(" "):
            if len(word) > 0:
                doc_freq[word] = doc_freq[word] + 1
    entropy = []
    for doc in docs:
        h = 0
        for word in doc.split(" "):
            if len(word) > 0:
                p = doc_freq[word] / n
                h += -p * math.log(p)
        entropy.append(h)
    print(f"num of entropy scores {len(entropy)}")
    pairs = zip(log_lines, entropy)
    top = sorted(pairs, key=lambda x: x[1])[-10:]
    for line, score in top:
        print(f"=== {score} ===")
        print(f"{human_readable(line)}")
    return pairs


if __name__ == "__main__":
    pairs = information(sys.argv[1])
    with open(sys.argv[2], "w") as file:
        for line, score in pairs:
            file.write(human_readable(line))

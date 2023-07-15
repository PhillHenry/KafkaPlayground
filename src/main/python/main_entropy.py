import sys
from collections import defaultdict

import math

from kafka_log_parser import read_file, LogLine
from rendering import human_readable
from text_utils import to_shingles


def clean(log: LogLine) -> str:
    return log.payload_str.lower().replace("_", " ").replace("-", " ").strip()


def information(filename: str):
    log_lines = read_file(filename)
    n = len(log_lines)
    docs = list(map(clean, log_lines))
    doc_freq = defaultdict(int)
    for doc in docs:
        words = to_shingles(doc)
        for word in words:
            if len(word) > 0:
                doc_freq[word] = doc_freq[word] + 1
    entropy = []
    for doc in docs:
        h = 0
        doc_words = set()
        for word in to_shingles(doc):
            p = doc_freq[word] / n
            h += -p * math.log(p)
            doc_words.add(word)
        entropy.append(h)
    print(f"num of entropy scores {len(entropy)}")
    pairs = sorted(list(zip(log_lines, entropy)), key=lambda x: x[1])
    top = pairs[-10:]
    for line, score in top:
        print(f"=== {score} ===")
        print(f"{human_readable(line)}")
    return pairs


if __name__ == "__main__":
    pairs = information(sys.argv[1])
    with open(sys.argv[2], "w") as file:
        for line, score in pairs:
            file.write(f"=== {score} ===\n")
            file.write(human_readable(line))

import sys

from kafka_log_parser import read_file, LogLine
from rendering import human_readable
from text_utils import entropy_of, frequencies

SHINGLES = {3, 4, 5}


def clean(log: LogLine) -> str:
    return log.payload_str.lower().replace("_", " ").replace("-", " ").strip()


def information(filename: str):
    log_lines = read_file(filename)
    docs = list(map(clean, log_lines))
    doc_freq = frequencies(docs, SHINGLES)
    entropy = entropy_of(docs, doc_freq, SHINGLES)
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

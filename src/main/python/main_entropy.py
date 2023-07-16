import sys

from kafka_log_parser import read_file, LogLine
from rendering import human_readable
from text_utils import entropy_of, frequencies, average_entropy_of, camel_case_split

SHINGLES = {3, 4, 5}


def delimiting(x: str) -> str:
    for delimiter in ["_", "-", "=", ".", "(", ")", "[", "]", "$", ",", "/", "{", "}", ":", ",", "@", "'", "\"", "#"]:
        x = x.replace(delimiter, " ")
    return x


def clean(log: LogLine) -> str:
    return delimiting(" ".join(camel_case_split(log.payload_str)).lower()).strip()


def information(filename: str):
    log_lines = read_file(filename)
    docs = list(map(clean, log_lines))

    word_freq = frequencies(docs, SHINGLES)
    doc_entropy = entropy_of(docs, ignoring_common(word_freq, 100), SHINGLES)

    words = list(frequencies(docs, {1}).keys())
    word_shingles = {2, 3,}
    char_freq = frequencies(words, word_shingles, None)
    word_entropy = entropy_of(words, char_freq, word_shingles, None, True)
    word_scores = sorted(zip(words, word_entropy), key=lambda x: x[1])
    for word, score in word_scores[-20:]:
        print(f"=== {score} ===")
        print(f"{word}")

    print(f"num of entropy scores {len(doc_entropy)}")
    pairs = sorted(list(zip(log_lines, doc_entropy)), key=lambda x: x[1])
    for word, score in pairs[-10:]:
        print(f"=== {score} ===")
        print(f"{human_readable(word)}")
    return pairs


def ignoring_common(word_freq: dict, limit: int):
    return {k: v for k, v in word_freq.items() if v <= limit}


if __name__ == "__main__":
    pairs = information(sys.argv[1])
    with open(sys.argv[2], "w") as file:
        for line, score in pairs:
            file.write(f"=== {score} ===\n")
            file.write(human_readable(line))

import sys

from kafka_log_parser import read_file, LogLine, read_plain_file
from rendering import human_readable
from text_utils import entropy_of, frequencies, average_entropy_of, camel_case_split, \
    remove_pure_numbers
import string

CHAR_SHINGLES = {2, 3, }
WORD_SHINGLES = {3, 4, 5}


def delimiting(x: str) -> str:
    for delimiter in string.punctuation:
        x = x.replace(delimiter, " ")
    return x


def clean(log: LogLine) -> str:
    return delimiting(" ".join(camel_case_split(log.payload_str)).lower()).strip()


def information(filename: str, words_file: str):
    log_lines = read_file(filename)
    docs = list(map(clean, log_lines))

    word_freq = frequencies(docs, WORD_SHINGLES)
    doc_entropy = entropy_of(docs, normalize(word_freq), WORD_SHINGLES, penalty=1e-3)
    print("\nDocument entropy")
    print_sample(doc_entropy, log_lines)

    english = read_plain_file(words_file)
    print(f"number of English words is {len(english)}")
    char_freq = entropy_words(docs, english)
    doc_word_entropy = []
    for doc in docs:
        cleaned = " ".join(remove_pure_numbers(doc.split(" ")))
        h = average_entropy_of([cleaned], char_freq, CHAR_SHINGLES, None, True, penalty=1e-2)
        doc_word_entropy.append(h[0])
    print("\nDocument word entropy")
    pairs = print_sample(doc_word_entropy, log_lines)

    return pairs


def print_sample(entropies, log_lines):
    pairs = sorted(list(zip(log_lines, entropies)), key=lambda x: x[1])
    for word, score in pairs[-10:]:
        print(f"=== {score} ===")
        print(f"{human_readable(word)}")
    return pairs


def entropy_words(docs: [str], english: [str]) -> dict:
    words = list(frequencies(docs, {1}).keys())
    words = remove_pure_numbers(words)
    char_freq = frequencies(english, CHAR_SHINGLES, None)
    probabilities = normalize(char_freq)
    word_entropy = entropy_of(words, probabilities, CHAR_SHINGLES, None, True, penalty=1e-2)
    word_scores = sorted(zip(words, word_entropy), key=lambda x: x[1])
    # non_english = list(filter(lambda x: x[0] not in english, word_scores))
    for word, score in [x for x in word_scores if x[0] not in english][-20:]:
        print(f"=== {score} ===")
        print(f"{word}")
    return probabilities


def normalize(token_freq: dict):
    n = sum([v for v in token_freq.values()])
    probabilities = {k: v / n for k, v in token_freq.items()}
    return probabilities


def ignoring_common(word_freq: dict, limit: int):
    return {k: v for k, v in word_freq.items() if v <= limit}


if __name__ == "__main__":
    pairs = information(sys.argv[1], sys.argv[3])
    with open(sys.argv[2], "w") as file:
        for line, score in pairs:
            file.write(f"=== {score} ===\n")
            file.write(human_readable(line))

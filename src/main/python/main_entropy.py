import sys

from kafka_log_parser import read_file, read_plain_file, ClientLogLine, LogLine
from rendering import human_readable
from text_utils import entropy_of, frequencies, average_entropy_of, clean, \
    word_shingle_probabilities_from, normalize, sorted_word_average_entropy

WORD_PENALTY = 1e-2
CHAR_SHINGLES = {2, 3, }
WORD_SHINGLES = {3, 4, 5}


def top_word_to_entropy_tuples(filename: str, english: [str]) -> []:
    log_lines = read_file(filename, lambda x: LogLine(x))
    docs = list(map(clean, log_lines))

    print_entropy_of_entire_doc(docs, log_lines)

    print(f"number of English words is {len(english)}")
    char_freq = word_shingle_probabilities_from(english, CHAR_SHINGLES)
    word_scores = print_most_entropic_words(docs, english, char_freq)
    doc_word_entropy = []
    for doc in docs:
        h = average_entropy_of([doc], char_freq, CHAR_SHINGLES, None, True, penalty=WORD_PENALTY)
        doc_word_entropy.append(h[0])
    print("\nAverage document word entropy")
    pairs = print_sample(doc_word_entropy, log_lines)

    return word_scores


def print_entropy_of_entire_doc(docs, log_lines):
    word_freq = frequencies(docs, WORD_SHINGLES)
    doc_entropy = entropy_of(docs, normalize(word_freq), WORD_SHINGLES, penalty=1e-3)
    print("\nDocument entropy")
    print_sample(doc_entropy, log_lines)


def print_sample(entropies, log_lines):
    pairs = sorted(list(zip(log_lines, entropies)), key=lambda x: x[1])
    score_to_words = {s: w for w, s in pairs}  # unique entropy values
    pairs = sorted([[w, s] for s, w in score_to_words.items()], key=lambda x: x[1])
    for word, score in pairs[-20:]:
        print(f"=== {score} ===")
        print(f"{human_readable(word)}")
    return pairs


def print_most_entropic_words(docs: [str], english: [str], probabilities: dict):
    word_scores = sorted_word_average_entropy(docs, probabilities, CHAR_SHINGLES, WORD_PENALTY)
    for word, score in [x for x in word_scores if x[0] not in english and len(x[0]) > 3][-40:]:
        print(f"=== {score} ===")
        print(f"{word}")
    return word_scores


def ignoring_common(word_freq: dict, limit: int):
    return {k: v for k, v in word_freq.items() if v <= limit}


if __name__ == "__main__":
    english = read_plain_file(sys.argv[3])
    pairs = top_word_to_entropy_tuples(sys.argv[1], english)
    with open(sys.argv[2], "w") as file:
        for line, score in pairs:
            file.write(f"=== {score} ===\n")
            file.write(line)

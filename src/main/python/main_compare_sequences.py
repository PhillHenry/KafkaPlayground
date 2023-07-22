import sys

import matplotlib.pyplot as plt
import numpy as np

from kafka_log_parser import read_file, LogLine, read_plain_file
from text_utils import clean, \
    frequencies, lsh_bin_logs, word_shingle_probabilities_from, words_to_ignore_in
from vectorizing import generate_random_vectors, lsh_projection, one_hot, tf_idf

WORD_SHINGLES = {5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
WORD_PENALTY = 1e-2
CHAR_SHINGLES = {2, 3, 4}
VEC_SIZE = 14


def to_log_index_tuples(x: dict) -> []:
    log_bin = []
    for k in x.keys():
        for log in x[k]:
            log_bin.append((log, k))
    return log_bin


def log_to_index(index_to_log: dict) -> dict:
    log_index = {}
    for index in index_to_log.keys():
        for log in index_to_log[index]:
            log_index[log] = index
    return log_index


def plot_line(log_index: dict, logs: [LogLine], machine: str, colour: str, label: str):
    logs = logs[:1000]
    ys = [log_index[log] for log in logs]
    plt.scatter(range(len(logs)), ys, s=1, c=colour, label=f"{machine} {label}")
    return ys


def information(words_file: str, first: str, second: str):
    first_hash_to_logs, first_lines, second_hash_to_logs, second_lines, _ = sequences_of(first,
                                                                                         second,
                                                                                         words_file,
                                                                                         [])
    print_bins(first_hash_to_logs)
    print_bins(second_hash_to_logs)
    fig, ax = plt.subplots(1, 1)
    fig = plt.figure(figsize=(16, 6))
    machine = "kafka1:"
    first_ys = plot_line(log_to_index(first_hash_to_logs), first_lines, machine, "red", "first run")
    second_ys = plot_line(log_to_index(second_hash_to_logs), second_lines, machine, "blue",
                          "second run")
    plt.legend()
    plt.savefig(f"/tmp/one_hot_{machine}.pdf")
    plt.show()
    return first_ys, second_ys


def is_within(word: str, ignore_words: [str]) -> bool:
    for forbidden in ignore_words:
        if forbidden in word:
            return True
    return False


def sequences_of(first_file: str, second_file: str, words_file, ignore_words: [str]):
    first_lines = read_file(first_file)
    first_docs = list(map(clean, first_lines))
    second_lines = read_file(second_file)
    second_docs = list(map(clean, second_lines))
    print(f"Number of lines = {len(first_docs)}")

    english = read_plain_file(words_file)
    char_freq = word_shingle_probabilities_from(english, CHAR_SHINGLES)
    ignore_words = ignore_words + words_to_ignore_in(first_docs + second_docs, char_freq, CHAR_SHINGLES, WORD_PENALTY)

    min_size = 4
    first_word_count = frequencies(first_docs, WORD_SHINGLES, ignore_words=ignore_words, min_size=min_size)
    second_word_count = frequencies(second_docs, WORD_SHINGLES, ignore_words=ignore_words, min_size=min_size)

    all_words = first_word_count.copy()
    for word, count in second_word_count.items():
        if word in all_words:
            all_words[word] = all_words[word] + second_word_count[word]
        else:
            all_words[word] = second_word_count[word]
    all_words = {w: c for w, c in all_words.items()}  # if 1 < c < len(first_docs) // 100}
    print(
        f"top words: {[w for w, c in sorted(all_words.items(), key=lambda x: -x[1])][:10]}")
    words = list(all_words.keys())

    word_indices = {k: i for i, k in enumerate(words)}
    print(f"Number of words = {len(words)}")
    random_vectors = generate_random_vectors(len(words), VEC_SIZE)
    first_hash_to_logs, first_ignore = make_lsh_bins(first_docs, first_lines, first_word_count, random_vectors,
                                                     word_indices, ignore_words)
    second_hash_to_logs, second_ignore = make_lsh_bins(second_docs, second_lines, second_word_count,
                                                       random_vectors,
                                                       word_indices,
                                                       ignore_words)
    return first_hash_to_logs, first_lines, second_hash_to_logs, second_lines, first_ignore + second_ignore


def make_lsh_bins(docs: [LogLine],
                  lines: [str],
                  first_word_count,
                  random_vectors: np.ndarray,
                  word_indices: dict,
                  ignore_words: []) -> (dict, [str]):

    # df = tf_idf(docs, word_indices, first_word_count, ignore_words, WORD_SHINGLES)
    df = one_hot(docs, word_indices, ignore_words, WORD_SHINGLES)
    bin_indices, bin_indices_bits = lsh_projection(df, random_vectors)
    hash_to_logs = lsh_bin_logs(bin_indices, lines)
    return hash_to_logs, ignore_words


def print_bins(hash_to_logs):
    print(len(hash_to_logs))
    for hash_code, logs in hash_to_logs.items():
        delimiter = f"==== {hash_code} ===="
        print(delimiter)
        if len(logs) > 5:
            for log in logs[:5]:
                line = f"{log}"
                print(line)


if __name__ == "__main__":
    first_ys, second_ys = information(sys.argv[1], sys.argv[2], sys.argv[3])

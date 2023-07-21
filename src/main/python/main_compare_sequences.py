import sys

import matplotlib.pyplot as plt
import numpy as np

from kafka_log_parser import read_file, LogLine, read_plain_file
from text_utils import clean, \
    frequencies, lsh_bin_logs, word_shingle_probabilities_from, words_to_ignore_in
from vectorizing import generate_random_vectors, lsh_projection, one_hot

WORD_SHINGLES = {2, 3}
WORD_PENALTY = 1e-2
CHAR_SHINGLES = {2, 3, }
VEC_SIZE = 12


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
                                                                                         words_file)
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


def sequences_of(first_file: str, second_file: str, words_file):
    first_lines = read_file(first_file)
    first_docs = list(map(clean, first_lines))
    second_lines = read_file(second_file)
    second_docs = list(map(clean, second_lines))
    print(f"Number of lines = {len(first_docs)}")
    first_word_count = frequencies(first_docs, WORD_SHINGLES)
    second_word_count = frequencies(second_docs, WORD_SHINGLES)
    print(
        f"top words: {[w for w, c in sorted(first_word_count.items(), key=lambda x: -x[1])][:10]}")
    all_words = set(list(first_word_count.keys()) + list(second_word_count.keys()))
    words = {w for w in all_words if w in first_word_count.keys() and w in second_word_count.keys()}

    english = read_plain_file(words_file)
    char_freq = word_shingle_probabilities_from(english, CHAR_SHINGLES)

    word_indices = {k: i for i, k in enumerate(words)}
    print(f"Number of words = {len(words)}")
    random_vectors = generate_random_vectors(len(words), VEC_SIZE)
    first_hash_to_logs, first_ignore = make_lsh_bins(first_docs, first_lines, random_vectors,
                                                     word_indices, char_freq)
    second_hash_to_logs, second_ignore = make_lsh_bins(second_docs, second_lines,
                                                       random_vectors,
                                                       word_indices,
                                                       char_freq)
    return first_hash_to_logs, first_lines, second_hash_to_logs, second_lines, first_ignore + second_ignore


def make_lsh_bins(docs: [LogLine],
                  lines: [str],
                  random_vectors: np.ndarray,
                  word_indices: dict,
                  char_freq: dict) -> (dict, [str]):
    ignore_words = words_to_ignore_in(docs, char_freq, CHAR_SHINGLES, WORD_PENALTY)
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

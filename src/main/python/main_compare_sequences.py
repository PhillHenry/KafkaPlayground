import sys
from collections import defaultdict

import math
import matplotlib.pyplot as plt
import numpy as np

from kafka_log_parser import read_file, LogLine, read_plain_file
from text_utils import clean, \
    frequencies, to_shingles, lsh_bin_logs, word_shingle_probabilities_from, words_to_ignore_in
from vectorizing import generate_random_vectors, lsh_projection

WORD_SHINGLES = {2,3}
WORD_PENALTY = 1e-2
CHAR_SHINGLES = {2, 3, }


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


def plot_line(log_index: dict, logs: [LogLine], machine: str, colour: str):
    logs = logs[:1000]
    ys = [log_index[log] for log in logs]
    plt.scatter(range(len(logs)), ys, s=1, c=colour, label=machine)


def information(words_file: str, first: str, second: str):
    english = read_plain_file(words_file)
    char_freq = word_shingle_probabilities_from(english, CHAR_SHINGLES)

    first_lines = read_file(first)
    first_docs = list(map(clean, first_lines))
    second_lines = read_file(second)
    second_docs = list(map(clean, second_lines))
    print(f"Number of lines = {len(first_docs)}")

    first_word_count = frequencies(first_docs, WORD_SHINGLES)
    second_word_count = frequencies(second_docs, WORD_SHINGLES)
    print(f"top words: {[w for w, c in sorted(first_word_count.items(), key=lambda x: -x[1])][:10]}")

    all_words = set(list(first_word_count.keys()) + list(second_word_count.keys()))
    words = {w for w in all_words if w in first_word_count.keys() and w in second_word_count.keys()}
    word_indices = {k: i for i, k in enumerate(words)}
    print(f"Number of words = {len(words)}")

    random_vectors = generate_random_vectors(len(words), 8)
    first_hash_to_logs = make_lsh_bins(first_docs, first_lines, first_word_count, random_vectors,
                                       word_indices, char_freq)
    second_hash_to_logs = make_lsh_bins(second_docs, second_lines, second_word_count, random_vectors,
                                        word_indices, char_freq)
    fig, ax = plt.subplots(1, 1)
    fig = plt.figure(figsize=(16,6))
    machine = "kafka1:"
    plot_line(log_to_index(first_hash_to_logs), first_lines, machine, "red")
    plot_line(log_to_index(second_hash_to_logs), second_lines, machine, "blue")
    plt.savefig(f"/tmp/compare_{machine}.pdf")
    plt.show()


def make_lsh_bins(docs: [LogLine],
                  lines: [str],
                  first_word_count: dict,
                  random_vectors: np.ndarray,
                  word_indices: dict,
                  char_freq: dict) -> dict:
    ignore_words = words_to_ignore_in(docs, char_freq, CHAR_SHINGLES, WORD_PENALTY)
    df = tf_idf(docs, word_indices, first_word_count, ignore_words)
    # df = one_hot(docs, word_indices, ignore_words)
    bin_indices, bin_indices_bits = lsh_projection(df, random_vectors)
    hash_to_logs = lsh_bin_logs(bin_indices, lines)
    print_bins(hash_to_logs)
    return hash_to_logs


def print_bins(hash_to_logs):
    print(len(hash_to_logs))
    for hash_code, logs in hash_to_logs.items():
        delimiter = f"==== {hash_code} ===="
        print(delimiter)
        if len(logs) > 5:
            for log in logs[:5]:
                line = f"{log}"
                print(line)


def one_hot(docs: [str], word_indices: dict, ignore_words: [str]):
    n = len(docs)
    m = np.zeros([n, len(word_indices)], float)
    for i, doc in enumerate(docs):
        for word in to_shingles(doc, WORD_SHINGLES):
            is_valid = True
            for ignoring in ignore_words:
                if ignoring in word:
                    is_valid = False
            if word in word_indices.keys() and is_valid:
                index = word_indices[word]
                m[i, index] = 1.
    return m


def tf_idf(docs: [str], word_indices: dict, word_count: dict, ignore_words: [str]):
    n = len(docs)
    m = np.zeros([n, len(word_indices)], float)
    for i, doc in enumerate(docs):
        doc_word_count = defaultdict(int)
        for word in to_shingles(doc, WORD_SHINGLES):
            is_valid = True
            for ignoring in ignore_words:
                if ignoring in word:
                    is_valid = False
            if is_valid:
                doc_word_count[word] = doc_word_count[word] + 1
        for word, is_valid in doc_word_count.items():
            d = word_count[word]
            f = doc_word_count[word]
            if word in word_indices.keys():
                index = word_indices[word]
                m[i, index] = f * math.log(n / d)
    return m


if __name__ == "__main__":
    information(sys.argv[1], sys.argv[2], sys.argv[3])

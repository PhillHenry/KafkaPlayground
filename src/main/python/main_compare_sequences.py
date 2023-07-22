import sys

import matplotlib.pyplot as plt
import numpy as np

from kafka_log_parser import read_file, LogLine, read_plain_file
from rendering import human_readable
from text_utils import clean, \
    frequencies, lsh_bin_logs, word_shingle_probabilities_from, words_to_ignore_in
from vectorizing import generate_random_vectors, lsh_projection, one_hot, tf_idf, reduce_dimension

WORD_SHINGLES = {x for x in range(30) if x > 1}
WORD_PENALTY = 1e-2
CHAR_SHINGLES = {2, 3, 4, 5}
VEC_SIZE = 24


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


def plot_line(log_index: dict, logs: [LogLine], machine: str, colour: str, label: str, offset: int, mappings: dict):
    logs = logs[:1000]
    ys = reduce_dimension(log_index, logs, mappings)
    plt.scatter(range(len(logs)), [(2 * y) + offset for y in ys], s=1, c=colour, label=f"{machine} {label}")
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
    mappings = dict()
    first_ys = plot_line(log_to_index(first_hash_to_logs), first_lines, machine, "red", "first run", 0, mappings)
    second_ys = plot_line(log_to_index(second_hash_to_logs), second_lines, machine, "blue",
                          "second run", 1, mappings)
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
    first_logs = read_file(first_file)
    first_docs = list(map(clean, first_logs))
    second_logs = read_file(second_file)
    second_docs = list(map(clean, second_logs))

    ignore_words = ignore_words + high_entropy_words(first_docs + second_docs, words_file)

    min_size = 4
    first_word_count = frequencies(first_docs, WORD_SHINGLES, ignore_words=ignore_words, min_size=min_size)
    second_word_count = frequencies(second_docs, WORD_SHINGLES, ignore_words=ignore_words, min_size=min_size)

    words = merge_words(first_word_count, second_word_count)

    word_indices = {k: i for i, k in enumerate(words)}

    random_vectors = generate_random_vectors(len(words), VEC_SIZE)
    first_hash_to_logs, first_ignore = make_lsh_bins(first_docs, first_logs, first_word_count, random_vectors,
                                                     word_indices, ignore_words)
    save_hashes(first_hash_to_logs, "first")
    second_hash_to_logs, second_ignore = make_lsh_bins(second_docs, second_logs, second_word_count,
                                                       random_vectors,
                                                       word_indices,
                                                       ignore_words)
    save_hashes(second_hash_to_logs, "second")
    return first_hash_to_logs, first_logs, second_hash_to_logs, second_logs, first_ignore + second_ignore


def merge_words(first_word_count: dict, second_word_count: dict) -> []:
    all_words = first_word_count.copy()
    for word, count in second_word_count.items():
        if word in all_words:
            all_words[word] = all_words[word] + second_word_count[word]
        else:
            all_words[word] = second_word_count[word]
    all_words = {w: c for w, c in all_words.items() if
                 w in first_word_count.keys() and w in second_word_count.keys()}
    print(
        f"top words: {[w for w, c in sorted(all_words.items(), key=lambda x: -x[1])][:10]}")
    words = list(all_words.keys())
    print(f"Number of words = {len(words)}")
    with open("/tmp/words.txt", "w") as f:
        for word in sorted(words):
            f.write(f"{word}\n")
    return words


def high_entropy_words(docs: [str], words_file: str) -> [str]:
    english = read_plain_file(words_file)
    char_freq = word_shingle_probabilities_from(english, CHAR_SHINGLES)
    high_entropy = words_to_ignore_in(docs, char_freq, CHAR_SHINGLES, WORD_PENALTY)
    return high_entropy


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


def save_hashes(hash_to_logs: dict, label: str):
    with open(f"/tmp/hash_to_logs_{label}.txt", "w") as f:
        for bin, logs in sorted(hash_to_logs.items(), key=lambda x: -len(x[1])):
            f.write(f"=== {bin} ===\n")
            for log in logs:
                f.write(f"{human_readable(log)}\n")


def print_bins(hash_to_logs):
    print(len(hash_to_logs))
    for hash_code, logs in hash_to_logs.items():
        if len(logs) > 5:
            delimiter = f"==== {hash_code} ===="
            print(delimiter)
            for log in logs[:5]:
                print(human_readable(log))


if __name__ == "__main__":
    first_ys, second_ys = information(sys.argv[1], sys.argv[2], sys.argv[3])

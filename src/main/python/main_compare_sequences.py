import sys
from collections import defaultdict

import math
import numpy as np

from kafka_log_parser import read_file
from text_utils import clean, \
    frequencies, to_shingles, lsh_bin
from vectorizing import generate_random_vectors, lsh_projection

WORD_SHINGLES = {2,3}


def information(first: str, second: str):
    first_lines = read_file(first)
    first_docs = list(map(clean, first_lines))
    second_lines = read_file(second)
    second_docs = list(map(clean, second_lines))
    print(f"Number of lines = {len(first_docs)}")

    first_word_count = frequencies(first_docs, WORD_SHINGLES)
    second_word_count = frequencies(second_docs, WORD_SHINGLES)
    print(f"top words: {[w for w, c in sorted(first_word_count.items(), key=lambda x: -x[1])][:10]}")

    words = {w for w in list(first_word_count.keys()) + list(second_word_count.keys())}
    word_indices = {k: i for i, k in enumerate(words)}
    print(f"Number of words = {len(words)}")

    df = tf_idf(first_docs, word_indices, first_word_count)
    random_vectors = generate_random_vectors(len(words), 8)
    bin_indices, bin_indices_bits = lsh_projection(df, random_vectors)
    hash_to_logs = lsh_bin(bin_indices, first_lines)
    print_bins(hash_to_logs)


def print_bins(hash_to_logs):
    print(len(hash_to_logs))
    for hash_code, logs in hash_to_logs.items():
        delimiter = f"==== {hash_code} ===="
        print(delimiter)
        if len(logs) > 5:
            for log in logs[:5]:
                line = f"{log.strip()}"
                print(line)


def tf_idf(docs: [str], word_indices: dict, word_count: dict):
    n = len(docs)
    m = np.zeros([n, len(word_indices)], float)
    for i, doc in enumerate(docs):
        doc_word_count = defaultdict(int)
        for word in to_shingles(doc, WORD_SHINGLES):
            doc_word_count[word] = doc_word_count[word] + 1
        for word, count in doc_word_count.items():
            d = word_count[word]
            f = doc_word_count[word]
            index = word_indices[word]
            m[i, index] = f * math.log(n / d)
        # print(f"{len(doc_word_count)}: {doc}")
    return m


if __name__ == "__main__":
    information(sys.argv[1], sys.argv[2])

import sys
from collections import defaultdict

import math
import numpy as np

from kafka_log_parser import read_file
from text_utils import clean, \
    frequencies, to_shingles

WORD_SHINGLES = {1}


def information(first: str, second: str):
    first_lines = read_file(first)
    first_docs = list(set(map(clean, first_lines)))
    second_lines = read_file(second)
    second_docs = list(set(map(clean, second_lines)))

    first_word_count = frequencies(first_docs, WORD_SHINGLES)
    second_word_count = frequencies(second_docs, WORD_SHINGLES)

    words = {w for w in list(first_word_count.keys()) + list(second_word_count.keys())}
    word_indices = {k: i for i, k in enumerate(words)}

    m = tf_idf(first_docs, word_indices, first_word_count)


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

    return m


if __name__ == "__main__":
    information(sys.argv[1], sys.argv[2])

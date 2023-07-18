from collections import defaultdict

from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
import math
from text_utils import to_shingles


def to_tf_idf_vectors(raw_documents):
    tfidf = TfidfVectorizer(
        analyzer='word',
        ngram_range=(1, 5),
        min_df=0,
        stop_words='english'
    )
    return tfidf.fit_transform(raw_documents), tfidf


def generate_random_vectors(dim, n_vectors) -> np.ndarray:
    """
    generate random projection vectors
    the dims comes first in the matrix's shape,
    so we can use it for matrix multiplication.
    """
    return np.random.randn(dim, n_vectors)


def lsh_projection(df, random_vectors:  np.ndarray):
    bin_indices_bits = df.dot(random_vectors) >= 0
    powers_of_two = 1 << np.arange(random_vectors.shape[1] - 1, -1, step=-1)
    bin_indices = bin_indices_bits.dot(powers_of_two)
    return bin_indices, bin_indices_bits


def one_hot(docs: [str], word_indices: dict, ignore_words: [str], shingles: set[int]):
    n = len(docs)
    m = np.zeros([n, len(word_indices)], float)
    for i, doc in enumerate(docs):
        for word in to_shingles(doc, shingles):
            is_valid = True
            for ignoring in ignore_words:
                if ignoring in word:
                    is_valid = False
            if word in word_indices.keys() and is_valid:
                index = word_indices[word]
                m[i, index] = 1.
    return m


def tf_idf(docs: [str], word_indices: dict, word_count: dict, ignore_words: [str], shingles: set[int]):
    n = len(docs)
    m = np.zeros([n, len(word_indices)], float)
    for i, doc in enumerate(docs):
        doc_word_count = defaultdict(int)
        for word in to_shingles(doc, shingles):
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


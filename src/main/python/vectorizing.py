from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np


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



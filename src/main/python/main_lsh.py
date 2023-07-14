import sys

from kafka_log_parser import read_file
import numpy as np

from vectorizing import generate_random_vectors, to_tf_idf_vectors


def do_lsh(filename: str):
    """
    See http://ethen8181.github.io/machine-learning/recsys/content_based/lsh_text.html
    :param filename: line delimited text file
    :return:
    """
    log_lines = read_file(filename)
    np.random.seed(0)
    df, tfidf = to_tf_idf_vectors([f"{x}" for x in log_lines])
    vocab_size = len(tfidf.get_feature_names_out())
    n_vectors = 16
    random_vectors = generate_random_vectors(vocab_size, n_vectors)
    print(df.shape)
    print(random_vectors.shape)
    find_similar(random_vectors, df)


def find_similar(random_vectors: np.ndarray, tfidf: np.ndarray):
    bin_indices_bits = tfidf.dot(random_vectors) >= 0
    print(bin_indices_bits.shape)
    powers_of_two = 1 << np.arange(random_vectors.shape[1] - 1, -1, step=-1)
    bin_indices = bin_indices_bits.dot(powers_of_two)
    print(bin_indices.shape)


if __name__ == "__main__":
    do_lsh(sys.argv[1])

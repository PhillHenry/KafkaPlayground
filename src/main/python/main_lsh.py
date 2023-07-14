import sys
from collections import defaultdict

import numpy as np

from kafka_log_parser import read_file
from vectorizing import generate_random_vectors, to_tf_idf_vectors


def do_lsh(filename: str, n_vectors: int):
    """
    See http://ethen8181.github.io/machine-learning/recsys/content_based/lsh_text.html
    :param filename: line delimited text file
    :param n_vectors: number of random vectors
    :return: the model
    """
    log_lines = read_file(filename)
    np.random.seed(0)
    df, tfidf = to_tf_idf_vectors([f"{' '.join(x.payload)}" for x in log_lines])
    vocab_size = len(tfidf.get_feature_names_out())
    random_vectors = generate_random_vectors(vocab_size, n_vectors)
    bin_indices_bits = df.dot(random_vectors) >= 0
    powers_of_two = 1 << np.arange(random_vectors.shape[1] - 1, -1, step=-1)
    bin_indices = bin_indices_bits.dot(powers_of_two)
    table = defaultdict(list)
    for idx, bin_index in enumerate(bin_indices):
        table[bin_index].append(idx)

    # note that we're storing the bin_indices here
    # so we can do some ad-hoc checking with it,
    # this isn't actually required
    model = {'table': table,
             'random_vectors': random_vectors,
             'bin_indices': bin_indices,
             'bin_indices_bits': bin_indices_bits,
             'lines': log_lines}
    return table, random_vectors, bin_indices, bin_indices_bits, log_lines


if __name__ == "__main__":
    do_lsh(sys.argv[1], 16)

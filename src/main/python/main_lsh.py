import sys
from collections import defaultdict

import numpy as np

from kafka_log_parser import read_file, LogLine
from text_utils import clean, lsh_bin
from vectorizing import generate_random_vectors, to_tf_idf_vectors, lsh_projection


def do_lsh(filename: str, n_vectors: int):
    """
    See http://ethen8181.github.io/machine-learning/recsys/content_based/lsh_text.html
    :param filename: line delimited text file
    :param n_vectors: number of random vectors
    :return: the model
    """
    log_lines = read_file(filename)
    np.random.seed(0)
    df, tfidf = to_tf_idf_vectors(list(map(clean, log_lines)))
    vocab_size = len(tfidf.get_feature_names_out())
    random_vectors = generate_random_vectors(vocab_size, n_vectors)
    bin_indices, bin_indices_bits = lsh_projection(df, random_vectors)
    table = defaultdict(list)
    for idx, bin_index in enumerate(bin_indices):
        table[bin_index].append(idx)
    return table, random_vectors, bin_indices, bin_indices_bits, log_lines


def compare(bin_indices: np.ndarray,
            lines: list[LogLine],
            out_file: str):
    hash_to_logs = lsh_bin(bin_indices, lines)
    with open(out_file, "w") as file:
        for hash_code, logs in hash_to_logs.items():
            if len(logs) < 2:
                delimiter = f"==== {hash_code} ===="
                print(delimiter)
                file.write(f"{delimiter}\n")
                for log in logs:
                    line = f"{log.strip()}"
                    print(line)
                    file.write(f"{line}\n")


if __name__ == "__main__":
    table, random_vectors, bin_indices, bin_indices_bits, log_lines = do_lsh(sys.argv[1], 8)
    compare(bin_indices, log_lines, sys.argv[2])

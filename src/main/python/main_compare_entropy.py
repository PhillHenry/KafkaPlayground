import sys

from kafka_log_parser import read_plain_file, ClientLogLine
from main_compare_sequences import sequences_of
from main_entropy import top_word_to_entropy_tuples


def top_entropic_words(filename: str, english: [str]) -> [str]:
    return [x[0] for x in top_word_to_entropy_tuples(filename, english)][-10:]


def do_pivot(first_file: str, second_file: str, words_file: str):
    english = read_plain_file(words_file)
    first_words = top_entropic_words(first_file, english)
    second_words = top_entropic_words(second_file, english)
    ignoring = first_words + second_words
    for ignore_word in ignoring:
        print(f"Ignoring {ignore_word}")
    sequences_of(first_file, second_file, words_file, ignoring, lambda x: ClientLogLine(x))


if __name__ == "__main__":
    do_pivot(sys.argv[1], sys.argv[2], sys.argv[3])

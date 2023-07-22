import sys

from main_compare_sequences import sequences_of
from main_lcs import CONTEXT_IGNORE_WORDS
from rendering import human_readable


def compare_lcs(first_file: str, second_file: str, english_file: str):
    first_hash_to_logs, first_logs, second_hash_to_logs, second_logs, ignored_words = sequences_of(
        first_file, second_file, english_file, CONTEXT_IGNORE_WORDS)
    first_only = {k: v for k, v in first_hash_to_logs.items() if k not in second_hash_to_logs}
    second_only = {k: v for k, v in second_hash_to_logs.items() if k not in first_hash_to_logs}
    print("In first only")
    print_hash_to_log(first_only)
    print("\n\nIn second only")
    print_hash_to_log(second_only)


def print_hash_to_log(log_to_hash: dict):
    for k, v in log_to_hash.items():
        print(f"=== {k} ===")
        for log in v:
            print(human_readable(log))


if __name__ == "__main__":
    first_file = sys.argv[2]
    second_file = sys.argv[3]
    english_file = sys.argv[1]
    compare_lcs(first_file, second_file, english_file)

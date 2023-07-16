from collections import defaultdict
import math


def to_shingles(doc: str, ngrams={1, 2, 3, 4, 5}):
    words = [word for word in doc.split(" ") if len(word) > 0]
    shingles = []
    for ngram in ngrams:
        for start in range(len(words) - ngram + 1):
            end = start + ngram
            if end <= len(words):
                shingle = " ".join(words[start:end])
                shingles.append(shingle)
    return shingles


def entropy_of(docs, doc_freq, shingles):
    entropy = []
    for doc in docs:
        h = 0
        doc_words = set()
        for word in to_shingles(doc, shingles):
            if word not in doc_words:
                p = doc_freq[word] / len(docs)
                h += -p * math.log(p)
                doc_words.add(word)
        entropy.append(h)
    return entropy


def frequencies(docs, shingles):
    doc_freq = defaultdict(int)
    for doc in docs:
        words = to_shingles(doc, shingles)
        for word in words:
            if len(word) > 0:
                doc_freq[word] = doc_freq[word] + 1
    return doc_freq



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

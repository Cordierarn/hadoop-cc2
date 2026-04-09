"""
Q4 - Combien de fois chaque tag a-t-il ete utilise pour taguer un film ?
Configuration Hadoop : taille de bloc = 64 Mo.
"""
from mrjob.job import MRJob
from mrjob.step import MRStep


class TagUsageCount(MRJob):

    def mapper(self, _, line):
        try:
            parts = line.split(',')
            if parts[0] == 'userId':
                return  # ignorer l'en-tete
            # Le tag peut contenir des virgules : on prend tout entre movieId et timestamp
            tag = ','.join(parts[2:-1]).strip().lower()
            yield tag, 1
        except Exception:
            pass

    def reducer(self, tag, counts):
        yield tag, sum(counts)


if __name__ == '__main__':
    TagUsageCount.run()

"""
Q4 - Combien de fois chaque tag a-t-il ete utilise pour taguer un film ?
Configuration Hadoop : taille de bloc = 64 Mo.
"""
from mrjob.job import MRJob


class ComptageTags(MRJob):

    def mapper(self, _, ligne):
        try:
            elements = ligne.split(',')
            if elements[0] == 'userId':
                return  # ignorer l'en-tete
            # Le tag peut contenir des virgules : on prend tout entre movieId et timestamp
            tag = ','.join(elements[2:-1]).strip().lower()
            yield tag, 1
        except Exception:
            pass

    def reducer(self, tag, occurences):
        yield tag, sum(occurences)


if __name__ == '__main__':
    ComptageTags.run()

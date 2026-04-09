"""
Q2 - Combien de tags chaque utilisateur a-t-il ajoutes ?
Configuration Hadoop par defaut.
"""
from mrjob.job import MRJob
from mrjob.step import MRStep


class TagsPerUser(MRJob):

    def mapper(self, _, line):
        try:
            parts = line.split(',')
            if parts[0] == 'userId':
                return  # ignorer l'en-tete
            userId = parts[0]
            yield userId, 1
        except Exception:
            pass

    def reducer(self, userId, counts):
        yield userId, sum(counts)


if __name__ == '__main__':
    TagsPerUser.run()

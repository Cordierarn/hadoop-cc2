"""
Q1 - Combien de tags chaque film possede-t-il ?
Configuration Hadoop par defaut.
"""
from mrjob.job import MRJob
from mrjob.step import MRStep


class TagsPerMovie(MRJob):

    def mapper(self, _, line):
        try:
            parts = line.split(',')
            if parts[0] == 'userId':
                return  # ignorer l'en-tete
            movieId = parts[1]
            yield movieId, 1
        except Exception:
            pass

    def reducer(self, movieId, counts):
        yield movieId, sum(counts)


if __name__ == '__main__':
    TagsPerMovie.run()

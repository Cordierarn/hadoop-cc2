"""
Q5 - Pour chaque film, combien de tags le meme utilisateur a-t-il introduits ?
Configuration Hadoop : taille de bloc = 64 Mo.
"""
from mrjob.job import MRJob
from mrjob.step import MRStep


class TagsPerUserMovie(MRJob):

    def mapper(self, _, line):
        try:
            parts = line.split(',')
            if parts[0] == 'userId':
                return  # ignorer l'en-tete
            userId = parts[0]
            movieId = parts[1]
            yield (movieId + "\t" + userId), 1
        except Exception:
            pass

    def reducer(self, movie_user, counts):
        yield movie_user, sum(counts)


if __name__ == '__main__':
    TagsPerUserMovie.run()

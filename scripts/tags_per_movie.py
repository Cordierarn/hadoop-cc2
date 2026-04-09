"""
Q1 - Combien de tags chaque film possede-t-il ?
Configuration Hadoop par defaut.
"""
from mrjob.job import MRJob

class TagsParFilm(MRJob):

    def mapper(self, _, ligne):
        try:
            elements = ligne.split(',')
            if elements[0] == 'userId':
                return  # ignorer l'en-tete
            id_film = elements[1]
            yield id_film, 1
        except Exception:
            pass

    def reducer(self, id_film, occurences):
        yield id_film, sum(occurences)

if __name__ == '__main__':
    TagsParFilm.run()

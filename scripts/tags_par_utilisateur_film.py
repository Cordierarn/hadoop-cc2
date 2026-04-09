"""
Q5 - Pour chaque film, combien de tags le meme utilisateur a-t-il introduits ?
Configuration Hadoop : taille de bloc = 64 Mo.
"""
from mrjob.job import MRJob


class TagsParUtilisateurFilm(MRJob):

    def mapper(self, _, ligne):
        try:
            elements = ligne.split(',')
            if elements[0] == 'userId':
                return  # ignorer l'en-tete
            id_utilisateur = elements[0]
            id_film = elements[1]
            yield (id_film + "\t" + id_utilisateur), 1
        except Exception:
            pass

    def reducer(self, film_utilisateur, occurences):
        yield film_utilisateur, sum(occurences)


if __name__ == '__main__':
    TagsParUtilisateurFilm.run()

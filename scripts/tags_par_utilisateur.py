"""
Q2 - Combien de tags chaque utilisateur a-t-il ajoutes ?
Configuration Hadoop par defaut.
"""
from mrjob.job import MRJob


class TagsParUtilisateur(MRJob):

    def mapper(self, _, ligne):
        try:
            elements = ligne.split(',')
            if elements[0] == 'userId':
                return  # ignorer l'en-tete
            id_utilisateur = elements[0]
            yield id_utilisateur, 1
        except Exception:
            pass

    def reducer(self, id_utilisateur, occurences):
        yield id_utilisateur, sum(occurences)


if __name__ == '__main__':
    TagsParUtilisateur.run()

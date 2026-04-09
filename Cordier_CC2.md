# CC2 - Pratique Hadoop MapReduce

**Auteur :** Chabanel Tristan, Desvalcy Andrew, Cordier Arnaud  
**Date :** 09/04/2026  
**Fichier source :** `ml-25m/tags.csv`  
**Repo GitHub :** [https://github.com/Cordierarn/hadoop-cc2](https://github.com/Cordierarn/hadoop-cc2)

---

## Table des matieres

1. [Preparation de l'environnement](#1-preparation-de-lenvironnement)
2. [Partie 1 - Configuration Hadoop par defaut](#2-partie-1---configuration-hadoop-par-defaut)
   - [Q1 - Tags par film](#q1---combien-de-tags-chaque-film-possede-t-il-)
   - [Q2 - Tags par utilisateur](#q2---combien-de-tags-chaque-utilisateur-a-t-il-ajoutes-)
3. [Partie 2 - Configuration Hadoop avec blocs de 64 Mo](#3-partie-2---configuration-hadoop-avec-blocs-de-64-mo)
   - [Q3 - Nombre de blocs HDFS](#q3---combien-de-blocs-le-fichier-occupe-t-il-dans-hdfs-)
   - [Q4 - Frequence d'utilisation de chaque tag](#q4---combien-de-fois-chaque-tag-a-t-il-ete-utilise-)
   - [Q5 - Tags par utilisateur par film](#q5---pour-chaque-film-combien-de-tags-le-meme-utilisateur-a-t-il-introduits-)

---

## 1. Preparation de l'environnement

> Toutes les commandes ci-dessous sont a executer sur la sandbox HDP, connecte en SSH en tant que `maria_dev`.

### Etape 1 - Telechargement et extraction du dataset

```bash
cd ~
wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
unzip ml-25m.zip
```

### Etape 2 - Exploration du fichier tags.csv

```bash
head -5 ml-25m/tags.csv
```

**Resultat :**
```
userId,movieId,tag,timestamp
3,260,classic,1439472355
3,260,sci-fi,1439472256
4,1732,dark comedy,1573943598
4,1732,great dialogue,1573943604
```

Le fichier `tags.csv` est au format CSV (separe par des virgules) avec 4 colonnes :
- `userId` : identifiant de l'utilisateur
- `movieId` : identifiant du film
- `tag` : le tag attribue (peut contenir des virgules)
- `timestamp` : horodatage UNIX

```bash
ls -lh ml-25m/tags.csv
wc -l ml-25m/tags.csv
```

**Resultat :**
```
-rw-rw-r-- 1 maria_dev maria_dev 38M Nov 21  2019 ml-25m/tags.csv
1093361 ml-25m/tags.csv
```

Le fichier fait **37 Mo** et contient **1 093 360 tags** (+ 1 ligne d'en-tete).

### Etape 3 - Creation d'un fichier d'echantillon pour les tests

Avant de lancer les jobs sur le fichier complet, on cree un petit echantillon pour valider nos scripts :

```bash
head -1 ml-25m/tags.csv > ~/tags_test.csv
tail -n +2 ml-25m/tags.csv | head -20 >> ~/tags_test.csv
```

### Etape 4 - Chargement des fichiers dans HDFS

On charge le fichier dans HDFS avec deux configurations differentes :

```bash
# Configuration par defaut (blocs de 128 Mo)
hdfs dfs -mkdir -p /user/maria_dev/cc2/input
hdfs dfs -put ~/ml-25m/tags.csv /user/maria_dev/cc2/input/tags.csv
hdfs dfs -put ~/tags_test.csv /user/maria_dev/cc2/input/tags_test.csv

# Configuration avec blocs de 64 Mo
hdfs dfs -mkdir -p /user/maria_dev/cc2/input_64mb
hdfs dfs -D dfs.blocksize=67108864 -put ~/ml-25m/tags.csv /user/maria_dev/cc2/input_64mb/tags.csv
```

Verification :
```bash
hdfs dfs -ls /user/maria_dev/cc2/input/
hdfs dfs -ls /user/maria_dev/cc2/input_64mb/
```

### Etape 5 - Recuperation des scripts Python sur la sandbox

Les scripts MRJob sont disponibles dans le repertoire [`scripts/`](scripts/) du repo GitHub. On les recupere directement avec `wget` :

```bash
mkdir -p ~/scripts
wget -O ~/scripts/tags_par_film.py https://raw.githubusercontent.com/Cordierarn/hadoop-cc2/master/scripts/tags_par_film.py
wget -O ~/scripts/tags_par_utilisateur.py https://raw.githubusercontent.com/Cordierarn/hadoop-cc2/master/scripts/tags_par_utilisateur.py
wget -O ~/scripts/comptage_tags.py https://raw.githubusercontent.com/Cordierarn/hadoop-cc2/master/scripts/comptage_tags.py
wget -O ~/scripts/tags_par_utilisateur_film.py https://raw.githubusercontent.com/Cordierarn/hadoop-cc2/master/scripts/tags_par_utilisateur_film.py
```

Verification :
```bash
ls -la ~/scripts/
```

**Resultat attendu :**
```
tags_par_film.py
tags_par_utilisateur.py
comptage_tags.py
tags_par_utilisateur_film.py
```

L'environnement est pret. On peut maintenant repondre aux questions.

---

## 2. Partie 1 - Configuration Hadoop par defaut

> La configuration par defaut de Hadoop sur HDP utilise une **taille de bloc de 128 Mo**.

### Q1 - Combien de tags chaque film possede-t-il ?

**Script :** [`scripts/tags_par_film.py`](scripts/tags_par_film.py)

```python
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
```

**Logique MapReduce :**
- **Mapper** : pour chaque ligne, on cree une paire cle/valeur `(id_film, 1)`. On ignore la ligne d'en-tete et on encapsule dans un `try/except` pour gerer les lignes malformees.
- **Reducer** : somme toutes les valeurs pour chaque `id_film`, donnant le nombre total de tags par film.

**Etape 6 - Test sur l'echantillon :**

```bash
python ~/scripts/tags_par_film.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/cc2/input/tags_test.csv -o hdfs:///user/maria_dev/cc2/output/q1_sample
```

Affichage du resultat :
```bash
hdfs dfs -cat /user/maria_dev/cc2/output/q1_sample/part-*
```

**Resultat attendu :**
```
"115569"        1                                 
"115713"        3 
...
```

**Etape 7 - Execution sur le fichier complet :**

```bash
python ~/scripts/tags_par_film.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/cc2/input/tags.csv -o hdfs:///user/maria_dev/cc2/output/q1_tags_par_film
```

Recuperation du fichier de resultats :
```bash
hdfs dfs -getmerge /user/maria_dev/cc2/output/q1_tags_par_film ~/q1_tags_par_film.txt
head -20 ~/q1_tags_par_film.txt
```

**Analyse :** Le job a traite **1 093 360 enregistrements** et produit **45 251 films** distincts. Le fichier de resultats contient le nombre de tags pour chacun des 45 251 films presents dans le dataset. Ce fichier etant volumineux, il est disponible sur le repo GitHub.

> **Fichier resultat :** [q1_tags_par_film.txt](https://github.com/Cordierarn/hadoop-cc2/blob/main/results/q1_tags_par_film/q1_tags_par_film.txt)

**Extrait des 20 premiers resultats :**

```
"1"	697
"10"	137
"100"	18
"1000"	10
"100001"	1
"100003"	3
"100008"	9
"100017"	9
"100032"	2
"100034"	19
"100036"	1
"100038"	4
"100042"	2
"100044"	12
"100046"	3
"100048"	1
"100052"	4
"100054"	6
"100060"	10
"100062"	2
```

On observe par exemple que le film **1** (Toy Story) possede **697 tags**, ce qui en fait un des films les plus tagges du dataset.

---

### Q2 - Combien de tags chaque utilisateur a-t-il ajoutes ?

**Script :** [`scripts/tags_par_utilisateur.py`](scripts/tags_par_utilisateur.py)

```python
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
```

**Logique MapReduce :**
- **Mapper** : pour chaque ligne, on cree une paire cle/valeur `(id_utilisateur, 1)`.
- **Reducer** : somme toutes les valeurs pour chaque `id_utilisateur`, donnant le nombre total de tags par utilisateur.

**Etape 8 - Test sur l'echantillon :**

```bash
python ~/scripts/tags_par_utilisateur.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/cc2/input/tags_test.csv -o hdfs:///user/maria_dev/cc2/output/q2_sample
```

```bash
hdfs dfs -cat /user/maria_dev/cc2/output/q2_sample/part-*
```

**Etape 9 - Execution sur le fichier complet :**

```bash
python ~/scripts/tags_par_utilisateur.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/cc2/input/tags.csv -o hdfs:///user/maria_dev/cc2/output/q2_tags_par_utilisateur
```

Recuperation du fichier de resultats :
```bash
hdfs dfs -getmerge /user/maria_dev/cc2/output/q2_tags_par_utilisateur ~/q2_tags_par_utilisateur.txt
head -20 ~/q2_tags_par_utilisateur.txt
```

**Analyse :** Le job a produit **14 592 utilisateurs** distincts. Le fichier de resultats contient le nombre de tags pour chacun des 14 592 utilisateurs.

> **Fichier resultat :** [q2_tags_par_utilisateur.txt](https://github.com/Cordierarn/hadoop-cc2/blob/main/results/q2_tags_par_utilisateur/q2_tags_par_utilisateur.txt)

**Extrait des 20 premiers resultats :**

```
"100001"	9
"100016"	50
"100028"	4
"100029"	1
"100033"	1
"100046"	133
"100051"	19
"100058"	5
"100065"	2
"100068"	19
"100076"	4
"100085"	3
"100087"	8
"100088"	13
"100091"	29
"100101"	3
"100125"	3
"100130"	2
"100140"	5
"100141"	26
```

On observe une grande disparite : certains utilisateurs n'ont ajoute qu'un seul tag tandis que d'autres en ont ajoute plus d'une centaine (ex: utilisateur 100046 avec 133 tags).

---

## 3. Partie 2 - Configuration Hadoop avec blocs de 64 Mo

### Q3 - Combien de blocs le fichier occupe-t-il dans HDFS ?

Pour cette question, on compare le nombre de blocs dans deux configurations :
1. **Configuration par defaut** : taille de bloc = 128 Mo
2. **Configuration modifiee** : taille de bloc = 64 Mo

Le fichier a deja ete charge dans les deux configurations a l'etape 4.

**Etape 10 - Verification des blocs (config par defaut, 128 Mo) :**

```bash
hdfs fsck /user/maria_dev/cc2/input/tags.csv -files -blocks
```

**Resultat :**
```
/user/maria_dev/cc2/input/tags.csv 38810332 bytes, 1 block(s):  OK
0. BP-243674277-172.17.0.2-1529333510191:blk_1073743320_2502 len=38810332 repl=1

Status: HEALTHY
 Total size:	38810332 B
 Total blocks (validated):	1 (avg. block size 38810332 B)
```

**Etape 11 - Verification des blocs (config 64 Mo) :**

```bash
hdfs fsck /user/maria_dev/cc2/input_64mb/tags.csv -files -blocks
```

**Resultat :**
```
/user/maria_dev/cc2/input_64mb/tags.csv 38810332 bytes, 1 block(s):  OK
0. BP-243674277-172.17.0.2-1529333510191:blk_1073743321_2503 len=38810332 repl=1

Status: HEALTHY
 Total size:	38810332 B
 Total blocks (validated):	1 (avg. block size 38810332 B)
```

**Analyse :**

| Configuration | Taille de bloc | Taille du fichier | Nombre de blocs |
|---|---|---|---|
| Par defaut | 128 Mo | 37 Mo (38 810 332 B) | **1** |
| Modifiee | 64 Mo | 37 Mo (38 810 332 B) | **1** |

**Formule :** `nombre_de_blocs = ceil(taille_fichier / taille_bloc)`
- 128 Mo : `ceil(37 / 128) = 1`
- 64 Mo : `ceil(37 / 64) = 1`

> **Remarque :** Le fichier `tags.csv` fait 37 Mo, ce qui est inferieur aux deux tailles de bloc (64 Mo et 128 Mo). Il occupe donc **1 seul bloc** dans les deux configurations. Pour observer une difference, il faudrait un fichier de plus de 64 Mo.

---

### Q4 - Combien de fois chaque tag a-t-il ete utilise ?

**Script :** [`scripts/comptage_tags.py`](scripts/comptage_tags.py)

```python
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
```

**Logique MapReduce :**
- **Mapper** : pour chaque ligne, extrait le tag (colonnes entre `movieId` et `timestamp`). On utilise `','.join(elements[2:-1])` pour gerer les tags contenant des virgules. Le tag est normalise en minuscules avec `.lower()` pour regrouper les variantes de casse. Emet `(tag, 1)`.
- **Reducer** : somme les occurrences pour chaque tag.

**Etape 12 - Test sur l'echantillon :**

```bash
python ~/scripts/comptage_tags.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/cc2/input/tags_test.csv -o hdfs:///user/maria_dev/cc2/output/q4_sample
```

```bash
hdfs dfs -cat /user/maria_dev/cc2/output/q4_sample/part-*
```

**Etape 13 - Execution sur le fichier complet (blocs de 64 Mo) :**

```bash
python ~/scripts/comptage_tags.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/cc2/input_64mb/tags.csv -o hdfs:///user/maria_dev/cc2/output/q4_comptage_tags
```

Recuperation du fichier de resultats :
```bash
hdfs dfs -getmerge /user/maria_dev/cc2/output/q4_comptage_tags ~/q4_comptage_tags.txt
head -20 ~/q4_comptage_tags.txt
```

**Analyse :** Le job a identifie **65 414 tags uniques**. Le fichier de resultats contient la frequence d'utilisation de chaque tag.

> **Fichier resultat :** [q4_comptage_tags.txt](https://github.com/Cordierarn/hadoop-cc2/blob/main/results/q4_comptage_tags/q4_comptage_tags.txt)

**Extrait des 20 premiers resultats :**

```
"!950's superman tv show"	1
"#1 prediction"	3
"#adventure"	1
"#antichrist"	1
"#boring #lukeiamyourfather"	1
"#boring"	1
"#danish"	2
"#documentary"	1
"#entertaining"	1
"#exorcism"	1
"#fantasy"	2
"#hanks #muchstories"	1
"#jesus"	1
"#lifelessons"	1
"#lukeiamyourfather"	1
"#metoo"	1
"#mindfulness"	1
"#notscary"	1
"#rap"	1
"#science"	1
```

On observe une tres grande variete de tags : beaucoup sont des tags de niche utilises une seule fois, tandis que les tags generiques comme "sci-fi", "comedy" ou "based on a book" sont utilises des centaines de fois.

---

### Q5 - Pour chaque film, combien de tags le meme utilisateur a-t-il introduits ?

**Script :** [`scripts/tags_par_utilisateur_film.py`](scripts/tags_par_utilisateur_film.py)

```python
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
```

**Logique MapReduce :**
- **Mapper** : pour chaque ligne, on cree une paire cle/valeur `(id_film + TAB + id_utilisateur, 1)`. La cle composite `id_film\tid_utilisateur` permet de regrouper les tags par couple (film, utilisateur).
- **Reducer** : somme les valeurs pour chaque couple `(id_film, id_utilisateur)`, donnant le nombre de tags qu'un meme utilisateur a attribues a un meme film.

**Etape 14 - Test sur l'echantillon :**

```bash
python ~/scripts/tags_par_utilisateur_film.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/cc2/input/tags_test.csv -o hdfs:///user/maria_dev/cc2/output/q5_sample
```

```bash
hdfs dfs -cat /user/maria_dev/cc2/output/q5_sample/part-*
```

**Etape 15 - Execution sur le fichier complet (blocs de 64 Mo) :**

```bash
python ~/scripts/tags_par_utilisateur_film.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/cc2/input_64mb/tags.csv -o hdfs:///user/maria_dev/cc2/output/q5_tags_par_utilisateur_film
```

Recuperation du fichier de resultats :
```bash
hdfs dfs -getmerge /user/maria_dev/cc2/output/q5_tags_par_utilisateur_film ~/q5_tags_par_utilisateur_film.txt
head -20 ~/q5_tags_par_utilisateur_film.txt
```

**Analyse :** Le job a produit **305 356 couples (film, utilisateur)** distincts. Le fichier de resultats contient, pour chaque couple, le nombre de tags que cet utilisateur a attribues a ce film.

> **Fichier resultat :** [q5_tags_par_utilisateur_film.txt](https://github.com/Cordierarn/hadoop-cc2/blob/main/results/q5_tags_par_utilisateur_film/q5_tags_par_utilisateur_film.txt)

**Extrait des 20 premiers resultats :**

```
"100001\t6550"	1
"100003\t6550"	2
"100003\t70092"	1
"100008\t21096"	2
"100008\t62199"	1
"100008\t6550"	6
"100017\t103126"	1
"100017\t62199"	1
"100017\t6550"	7
"100032\t62199"	1
"100032\t70092"	1
"100034\t129101"	3
"100034\t14116"	9
"100034\t62199"	2
"100034\t6550"	4
"100034\t70092"	1
"100036\t62199"	1
"100038\t62199"	1
"100038\t6550"	2
"100038\t70092"	1
```

Le format de sortie est `"id_film\tid_utilisateur" nombre_de_tags`. Par exemple, l'utilisateur **6550** a ajoute **7 tags** au film **100017**, tandis que l'utilisateur **14116** a ajoute **9 tags** au film **100034**. On observe que certains utilisateurs sont tres actifs sur certains films.

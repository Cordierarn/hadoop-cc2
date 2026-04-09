# CC2 - Pratique Hadoop MapReduce

**Auteur :** Chabanel Tristan, Desvalcy Andrew, Cordier Arnaud  
**Date :** 09/04/2026  
**Fichier source :** `ml-25m/tags.csv`  
**Repo GitHub :** [https://github.com/Cordierarn/hadoop-cc2](https://github.com/Cordierarn/hadoop-cc2)

---

## Table des matières

1. [Préparation de l'environnement](#1-préparation-de-lenvironnement)
2. [Partie 1 - Configuration Hadoop par défaut](#2-partie-1---configuration-hadoop-par-défaut)
   - [Q1 - Tags par film](#q1---combien-de-tags-chaque-film-possède-t-il-)
   - [Q2 - Tags par utilisateur](#q2---combien-de-tags-chaque-utilisateur-a-t-il-ajoutés-)
3. [Partie 2 - Configuration Hadoop avec blocs de 64 Mo](#3-partie-2---configuration-hadoop-avec-blocs-de-64-mo)
   - [Q3 - Nombre de blocs HDFS](#q3---combien-de-blocs-le-fichier-occupe-t-il-dans-hdfs-)
   - [Q4 - Fréquence d'utilisation de chaque tag](#q4---combien-de-fois-chaque-tag-a-t-il-été-utilisé-)
   - [Q5 - Tags par utilisateur par film](#q5---pour-chaque-film-combien-de-tags-le-même-utilisateur-a-t-il-introduits-)

---

## 1. Préparation de l'environnement

> Toutes les commandes ci-dessous sont à exécuter sur la sandbox HDP, connecté en SSH en tant que `maria_dev`.

### Étape 1 - Téléchargement et extraction du dataset

```bash
cd ~
wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
unzip ml-25m.zip
```

### Étape 2 - Exploration du fichier tags.csv

```bash
head -5 ml-25m/tags.csv
```

**Résultat :**
```
userId,movieId,tag,timestamp
3,260,classic,1439472355
3,260,sci-fi,1439472256
4,1732,dark comedy,1573943598
4,1732,great dialogue,1573943604
```

Le fichier `tags.csv` est au format CSV (séparé par des virgules) avec 4 colonnes :
- `userId` : identifiant de l'utilisateur
- `movieId` : identifiant du film
- `tag` : le tag attribué (peut contenir des virgules)
- `timestamp` : horodatage UNIX

```bash
ls -lh ml-25m/tags.csv
wc -l ml-25m/tags.csv
```

**Résultat :**
```
-rw-rw-r-- 1 maria_dev maria_dev 38M Nov 21  2019 ml-25m/tags.csv
1093361 ml-25m/tags.csv
```

Le fichier fait **37 Mo** et contient **1 093 360 tags** (+ 1 ligne d'en-tête).

### Étape 3 - Création d'un fichier d'échantillon pour les tests

Avant de lancer les jobs sur le fichier complet, on crée un petit échantillon pour valider nos scripts :

```bash
head -1 ml-25m/tags.csv > ~/tags_test.csv
tail -n +2 ml-25m/tags.csv | head -20 >> ~/tags_test.csv
```

### Étape 4 - Chargement des fichiers dans HDFS

On charge le fichier dans HDFS avec deux configurations différentes :

```bash
# Configuration par défaut (blocs de 128 Mo)
hdfs dfs -mkdir -p /user/maria_dev/cc2/input
hdfs dfs -put ~/ml-25m/tags.csv /user/maria_dev/cc2/input/tags.csv
hdfs dfs -put ~/tags_test.csv /user/maria_dev/cc2/input/tags_test.csv

# Configuration avec blocs de 64 Mo
hdfs dfs -mkdir -p /user/maria_dev/cc2/input_64mb
hdfs dfs -D dfs.blocksize=67108864 -put ~/ml-25m/tags.csv /user/maria_dev/cc2/input_64mb/tags.csv
```

Vérification :
```bash
hdfs dfs -ls /user/maria_dev/cc2/input/
hdfs dfs -ls /user/maria_dev/cc2/input_64mb/
```

### Étape 5 - Récupération des scripts Python sur la sandbox

Les scripts MRJob sont disponibles dans le répertoire [`scripts/`](scripts/) du repo GitHub. On les récupère directement avec `wget` :

```bash
mkdir -p ~/scripts
wget -O ~/scripts/tags_par_film.py https://raw.githubusercontent.com/Cordierarn/hadoop-cc2/master/scripts/tags_par_film.py
wget -O ~/scripts/tags_par_utilisateur.py https://raw.githubusercontent.com/Cordierarn/hadoop-cc2/master/scripts/tags_par_utilisateur.py
wget -O ~/scripts/comptage_tags.py https://raw.githubusercontent.com/Cordierarn/hadoop-cc2/master/scripts/comptage_tags.py
wget -O ~/scripts/tags_par_utilisateur_film.py https://raw.githubusercontent.com/Cordierarn/hadoop-cc2/master/scripts/tags_par_utilisateur_film.py
```

Vérification :
```bash
ls -la ~/scripts/
```

**Résultat attendu :**
```
tags_par_film.py
tags_par_utilisateur.py
comptage_tags.py
tags_par_utilisateur_film.py
```

L'environnement est prêt. On peut maintenant répondre aux questions.

---

## 2. Partie 1 - Configuration Hadoop par défaut

> La configuration par défaut de Hadoop sur HDP utilise une **taille de bloc de 128 Mo**.

### Q1 - Combien de tags chaque film possède-t-il ?

**Script :** [`scripts/tags_par_film.py`](scripts/tags_par_film.py)

```python
from mrjob.job import MRJob

class TagsParFilm(MRJob):

    def mapper(self, _, ligne):
        try:
            elements = ligne.split(',')
            if elements[0] == 'userId':
                return  # ignorer l'en-tête
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
- **Mapper** : pour chaque ligne, on crée une paire clé/valeur `(id_film, 1)`. On ignore la ligne d'en-tete et on encapsule dans un `try/except` pour gérer les lignes malformées.
- **Reducer** : somme toutes les valeurs pour chaque `id_film`, donnant le nombre total de tags par film.

**Étape 6 - Test sur l'échantillon :**

```bash
python ~/scripts/tags_par_film.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/cc2/input/tags_test.csv -o hdfs:///user/maria_dev/cc2/output/q1_sample
```

Affichage du résultat :
```bash
hdfs dfs -cat /user/maria_dev/cc2/output/q1_sample/part-*
```

**Résultat attendu :**
```
"115569"        1                                 
"115713"        3 
...
```

**Étape 7 - Exécution sur le fichier complet :**

```bash
python ~/scripts/tags_par_film.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/cc2/input/tags.csv -o hdfs:///user/maria_dev/cc2/output/q1_tags_par_film
```

Récupération du fichier de résultats :
```bash
hdfs dfs -getmerge /user/maria_dev/cc2/output/q1_tags_par_film ~/q1_tags_par_film.txt
```
Puis : 
```bash
head -20 ~/q1_tags_par_film.txt
```

**Analyse :** Le job a traité **1 093 360 enregistrements** et a produit **45 251 films** distincts. Le fichier de résultats contient le nombre de tags pour chacun des 45 251 films présents dans le dataset. Ce fichier étant volumineux, il est disponible sur le repo GitHub.

> **Fichier résultat :** [q1_tags_par_film.txt](https://github.com/Cordierarn/hadoop-cc2/blob/master/results/q1_tags_par_film/q1_tags_par_film.txt)

**Extrait des 20 premiers résultats :**

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

On observe par exemple que le film **1** possède **697 tags**, ce qui en fait un des films les plus taggés du dataset.

---

### Q2 - Combien de tags chaque utilisateur a-t-il ajoutés ?

**Script :** [`scripts/tags_par_utilisateur.py`](https://github.com/Cordierarn/hadoop-cc2/blob/master/scripts/tags_par_utilisateur.py)
```python
from mrjob.job import MRJob

class TagsParUtilisateur(MRJob):

    def mapper(self, _, ligne):
        try:
            elements = ligne.split(',')
            if elements[0] == 'userId':
                return  # ignorer l'en-tête
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
- **Mapper** : pour chaque ligne, on crée une paire clé/valeur `(id_utilisateur, 1)`.
- **Reducer** : somme toutes les valeurs pour chaque `id_utilisateur`, donnant le nombre total de tags par utilisateur.

**Étape 8 - Test sur l'échantillon :**

```bash
python ~/scripts/tags_par_utilisateur.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/cc2/input/tags_test.csv -o hdfs:///user/maria_dev/cc2/output/q2_sample
```

```bash
hdfs dfs -cat /user/maria_dev/cc2/output/q2_sample/part-*
```

**Étape 9 - Exécution sur le fichier complet :**

```bash
python ~/scripts/tags_par_utilisateur.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/cc2/input/tags.csv -o hdfs:///user/maria_dev/cc2/output/q2_tags_par_utilisateur
```

Récupération du fichier de résultats :
```bash
hdfs dfs -getmerge /user/maria_dev/cc2/output/q2_tags_par_utilisateur ~/q2_tags_par_utilisateur.txt
```
Puis
```bash
head -20 ~/q2_tags_par_utilisateur.txt
```

**Analyse :** Le job a produit **14 592 utilisateurs** distincts. Le fichier de résultats contient le nombre de tags pour chacun des 14 592 utilisateurs.

> **Fichier résultat :** [q2_tags_par_utilisateur.txt](https://github.com/Cordierarn/hadoop-cc2/blob/main/results/q2_tags_par_utilisateur/q2_tags_par_utilisateur.txt)

**Extrait des 20 premiers résultats :**

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

On observe une grande disparité : certains utilisateurs n'ont ajouté qu'un seul tag tandis que d'autres en ont ajouté plus d'une centaine (ex: utilisateur 100046 avec 133 tags).

---

## 3. Partie 2 - Configuration Hadoop avec blocs de 64 Mo

### Q3 - Combien de blocs le fichier occupe-t-il dans HDFS ?

Pour cette question, on compare le nombre de blocs dans deux configurations :
1. **Configuration par défaut** : taille de bloc = 128 Mo
2. **Configuration modifiée** : taille de bloc = 64 Mo

Le fichier a déjà été chargé dans les deux configurations à l'étape 4.

**Étape 10 - Vérification des blocs (config par défaut, 128 Mo) :**

```bash
hdfs fsck /user/maria_dev/cc2/input/tags.csv -files -blocks
```

**Résultat :**
```
/user/maria_dev/cc2/input/tags.csv 38810332 bytes, 1 block(s):  OK
0. BP-243674277-172.17.0.2-1529333510191:blk_1073743320_2502 len=38810332 repl=1

Status: HEALTHY
 Total size:	38810332 B
 Total blocks (validated):	1 (avg. block size 38810332 B)
```

**Étape 11 - Vérification des blocs (config 64 Mo) :**

```bash
hdfs fsck /user/maria_dev/cc2/input_64mb/tags.csv -files -blocks
```

**Résultat :**
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
| Par défaut | 128 Mo | 37 Mo (38 810 332 B) | **1** |
| Modifiée | 64 Mo | 37 Mo (38 810 332 B) | **1** |

**Formule :** `nombre_de_blocs = ceil(taille_fichier / taille_bloc)`
- 128 Mo : `ceil(37 / 128) = 1`
- 64 Mo : `ceil(37 / 64) = 1`

> **Remarque :** Le fichier `tags.csv` fait 37 Mo, ce qui est inférieur aux deux tailles de bloc (64 Mo et 128 Mo). Il occupe donc **1 seul bloc** dans les deux configurations. Pour observer une différence, il faudrait un fichier de plus de 64 Mo.

---

### Q4 - Combien de fois chaque tag a-t-il été utilisé ?

**Script :** [`scripts/comptage_tags.py`](scripts/comptage_tags.py)

```python
from mrjob.job import MRJob

class ComptageTags(MRJob):

    def mapper(self, _, ligne):
        try:
            elements = ligne.split(',')
            if elements[0] == 'userId':
                return  # ignorer l'en-tête
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
- **Mapper** : pour chaque ligne, extrait le tag (colonnes entre `movieId` et `timestamp`). On utilise `','.join(elements[2:-1])` pour gérer les tags contenant des virgules. Le tag est normalisé en minuscules avec `.lower()` pour regrouper les variantes de casse. Émet `(tag, 1)`.
- **Reducer** : somme les occurrences pour chaque tag.

**Étape 12 - Test sur l'échantillon :**

```bash
python ~/scripts/comptage_tags.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/cc2/input/tags_test.csv -o hdfs:///user/maria_dev/cc2/output/q4_sample
```

```bash
hdfs dfs -cat /user/maria_dev/cc2/output/q4_sample/part-*
```

**Étape 13 - Exécution sur le fichier complet (blocs de 64 Mo) :**

```bash
python ~/scripts/comptage_tags.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/cc2/input_64mb/tags.csv -o hdfs:///user/maria_dev/cc2/output/q4_comptage_tags
```

Récupération du fichier de résultats :
```bash
hdfs dfs -getmerge /user/maria_dev/cc2/output/q4_comptage_tags ~/q4_comptage_tags.txt
```
Puis
```bash
head -20 ~/q4_comptage_tags.txt
```

**Analyse :** Le job a identifié **65 414 tags uniques**. Le fichier de résultats contient la fréquence d'utilisation de chaque tag.

> **Fichier résultat :** [q4_comptage_tags.txt](https://github.com/Cordierarn/hadoop-cc2/blob/main/results/q4_comptage_tags/q4_comptage_tags.txt)

**Extrait des 20 premiers résultats :**

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

On observe une très grande variété de tags : beaucoup sont des tags de niche utilisés une seule fois, tandis que les tags generiques comme "sci-fi", "comedy" ou "based on a book" sont utilisés des centaines de fois.

---

### Q5 - Pour chaque film, combien de tags le même utilisateur a-t-il introduits ?

**Script :** [`scripts/tags_par_utilisateur_film.py`](scripts/tags_par_utilisateur_film.py)

```python
from mrjob.job import MRJob

class TagsParUtilisateurFilm(MRJob):

    def mapper(self, _, ligne):
        try:
            elements = ligne.split(',')
            if elements[0] == 'userId':
                return  # ignorer l'en-tête
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
- **Mapper** : pour chaque ligne, on crée une paire clé/valeur `(id_film + TAB + id_utilisateur, 1)`. La clé composite `id_film\tid_utilisateur` permet de regrouper les tags par couple (film, utilisateur).
- **Reducer** : somme les valeurs pour chaque couple `(id_film, id_utilisateur)`, donnant le nombre de tags qu'un même utilisateur a attribués à un même film.

**Étape 14 - Test sur l'échantillon :**

```bash
python ~/scripts/tags_par_utilisateur_film.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/cc2/input/tags_test.csv -o hdfs:///user/maria_dev/cc2/output/q5_sample
```

```bash
hdfs dfs -cat /user/maria_dev/cc2/output/q5_sample/part-*
```

**Étape 15 - Exécution sur le fichier complet (blocs de 64 Mo) :**

```bash
python ~/scripts/tags_par_utilisateur_film.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/cc2/input_64mb/tags.csv -o hdfs:///user/maria_dev/cc2/output/q5_tags_par_utilisateur_film
```

Récupération du fichier de résultats :
```bash
hdfs dfs -getmerge /user/maria_dev/cc2/output/q5_tags_par_utilisateur_film ~/q5_tags_par_utilisateur_film.txt
```
Puis
```bash
head -20 ~/q5_tags_par_utilisateur_film.txt
```

**Analyse :** Le job a produit **305 356 couples (film, utilisateur)** distincts. Le fichier de résultats contient, pour chaque couple, le nombre de tags que cet utilisateur a attribués à ce film.

> **Fichier résultat :** [q5_tags_par_utilisateur_film.txt](https://github.com/Cordierarn/hadoop-cc2/blob/main/results/q5_tags_par_utilisateur_film/q5_tags_par_utilisateur_film.txt)

**Extrait des 20 premiers résultats :**

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

Le format de sortie est `"id_film\tid_utilisateur" nombre_de_tags`. Par exemple, l'utilisateur **6550** a ajouté **7 tags** au film **100017**, tandis que l'utilisateur **14116** a ajouté **9 tags** au film **100034**. On observe que certains utilisateurs sont très actifs sur certains films.

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

### 1.1 Telechargement et extraction du dataset

```bash
# Telechargement du dataset MovieLens 25M
wget https://files.grouplens.org/datasets/movielens/ml-25m.zip

# Extraction
unzip ml-25m.zip
```

**Resultat (dans mon cas, les fichiers étaient déjà présents):**
```
Archive:  ml-25m.zip                                                                                                                                                                                               
replace ml-25m/tags.csv? [y]es, [n]o, [A]ll, [N]one, [r]ename: y                                                                                                                                                   
  inflating: ml-25m/tags.csv                                                                                                                                                                                       
replace ml-25m/links.csv? [y]es, [n]o, [A]ll, [N]one, [r]ename: a                                                                                                                                                  
error:  invalid response [a]                                                                                                                                                                                       
replace ml-25m/links.csv? [y]es, [n]o, [A]ll, [N]one, [r]ename: y                                                                                                                                                  
  inflating: ml-25m/links.csv                                                                                                                                                                                      
replace ml-25m/README.txt? [y]es, [n]o, [A]ll, [N]one, [r]ename: y                                                                                                                                                 
  inflating: ml-25m/README.txt                                                                                                                                                                                     
replace ml-25m/ratings.csv? [y]es, [n]o, [A]ll, [N]one, [r]ename: y                                                                                                                                                
  inflating: ml-25m/ratings.csv                                                                                                                                                                                    
replace ml-25m/genome-tags.csv? [y]es, [n]o, [A]ll, [N]one, [r]ename: y                                                                                                                                            
  inflating: ml-25m/genome-tags.csv                                                                                                                                                                                
replace ml-25m/genome-scores.csv? [y]es, [n]o, [A]ll, [N]one, [r]ename: y                                                                                                                                          
  inflating: ml-25m/genome-scores.csv                                                                                                                                                                              
replace ml-25m/movies.csv? [y]es, [n]o, [A]ll, [N]one, [r]ename: y                                                                                                                                                 
  inflating: ml-25m/movies.csv                
```

### 1.2 Exploration du fichier tags.csv

```bash
# Apercu des premieres lignes
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
# Taille du fichier
ls -lh ml-25m/tags.csv
```

**Resultat :**
```
-rw-rw-r-- 1 maria_dev maria_dev 38M Nov 21  2019 ml-25m/tags.csv
```

```bash
# Nombre de lignes
wc -l ml-25m/tags.csv
```

**Resultat :**
```
1093361 ml-25m/tags.csv
```

Le fichier contient **1 093 360 tags**.

### 1.3 Creation d'un fichier d'echantillon pour les tests

Avant de lancer les jobs sur le fichier complet, on cree un petit echantillon pour valider nos scripts :

```bash
head -1 ml-25m/tags.csv > tags_test.csv
head -20 ml-25m/tags.csv >> tags_test.csv
```

### 1.4 Chargement des fichiers dans HDFS (configuration par defaut)

```bash
# Creation du repertoire sur HDFS
hdfs dfs -mkdir -p /user/maria_dev/cc2/input

# Chargement du fichier tags.csv (config par defaut : blocs de 128 Mo)
hdfs dfs -put ml-25m/tags.csv /user/maria_dev/cc2/input/tags.csv

# Chargement du fichier d'echantillon
hdfs dfs -put tags_test.csv /user/maria_dev/cc2/input/tags_test.csv

hdfs dfs -ls /user/maria_dev/cc2/input/
```

### 1.5 Transfert des scripts Python vers la sandbox

Les scripts MRJob utilises sont disponibles dans le repertoire [`scripts/`](scripts/) du repo GitHub :
- [`tags_par_film.py`](scripts/tags_par_film.py) - Q1
- [`tags_par_utilisateur.py`](scripts/tags_par_utilisateur.py) - Q2
- [`comptage_tags.py`](scripts/comptage_tags.py) - Q4
- [`tags_par_utilisateur_film.py`](scripts/tags_par_utilisateur_film.py) - Q5

```bash
# Sur la sandbox, creer le repertoire scripts et y placer les fichiers
hdfs dfs -mkdir -p scripts 
```

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
- **Mapper** : pour chaque ligne, on crée une paire clé,valeur `(movieId, 1)`. On ignore la ligne d'en-tete et on encapsule dans un `try/except` pour gerer les lignes malformées.
- **Reducer** : somme toutes les valeurs pour chaque `movieId`, donnant le nombre total de tags par film.

**Test sur l'echantillon :**

```bash
python ~/scripts/tags_par_films.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/cc2/input/tags_test.csv \
  -o hdfs:///user/maria_dev/cc2/output/q1_sample
```

```bash
hdfs dfs -cat /user/maria_dev/cc2/output/q1_sample/part-*
```

**Resultat attendu sur l'echantillon :**
```
"100"   4
"200"   4
"300"   3
"400"   3
```

Verification manuelle : le film 100 a 4 tags (sci-fi x2, action, classic), le film 200 a 4 tags (comedy x2, funny, feel-good) -- c'est correct.

**Execution sur le fichier complet :**

```bash
python ~/scripts/tags_par_film.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/cc2/input/tags.csv \
  -o hdfs:///user/maria_dev/cc2/output/q1_tags_par_film
```

**Recuperation des resultats :**

```bash
hdfs dfs -getmerge /user/maria_dev/cc2/output/q1_tags_par_film q1_tags_par_film.txt
```

**Analyse :** Le job a traite **1 093 360 enregistrements** (Map input records) et produit **45 251 films** distincts (Reduce output records). Le fichier de resultats contient le nombre de tags pour chacun des 45 251 films presents dans le dataset. Ce fichier etant volumineux, il est disponible sur le repo GitHub.

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

**Test sur l'echantillon :**

```bash
python ~/scripts/tags_par_utilisateur.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/cc2/input/tags_test.csv \
  -o hdfs:///user/maria_dev/cc2/output/q2_sample
```

```bash
hdfs dfs -cat /user/maria_dev/cc2/output/q2_sample/part-*
```

**Resultat attendu sur l'echantillon :**
```
"1"     3
"2"     3
"3"     4
"4"     2
"5"     4
```

Verification : l'utilisateur 1 a 3 tags, l'utilisateur 3 en a 4 -- c'est correct.

**Execution sur le fichier complet :**

```bash
python ~/scripts/tags_par_utilisateur.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/cc2/input/tags.csv \
  -o hdfs:///user/maria_dev/cc2/output/q2_tags_par_utilisateur
```

**Recuperation des resultats :**

```bash
hdfs dfs -getmerge /user/maria_dev/cc2/output/q2_tags_par_utilisateur q2_tags_par_utilisateur.txt
```

**Analyse :** Le job a produit **14 592 utilisateurs** distincts (Reduce output records). Le fichier de resultats contient le nombre de tags pour chacun des 14 592 utilisateurs. Ce fichier etant volumineux, il est disponible sur le repo GitHub.

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

#### Configuration par defaut (128 Mo)

Le fichier `tags.csv` a deja ete charge dans HDFS avec la configuration par defaut.

```bash
hdfs fsck /user/maria_dev/cc2/input/tags.csv -files -blocks
```

**Resultat :**
```
/user/maria_dev/cc2/input/tags.csv 38810332 bytes, 1 block(s):  OK
0. BP-243674277-172.17.0.2-1529333510191:blk_1073743320_2502 len=38810332 repl=1

Status: HEALTHY
 Total size:	38810332 B
 Total dirs:	0
 Total files:	1
 Total blocks (validated):	1 (avg. block size 38810332 B)
```

**Analyse :** Le fichier `tags.csv` fait **37 Mo** (38 810 332 octets). Avec une taille de bloc par defaut de **128 Mo**, le fichier tient entierement dans **1 seul bloc** car 37 Mo < 128 Mo.

#### Configuration avec blocs de 64 Mo

On charge le fichier avec une taille de bloc de 64 Mo :

```bash
# Charger le fichier avec des blocs de 64 Mo
hdfs dfs -mkdir -p /user/maria_dev/cc2/input_64mb
hdfs dfs -D dfs.blocksize=67108864 -put ml-25m/tags.csv /user/maria_dev/cc2/input_64mb/tags.csv
```

```bash
hdfs fsck /user/maria_dev/cc2/input_64mb/tags.csv -files -blocks
```

**Resultat :**
```
/user/maria_dev/cc2/input_64mb/tags.csv 38810332 bytes, 1 block(s):  OK
0. BP-243674277-172.17.0.2-1529333510191:blk_1073743321_2503 len=38810332 repl=1

Status: HEALTHY
 Total size:	38810332 B
 Total dirs:	0
 Total files:	1
 Total blocks (validated):	1 (avg. block size 38810332 B)
```

**Analyse :** Avec une taille de bloc de **64 Mo**, le fichier de 37 Mo tient egalement dans **1 seul bloc** car 37 Mo < 64 Mo. Le fichier n'est pas assez volumineux pour etre decoupe en plusieurs blocs, meme avec des blocs de 64 Mo.

#### Tableau recapitulatif

| Configuration | Taille de bloc | Taille du fichier | Nombre de blocs |
|---|---|---|---|
| Par defaut | 128 Mo | 37 Mo (38 810 332 B) | **1** |
| Modifiee | 64 Mo | 37 Mo (38 810 332 B) | **1** |

**Formule :** `nombre_de_blocs = ceil(taille_fichier / taille_bloc)`
- 128 Mo : `ceil(37 / 128) = 1`
- 64 Mo : `ceil(37 / 64) = 1`

> **Remarque :** Pour observer une difference de nombre de blocs entre les deux configurations, il faudrait un fichier de plus de 64 Mo. Ici le fichier `tags.csv` (37 Mo) est inferieur aux deux tailles de bloc, donc il occupe un seul bloc dans les deux cas.

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

> **Note :** Cette question doit etre executee avec le fichier charge avec des blocs de 64 Mo.

**Test sur l'echantillon :**

```bash
python ~/scripts/comptage_tags.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/cc2/input/tags_test.csv \
  -o hdfs:///user/maria_dev/cc2/output/q4_sample
```

```bash
hdfs dfs -cat /user/maria_dev/cc2/output/q4_sample/part-*
```

**Resultat attendu sur l'echantillon :**
```
"action"        1
"classic"       1
"comedy"        3
"drama"         2
"feel-good"     1
"funny"         1
"horror"        2
"intense"       1
"scary"         1
"sci-fi"        3
```

Verification : "sci-fi" apparait 3 fois (user 1 film 100, user 2 film 100, user 4 film 100) -- c'est correct.

**Execution sur le fichier complet (blocs de 64 Mo) :**

```bash
python ~/scripts/comptage_tags.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/cc2/input_64mb/tags.csv \
  -o hdfs:///user/maria_dev/cc2/output/q4_comptage_tags
```

**Recuperation des resultats :**

```bash
hdfs dfs -getmerge /user/maria_dev/cc2/output/q4_comptage_tags q4_comptage_tags.txt
```

**Analyse :** Le job a identifie **65 414 tags uniques** (Reduce output records). Le fichier de resultats contient la frequence d'utilisation de chaque tag. Ce fichier etant volumineux, il est disponible sur le repo GitHub.

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

> **Note :** Cette question doit etre executee avec le fichier charge avec des blocs de 64 Mo.

**Test sur l'echantillon :**

```bash
python ~/scripts/tags_par_utilisateur_film.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/cc2/input/tags_test.csv \
  -o hdfs:///user/maria_dev/cc2/output/q5_sample
```

```bash
hdfs dfs -cat /user/maria_dev/cc2/output/q5_sample/part-*
```

**Resultat attendu sur l'echantillon :**
```
"100\t1"        2
"100\t2"        2
"100\t4"        1
"200\t3"        2
"200\t5"        2
"300\t2"        1
"300\t3"        2
"400\t4"        1
"400\t5"        2
```

Verification : l'utilisateur 1 a mis 2 tags sur le film 100 (sci-fi, action), l'utilisateur 2 a mis 2 tags sur le film 100 (sci-fi, classic) -- c'est correct.

**Execution sur le fichier complet (blocs de 64 Mo) :**

```bash
python ~/scripts/tags_par_utilisateur_film.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/cc2/input_64mb/tags.csv \
  -o hdfs:///user/maria_dev/cc2/output/q5_tags_par_utilisateur_film
```

**Recuperation des resultats :**

```bash
hdfs dfs -getmerge /user/maria_dev/cc2/output/q5_tags_par_utilisateur_film q5_tags_par_utilisateur_film.txt
```

**Analyse :** Le job a produit **305 356 couples (film, utilisateur)** distincts (Reduce output records). Le fichier de resultats contient, pour chaque couple, le nombre de tags que cet utilisateur a attribues a ce film. Ce fichier etant volumineux, il est disponible sur le repo GitHub.

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

Le format de sortie est `"movieId\tuserId" nombre_de_tags`. Par exemple, l'utilisateur **6550** a ajoute **7 tags** au film **100017**, tandis que l'utilisateur **14116** a ajoute **9 tags** au film **100034**. On observe que certains utilisateurs sont tres actifs sur certains films.

---

## Recapitulatif des commandes

### Preparation

```bash
# Telechargement et extraction
wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
unzip ml-25m.zip

# Chargement dans HDFS (config par defaut, blocs de 128 Mo)
hdfs dfs -mkdir -p /user/maria_dev/cc2/input
hdfs dfs -put ml-25m/tags.csv /user/maria_dev/cc2/input/tags.csv

# Chargement dans HDFS (blocs de 64 Mo)
hdfs dfs -D dfs.blocksize=67108864 -put ml-25m/tags.csv /user/maria_dev/cc2/input_64mb/tags.csv
```

### Execution des jobs MapReduce

```bash
# Q1 - Tags par film (config par defaut)
python ~/scripts/tags_par_film.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/cc2/input/tags.csv \
  -o hdfs:///user/maria_dev/cc2/output/q1_tags_par_film

# Q2 - Tags par utilisateur (config par defaut)
python ~/scripts/tags_par_utilisateur.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/cc2/input/tags.csv \
  -o hdfs:///user/maria_dev/cc2/output/q2_tags_par_utilisateur

# Q3 - Nombre de blocs
hdfs fsck /user/maria_dev/cc2/input/tags.csv -files -blocks
hdfs fsck /user/maria_dev/cc2/input_64mb/tags.csv -files -blocks

# Q4 - Frequence d'utilisation des tags (blocs de 64 Mo)
python ~/scripts/comptage_tags.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/cc2/input_64mb/tags.csv \
  -o hdfs:///user/maria_dev/cc2/output/q4_comptage_tags

# Q5 - Tags par utilisateur par film (blocs de 64 Mo)
python ~/scripts/tags_par_utilisateur_film.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/cc2/input_64mb/tags.csv \
  -o hdfs:///user/maria_dev/cc2/output/q5_tags_par_utilisateur_film
```

### Recuperation des resultats

```bash
# Recuperer les fichiers de resultats depuis HDFS
hdfs dfs -getmerge /user/maria_dev/cc2/output/q1_tags_par_film q1_tags_par_film.txt
hdfs dfs -getmerge /user/maria_dev/cc2/output/q2_tags_par_utilisateur q2_tags_par_utilisateur.txt
hdfs dfs -getmerge /user/maria_dev/cc2/output/q4_comptage_tags q4_comptage_tags.txt
hdfs dfs -getmerge /user/maria_dev/cc2/output/q5_tags_par_utilisateur_film q5_tags_par_utilisateur_film.txt
```

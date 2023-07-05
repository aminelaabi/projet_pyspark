# FlightRadar24

# Sujet

Créer un pipeline ETL (Extract, Transform, Load) permettant de traiter les données de l'API [flightradar24](https://www.flightradar24.com/), qui répertorie l'ensemble des vols aériens, aéroports, compagnies aériennes mondiales.

> En python, cette librairie: https://github.com/JeanExtreme002/FlightRadarAPI facilite l'utilisation de l'API.

## Environnement de dev:  

Pour le développement, j'ai utilisé un container docker avec l'image juptyer/pyspark-notebook:latest, qui contient un serveur jupyter avec un interpréteur python et spark, et qui permet de développer en local sans avoir à installer spark sur la machine. En effet tous ce qu'il me fallait faire c'est de connecter mon notebook jupyter ouvert sur mon VSCode au serveur jupyter du container docker, pour pouvoir lancer mes requêtes spark.    

## Données:

J'ai utilisé 4 sources différentes pour récupérer les données:
1. Les vols en cours avec l'API Flightradar24
2. Les compagnies aériennes, et les avions depuis https://openflights.org/
3. Les aéroports depuis https://ourairports.com/data/

Le choix de diversifier les sources est volontaire, afin de simuler un environnement de production où les données sont récupérées de plusieurs sources différentes, et aussi pour rendre le kata plus challengeant.


## Résultats

Ce pipeline doit permettre de fournir les indicateurs suivants:
1. La compagnie avec le + de vols en cours (Fait)
2. Pour chaque continent, la compagnie avec le + de vols régionaux actifs (Fait)
3. Le vol en cours avec le trajet le plus long (Fait)
4. Pour chaque continent, la longueur de vol moyenne (Fait)
5. L'entreprise constructeur d'avions avec le plus de vols actifs (Fait avec une variation au lieu de donner le nom de l'entreprise, j'ai donné le nom de l'avion, car la seul données fiable que j'ai trouvé était [payante](https://applications.icao.int/dataservices/default.aspx))
6. Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage (Fait)
7. Questions Bonus: Quel aéroport a la plus grande différence entre le nombre de vol sortant et le nombre de vols entrants ? (Fait)


## Script PySpark
### Prérequis
Pour lancer le script, il faudra avoir plusieurs dépandances, mais cela peut être résole en packageant le script avec l'interpréteur python et les dépendances pour ensuite l'envoyer sur un cluster spark.
Aussi il faudra avoir au prealable, les 3 fichiers airports.csv, airlines.dat et planes.dat dans un endroit accessible par le cluster spark (ex: HDFS, S3, ...)
### Lancement

Le script est organisé de la manière suivante:
1. Definir les fonctions de transformation, qui permettent de transformer les données en un grand dataframe spark pour limiter le nombre de shuffle entre executors?
2. Effectuer les transformations sans utiliser d'actions spark (count, collect, write, ...)
3. Effectuer les actions spark


## Industrialisation



Pour l'industrialisation je propose d'utiliser un scheduler comme [Airflow](https://airflow.apache.org/), qui permet de planifier des jobs, et de les monitorer.  
Le dag airflow ressemblerait avec le code actuel à cela:  
1. Un LivyOperator qui permet d'éxécuter un script python sur un cluster spark.
Dans l'idéal, il faudra que chaque tâche soit dans son propre processus, afin de pouvoir les monitorer indépendamment, et en cas de problème, pouvoir relancer uniquement la tâche en erreur.  
Dans ce cas de figure je propose cette architecture:  
1. Un PythonOperator ou task (selon la version d'Airflow) qui permet de lancer un script python qui load les données depuis les differentes sources, et les mets sur S3 avec un S3Hook, native à Airflow. (Cette solution est proposée car la volumétrie des données est faible, et que le temps de traitement est faible, donc il n'est pas nécessaire de passer par un cluster spark pour faire le traitement)
2. Un LivyOperator qui permet d'éxécuter un script python sur un cluster spark, qui va lire les données depuis S3, et les traiter, et les écrire sur une base de données Postgres de staging.(Le choix de la base de données Postgres est volontaire, la première raison est que pour visualiser les données avec des outils comme Tableau, ou PowerBI, il est possible de se connecter directement à la base de données, et de créer des dashboards, deuxièment les schémas qu'on définira pour chaque table feront office d'un dernier firewall, en effet avec un schéma static, les applications consommatrices de données savent à l'avance la structure des données, et donc au moment de l'écriture depuis spark et en cas d'erreur la base refusera d'écrire des données incorrectes)
3. Un BranchPythonOperator ou task qui permet de vérifier que les données ont bien été écrites dans la base de données et qu'il sont fonctionnelement correctes, et de les écrire dans la base de données de production, et en cas d'erreur, alerté le responsable du pipeline.

La base de données contiendra une table pour chaque indicateur, avec chaqu'un une colonne date_heure qui contiendra la date et l'heure du calcul pour permettre aux data analyst de retrouver les données pour un couple (Date, Heure) donné.


# Pour executer le notebook jupyter
1. Puller l'image docker: docker pull jupyter/pyspark-notebook:latest
2. Lancer le container
3. Ouvrir le notebook jupyter
4. Executer les cellules
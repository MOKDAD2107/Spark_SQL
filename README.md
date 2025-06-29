<h1>TP Spark SQL — Traitement des incidents d’une entreprise industrielle</h1>
Ce projet est une démonstration du traitement de données avec Apache Spark SQL en Java, appliqué à un fichier d’incidents liés aux différents services d’une entreprise industrielle.

L’objectif est d’analyser ces incidents à l’aide de requêtes SQL exécutées sur un moteur distribué, en utilisant Spark pour :
- Charger et transformer des données structurées (CSV)
- Interroger les données via SQL 
- Extraire des statistiques utiles par service et par année

L’application est développée et testée localement en Java, puis est déployable sur un cluster Spark via Docker pour tirer profit du traitement parallèle.
<h3>Sujet</h3>
Exercice : Traitement des incidents
Développer une application Spark permettant, à partir d’un fichier CSV incidents.csv contenant les incidents d’une entreprise, de :

1. Afficher le nombre d’incidents par service.
2. Afficher les deux années où il y a eu le plus d’incidents.
<h3>Resultat attendue</h3>

<img src="capture_ecran/capture1.png">
<img src="capture_ecran/capture2.png">

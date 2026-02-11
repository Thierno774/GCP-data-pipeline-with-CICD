# 📌 Description

Ce projet illustre une pipeline de données en temps réel sur Google Cloud Platform (GCP).

Les fichiers CSV sont importés depuis un poste local vers Google Cloud Storage (GCS), déclenchant une Cloud Function qui charge et fusionne les données dans une table BigQuery Customer, avec un CI/CD automatique pour le code via Cloud Build.

# La pipeline gère :

* L’ingestion automatique de nouveaux fichiers CSV

* La validation et l’enrichissement des données

* Les mises à jour incrémentales dans BigQuery via MERGE

* La détection de changements (mise à jour uniquement si les données ont changé)

* Le déploiement automatisé de la Cloud Function via CI/CD
# 🏗 Architecture

Flux des données :

![Sparkify Data Model](/images_pipelines.png)    

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

# ⚡ Déploiement

Pousser vos modifications sur GitHub → déclenche Cloud Build → déploie automatiquement la Cloud Function.

Déposer un CSV dans le bucket GCS dans le dossier gcp_bq/ → déclenche la Cloud Function.

La Cloud Function valide et charge les données dans BigQuery.

# 🧪 Fonctionnalités clés

Event-driven : Déclenché par l’upload d’un fichier GCS

Temps réel : Les données sont fusionnées de manière incrémentale dans BigQuery

CI/CD intégré : Déploiement automatique depuis GitHub

Journalisation complète : Logs détaillés pour debug et monitoring

Scalable : Peut gérer de gros volumes de CSV

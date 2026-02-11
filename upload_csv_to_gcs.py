import os
import time
from google.cloud import storage
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

# === CONFIGURATION ===
WATCH_FOLDER = "gcp-de-services" # Le dossier local 
BUCKET_NAME = "gcp-de-services"
DESTINATION_FOLDER_IN_BUCKET = "gcp_bq"  # "" si racine

class CSVHandler(FileSystemEventHandler):
    def __init__(self, gcs_client):
        self.client = gcs_client
        self.bucket = self.client.bucket(BUCKET_NAME)

    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith(".csv"):
            return

        time.sleep(2)  # attendre la fin de l’écriture

        file_path = event.src_path
        file_name = os.path.basename(file_path)

        destination_blob_name = (
            f"{DESTINATION_FOLDER_IN_BUCKET}/{file_name}"
            if DESTINATION_FOLDER_IN_BUCKET
            else file_name
        )

        print(f"Nouveau fichier détecté : {file_path}")
        self.upload_to_gcs(file_path, destination_blob_name)

    def upload_to_gcs(self, source_file, destination_blob_name):
        try:
            blob = self.bucket.blob(destination_blob_name)
            blob.upload_from_filename(source_file)
            print(f"Upload OK : gs://{BUCKET_NAME}/{destination_blob_name}")
        except Exception as e:
            print(f"Échec upload {source_file} : {e}")

def upload_existing_files(client):
    print("Recherche des fichiers CSV existants...")
    bucket = client.bucket(BUCKET_NAME)

    for file_name in os.listdir(WATCH_FOLDER):
        if not file_name.endswith(".csv"):
            continue

        file_path = os.path.join(WATCH_FOLDER, file_name)
        destination_blob_name = (
            f"{DESTINATION_FOLDER_IN_BUCKET}/{file_name}"
            if DESTINATION_FOLDER_IN_BUCKET
            else file_name
        )

        try:
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(file_path)
            print(f"Upload existant : gs://{BUCKET_NAME}/{destination_blob_name}")
        except Exception as e:
            print(f"Échec upload {file_path} : {e}")

def main():
    # Auth via Application Default Credentials (PAS DE CLÉ)
    client = storage.Client()

    # Upload des fichiers déjà présents
    upload_existing_files(client)
    print(f"Le fichier a été directement chargé dans le gs://{BUCKET_NAME}/{DESTINATION_FOLDER_IN_BUCKET}")
# Watcher
    event_handler = CSVHandler(client)
    
if __name__ == "__main__":
    main()

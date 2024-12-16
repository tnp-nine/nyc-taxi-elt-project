import kaggle
import os

def download_kaggle_dataset(download_path):
    print("Downloading dataset...")
    kaggle.api.dataset_download_files('arnavsmayan/netflix-userbase-dataset', path=download_path, unzip=True)
    print("Complete download dataset.")
    downloaded_file = os.listdir(download_path)
    print(downloaded_file)

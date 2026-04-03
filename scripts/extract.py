import os
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile

def run_extraction(dataset_slug, download_path) :
    """
    Fungsi untuk mendownload dataset dari kaggle dan mengekstraknya.
    """
    
    try :
        api = KaggleApi()
        api.authenticate()
        
        print(f"[*] memulai mendownload dataset: {dataset_slug}.....")
        api.dataset_download_files(dataset_slug, path=download_path, unzip = False)
        
        for file in os.listdir(download_path):
            if file.endswith(".zip"):
                zip_path = os.path.join(download_path,file)
                with zipfile.ZipFile(zip_path,'r') as zip_ref:
                    zip_ref.extractall(download_path)
                    
                    os.remove(zip_path)
                    print(f"Berhasil extract file: {file}")
        
        print(f"Data mentah tersimpan di : {download_path}")
    except Exception as e:
        print(f"Error saat ekstraksi: {e}")


if __name__ == "__main__":
    RAW_PATH = "/media/farhan/DATA PRIBADI/Project/pipeline/data/raw"
    DATASET = "russellyates88/suicide-rates-overview-1985-to-2016"
    run_extraction(DATASET,RAW_PATH)
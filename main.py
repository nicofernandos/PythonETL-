from scripts.extract import run_extraction
from scripts.transform import run_transform

def main():
    print ("Memulai menjalankan pipeline")
    run_extraction
    run_transform
    print("Pipeline sudah dijalankan")
    
if __name__ == "main":
    main()
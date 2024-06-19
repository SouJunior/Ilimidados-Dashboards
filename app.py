from etl.etl_linkedin import EtlLinkedin
from datetime import datetime
import os

if __name__ == "__main__":
    start_time = datetime.now()
    etl = EtlLinkedin()
    extraction_folders = os.listdir(etl.path_etl)
    etl.mass_etl(extraction_folders)
    total_time = datetime.now() - start_time
    print("Total time:", total_time)
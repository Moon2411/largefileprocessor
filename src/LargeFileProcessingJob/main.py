""" This the main file for starting the job """

from jobs.ingestion_job import IngestionJob

def main():
    data_path = r'C:\Users\ub02459\OneDrive - The Dow Chemical Company\Desktop\LargeFileProcessor\data\products.csv'
    mode = 'full_load'

    job = IngestionJob()
    job.incremental_load(data_path)


if __name__ == '__main__':
    main()

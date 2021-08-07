""" This the main file for starting the job """

import sys, getopt
from jobs.ingestion_job import IngestionJob

def main(argv):
    try:
        opts, args = getopt.getopt(argv,"path:mode")
        for opt, arg in opts:
            if opt == '-mode':
                mode = arg
            
        job = IngestionJob()
        data_path = "data/products.csv"

        if mode == 'full_load':
            job.full_load(data_path)
        elif mode == 'incremental_load':
            job.incremental_load(data_path)
        else:
            print("Unkown mode")
    except:
        print("Could not complete the job")

if __name__ == '__main__':
    main(sys.argv[1:])

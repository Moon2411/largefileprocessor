python src\setup.py

python src\main.py -mode=full_load
python src\main.py -mode=incremental_load

python src\LargeFileProcessingJob\jobs\aggregate_job.py
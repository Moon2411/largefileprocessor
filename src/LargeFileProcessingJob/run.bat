python src\LargeFileProcessingJob\setup.py

python src\LargeFileProcessingJob\main.py -m full_load
python src\LargeFileProcessingJob\main.py -m incremental_load

python src\LargeFileProcessingJob\jobs\aggregate_job.py
# Marketing SFMC Raw Layer Pipeline

Data loading pipeline for SFMC data. It processes FileTransfer export CSVs.
Filtering based on latest load timestamp provides incremental load.
Result is landing in the defined BigQuery tables.

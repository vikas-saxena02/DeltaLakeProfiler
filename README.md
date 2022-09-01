# DeltaLakeProfiler
This repository contains code to run some basic checks on delta lake to identify common problems related to partitioning and Zordering

The repo contains three notebooks:
  1. Profiler: This is the main notebook which needs to be executed to run the profiler. This notebook takes ```databaseName``` as input through widget.
  2. Functions: This notebook contains useful functions that are referenced in the Profiler Notebook.
  3. DataGenerator: This notebbok is used to simulate various test tables required to test the Profiler's functionality
  
  
# List of planned improvements:
  1. Modify Profiler to run checks on each table in parallel rather than sequentially
  2. Add additional check to check for High Cardinality of ZORDER Columns
  3. Add following checks for partitioning: 
    
      1. partition column does not have null values
      2. partition column does not have blank or empty values
      3. Paritition column has low cardinality
      4. Partitition column provides a uniform data distribution

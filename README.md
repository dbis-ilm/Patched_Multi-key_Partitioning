# Patched_Multi-key_Partitioning

Repository containing supplementary material for the paper "Patched Multi-key Partitioning for Robust Query Performance". It provides partitioners that assign tuples of relational tables to partitions based on multiple partition keys without data replication, but containing exceptions to the partitioning. 
Also contains an example based on the CommonGovernment table of the PublicBI benchmark.

## Requirements

- Distributed Neighbour expansion ([Link](http://www.masahanai.jp/DistributedNE/)) must be installed and the path to the binary must be specified.
- PublicBI benchmark is used for the example: ([Link](https://github.com/cwida/public_bi_benchmark)). For GraphPartitioning, the CommonGovernment table must be pre-loaded, for the IterativePartitioner the path to the flat file must be provided. 
- The SQL queries performed by the scripts are based on the SQL dialect for the Actian Vector/Avalanche environment.

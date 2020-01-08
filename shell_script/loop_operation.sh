#!/bin/bash
  
for i in `seq 1 6`
do
    nohup python -u ./model_tsg_gene_computing_iterative.py > running_output_$i &
    sleep 1
done
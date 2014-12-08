#!/bin/bash
mpiexec.hydra -bootstrap slurm -n 10 ./work_handler.py $1

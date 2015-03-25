#!/bin/bash
#SBATCH --time=7-0:0
module add openmind/anaconda/2.1.0 #provides access to python2.7, mpi and hydra
module add mit/matlab/2014a #MATLAB
echo "Number of cpu's is" $((SLURM_CPUS_PER_TASK * SLURM_JOB_NUM_NODES))
mpiexec.hydra -bootstrap slurm -n $((SLURM_CPUS_PER_TASK * SLURM_JOB_NUM_NODES)) ./work_handler.py $1 $2

#example invocation: $sbatch --cpus-per-task=12 -N2 dispatch.sh testing/openmindAnnealingShortTest.txt 
#uses 24 cpus (12 cpus * 2 Nodes) to accomplish the work-items in openmindAnnealindShortTest.txt


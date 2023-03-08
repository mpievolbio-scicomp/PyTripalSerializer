#! /bin/bash -l


#SBATCH --ntasks=200
#SBATCH --cpus-per-task=1
#SBATCH --nodes=10
#SBATCH --time=01:00:00
#SBATCH --partition=global
#SBATCH -o dask.out

source ~/miniconda3_deepbio/bin/activate tripser

mpirun -np 200 python parse_mpi.py


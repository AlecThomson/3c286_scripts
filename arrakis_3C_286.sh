#!/bin/bash -l
#SBATCH --job-name=3c286
#SBATCH --export=NONE
#SBATCH --ntasks-per-node=1
#SBATCH --ntasks=1
#SBATCH --mem=48GB
#SBATCH --time=1-00:00:00
#SBATCH -A OD-217087
#SBATCH -o test_pipe.log
#SBATCH -e test_pipe.log
#SBATCH --qos=express

# I trust nothing
export OMP_NUM_THREADS=1

export APIURL=http://stokes.it.csiro.au:4200/api
export PREFECT_API_URL="${APIURL}"
export WORKDIR=$(pwd)
export PREFECT_HOME="${WORKDIR}/prefect"
export PREFECT_LOGGING_EXTRA_LOGGERS="arrakis"

echo "Sourcing home"
source /home/$(whoami)/.bashrc
module load singularity

echo "Activating conda arrakis environment"
conda activate arrakis310

echo "About to run 3C286"
python arrakis_3C_286.py --do-imager
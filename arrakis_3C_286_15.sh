#!/bin/bash -l
#SBATCH --job-name=3c286
#SBATCH --export=NONE
#SBATCH --ntasks-per-node=1
#SBATCH --ntasks=1
#SBATCH --mem=48GB
#SBATCH --time=1-00:00:00
#SBATCH -A OD-217087
#SBATCH -o /scratch3/projects/spiceracs/askap_pol_testing/52087_cal/logs/3c286_%j.log
#SBATCH -e /scratch3/projects/spiceracs/askap_pol_testing/52087_cal/logs/3c286_%j.log
#SBATCH --qos=express

# I trust nothing
export OMP_NUM_THREADS=1

export APIURL=http://stokes.it.csiro.au:4200/api
export PREFECT_API_URL="${APIURL}"
export WORKDIR=$(pwd)
export PREFECT_HOME="${WORKDIR}/prefect"
export PREFECT_LOGGING_EXTRA_LOGGERS="arrakis"

cd /scratch3/projects/spiceracs/askap_pol_testing/cubes

echo "Sourcing home"
source /home/$(whoami)/.bashrc
module load singularity

echo "Activating conda arrakis environment"
conda activate arrakis310

echo "About to run 3C286"
python /scratch3/projects/spiceracs/askap_pol_testing/3c286_scripts/arrakis_3C_286_15.py --do-cutout
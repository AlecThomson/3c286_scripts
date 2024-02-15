#!/usr/bin/env bash
#SBATCH --job-name=linmos
#SBATCH --export=NONE
#SBATCH --ntasks=36
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=24GB
#SBATCH --time=0-01:00:00
#SBATCH -A OD-217087
#SBATCH -o logs/linmos_%j.log
#SBATCH -e logs/linmos_%j.log
#SBATCH --qos=express
##SBATCH --array=0-35

module load singularity
module load rclone
module load openmpi

beam=0 #$SLURM_ARRAY_TASK_ID
workdir=/scratch3/projects/spiceracs/3C286_flint_main/51997
base=SB51997.3C286_45deg.beam$beam

rclone copy -P --transfers $SLURM_NTASKS --checkers $SLURM_NTASKS --include="$base.*cube.fits" ./ $MEMDIR/

for stokes in i q u; do
mv $MEMDIR/$base.${stokes}cube.fits $MEMDIR/$base.${stokes}.cube.fits
    ./fix_header.py $MEMDIR/$base.${stokes}.cube.fits
    cat << EOF > $MEMDIR/$base.linmos.$stokes.parset
linmos.names            = $MEMDIR/$base.${stokes}.cube.fixed
linmos.imagetype        = fits
linmos.outname          = $MEMDIR/$base.${stokes}.cube.pbcor
linmos.outweight        = $MEMDIR/$base.${stokes}.cube.pbcor.weight
# For ASKAPsoft>1.3.0
linmos.useweightslog    = true
linmos.weighttype       = FromPrimaryBeamModel
linmos.weightstate      = Inherent
linmos.primarybeam      = ASKAP_PB
linmos.primarybeam.ASKAP_PB.image = /scratch3/projects/spiceracs/akpb.iquv.closepack36.54.943MHz.SB51811.cube.fits
linmos.removeleakage    = true
EOF
    mpirun --mca btl_openib_allow_ib 1 singularity exec /datasets/work/sa-mhongoose/work/containers/askapsoft_1.15.0-openmpi4.sif linmos-mpi -c $MEMDIR/$base.linmos.$stokes.parset
done

rclone copy -P --transfers $SLURM_CPUS_PER_TASK --checkers $SLURM_CPUS_PER_TASK --include="$base.*cube.pbcor.fits" $MEMDIR/ ./
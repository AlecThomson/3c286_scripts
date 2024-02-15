#!/usr/bin/env bash
#SBATCH --job-name=wsclean
#SBATCH --export=NONE
#SBATCH --ntasks=8
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=512GB
#SBATCH --time=0-02:00:00
#SBATCH -A OD-217087
#SBATCH -o logs/wsclean_%j.log
#SBATCH -e logs/wsclean_%j.log
#SBATCH --qos=express
#SBATCH --array=0-35

module load singularity
module load rclone
module load openmpi

beam=$SLURM_ARRAY_TASK_ID
workdir=/scratch3/projects/spiceracs/3C286_flint_main/51997
base=SB51997.3C286_45deg.beam$beam
rclone copy -P --transfers $SLURM_NTASKS --checkers $SLURM_NTASKS $workdir/$base.ms $MEMDIR/$base.ms

mpirun --mca btl_openib_allow_ib 1 singularity exec  /scratch3/projects/spiceracs/singularity_images/wsclean_force_mask.sif wsclean-mp \
    -name $MEMDIR/$base \
    -pol IQU \
    -j $SLURM_CPUS_PER_TASK \
    -verbose \
    -channels-out 36 \
    -scale 2asec \
    -size 4096 4096 \
    -join-polarizations \
    -join-channels \
    -squared-channel-joining \
    -mgain 0.7 \
    -niter 1000000 \
    -auto-mask 3 \
    -force-mask-rounds 7 \
    -auto-threshold 0.5 \
    -gridder wgridder \
    -weight briggs -0.5 \
    -log-time \
    -mem 256 \
    -abs-mem 90 \
    -minuv-l 300 \
    -nmiter 0 \
    -local-rms \
    -local-rms-window 60 \
    -no-mf-weighting \
    $MEMDIR/$base.ms

rclone copy -P --transfers $SLURM_NTASKS --checkers $SLURM_NTASKS --include="$base*MFS*image.fits" $MEMDIR/ ./

mpirun beamcon_2D $MEMDIR/$base*image.fits -v --mpi

fitscube $MEMDIR/$base*{0..9}-I-image.sm.fits $MEMDIR/$base.icube.fits
fitscube $MEMDIR/$base*{0..9}-Q-image.sm.fits $MEMDIR/$base.qcube.fits
fitscube $MEMDIR/$base*{0..9}-U-image.sm.fits $MEMDIR/$base.ucube.fits

rclone copy -P --transfers $SLURM_NTASKS --checkers $SLURM_NTASKS --include="$base*cube.fits" $MEMDIR/ ./
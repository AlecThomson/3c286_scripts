import numpy as np
import matplotlib.pyplot as plt
from astropy.coordinates import SkyCoord
from astropy.io import fits
from spectral_cube import SpectralCube
from pathlib import Path
import os

from arrakis.cutout import CutoutArgs, cutout_image, cutout_weight
from arrakis.linmos import genparset, linmos, ImagePaths, find_images
from arrakis import imager
from arrakis.process_spice import create_dask_runner
from arrakis.logger import logger
from prefect import task, flow

@flow(name="Cutout 3C286")
def cutout_3c286():
    threec_286 = SkyCoord.from_name('3C 286')
    cutout_args = CutoutArgs(
        ra_high=threec_286.ra.deg + 0.0,
        ra_low=threec_286.ra.deg - 0.0,
        dec_high=threec_286.dec.deg + 0.0,
        dec_low=threec_286.dec.deg - 0.0,
        outdir="/scratch3/projects/spiceracs/askap_pol_testing/52087_cal/cutouts/3C286"
    )
    updates = {}
    w_updates = {}
    for stokes in "iqu":
        updates[stokes] = {}
        w_updates[stokes] = {}
        for beam in range(36):
            image_name = f"/scratch3/projects/spiceracs/askap_pol_testing/52087_cal/image.restored.{stokes}.3C286_15deg.contcube.beam{beam:02d}.conv.fits"
            update = cutout_image(
                image_name=image_name,
                data_in_mem=fits.getdata(image_name),
                old_header=fits.getheader(image_name),
                cube=SpectralCube.read(image_name),
                source_id="3C286",
                cutout_args=cutout_args,
                field="3C286",
                beam_num=beam,
                stoke=stokes,
                pad=3,
            )
            w_update = cutout_weight(
                image_name=image_name,
                source_id="3C286",
                cutout_args=cutout_args,
                field="3C286",
                stoke=stokes,
                beam_num=beam,
                dryrun=False,
            )
            updates[stokes][beam] = update
            w_updates[stokes][beam] = w_update

    beams_dict = dict(
            Source_ID="3C286", 
            beams={
                "3C286": {
                    "beam_list": [1,6],
                    f"i_beam0_image_file": updates["i"][0]._doc["$set"]["beams.3C286.i_beam0_image_file"],
                    f"i_beam1_image_file": updates["i"][1]._doc["$set"]["beams.3C286.i_beam1_image_file"],
                    f"i_beam6_image_file": updates["i"][6]._doc["$set"]["beams.3C286.i_beam6_image_file"],
                    f"q_beam0_image_file": updates["q"][0]._doc["$set"]["beams.3C286.q_beam0_image_file"],
                    f"q_beam1_image_file": updates["q"][1]._doc["$set"]["beams.3C286.q_beam1_image_file"],
                    f"q_beam6_image_file": updates["q"][6]._doc["$set"]["beams.3C286.q_beam6_image_file"],
                    f"u_beam0_image_file": updates["u"][0]._doc["$set"]["beams.3C286.u_beam0_image_file"],
                    f"u_beam1_image_file": updates["u"][1]._doc["$set"]["beams.3C286.u_beam1_image_file"],
                    f"u_beam6_image_file": updates["u"][6]._doc["$set"]["beams.3C286.u_beam6_image_file"],
                    f"i_beam0_weight_file": w_updates["i"][0]._doc["$set"]["beams.3C286.i_beam0_weight_file"],
                    f"i_beam1_weight_file": w_updates["i"][1]._doc["$set"]["beams.3C286.i_beam1_weight_file"],
                    f"i_beam6_weight_file": w_updates["i"][6]._doc["$set"]["beams.3C286.i_beam6_weight_file"],
                    f"q_beam0_weight_file": w_updates["q"][0]._doc["$set"]["beams.3C286.q_beam0_weight_file"],
                    f"q_beam1_weight_file": w_updates["q"][1]._doc["$set"]["beams.3C286.q_beam1_weight_file"],
                    f"q_beam6_weight_file": w_updates["q"][6]._doc["$set"]["beams.3C286.q_beam6_weight_file"],
                    f"u_beam0_weight_file": w_updates["u"][0]._doc["$set"]["beams.3C286.u_beam0_weight_file"],
                    f"u_beam1_weight_file": w_updates["u"][1]._doc["$set"]["beams.3C286.u_beam1_weight_file"],
                    f"u_beam6_weight_file": w_updates["u"][6]._doc["$set"]["beams.3C286.u_beam6_weight_file"],
                    
                }
            }
    )
    datadir=Path("/scratch3/projects/spiceracs/askap_pol_testing/52087_cal/cutouts")
    holofile=Path("/scratch3/projects/spiceracs/akpb.iquv.closepack36.54.943MHz.SB51811.cube.fits")
    siffile=Path("/datasets/work/sa-mhongoose/work/containers/askapsoft_1.15.0-openmpi4.sif")
    for stoke in "iqu":
        try:
            image_paths = find_images(
                field="3C286",
                beams=beams_dict,
                stoke=stoke,
                datadir=datadir,
            )
            parset = genparset(
                image_paths=image_paths,
                stoke=stoke,
                datadir=datadir,
                holofile=holofile
            )
            linmos(
                parset=parset,
                fieldname="3C286",
                image=siffile,
                holofile=holofile,
            )
        except Exception as e:
            logger.error(f"Failed for {stoke}: {e}")
            continue
def main(
        do_imager: bool=False,
        do_cutout: bool=False,
):

    if do_imager:
        dask_runner = create_dask_runner(
            dask_config="/scratch3/projects/spiceracs/fresh_test_spiceracs/petrichor.yaml",
            overload=True,
        )
        _ = imager.main.with_options(
            name=f"Arrakis Imaging -- 3C286", task_runner=dask_runner
        )(
            msdir=Path("/scratch3/projects/spiceracs/3C286_flint_main/52087/"),
            out_dir=Path("/scratch3/projects/spiceracs/askap_pol_testing/52087_cal"),
            size=4096,
            force_mask_rounds=10,
            minuv=300,
            gridder="wgridder",
            local_rms=True,
            local_rms_window=65,
            wsclean_path=Path("/datasets/work/sa-mhongoose/work/containers/wsclean:force_mask.sif"),
            ms_glob_pattern="SB52087.3C286_15deg.beam*[0-9].ms",
            skip_fix_ms=True,
            nchan=8,
        )
        del dask_runner

    if do_cutout:
        dask_runner = create_dask_runner(
            dask_config="/scratch3/projects/spiceracs/fresh_test_spiceracs/rm_petrichor.yaml",
        )
        cutout_3c286.with_options(
            task_runner=dask_runner
        )()
        del dask_runner

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--do-imager", action="store_true")
    parser.add_argument("--do-cutout", action="store_true")
    args = parser.parse_args()
    main(
        do_imager=args.do_imager,
        do_cutout=args.do_cutout,
    )
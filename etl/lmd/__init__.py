import logging
import glob
import os
import shutil
from os.path import exists

import db
from etl.util import download_file_if_not_exists, ensure_dir_exists
from etl.util.tar import extract_all_tar_gz
import hdf5_getters

logging.basicConfig(level=logging.INFO)

LMD_BASEPATH = os.environ["STORAGE_PATH"] + "/big/lmd/"

targz_files = [
    "lmd_full.tar.gz",
    "lmd_matched.tar.gz",
    "lmd_matched_h5.tar.gz"
]


def _download_lmd():
    baseurl = "http://hog.ee.columbia.edu/craffel/lmd/"
    for file in targz_files:
        download_file_if_not_exists(baseurl + file, LMD_BASEPATH + file)


def _process_midi(path: str, outfile: str, h5_path=None):
    midi_filename = path.split("/")[-1]
    if h5_path:
        metadata = db.get(midi_filename)
        if not metadata:
            h5 = hdf5_getters.open_h5_file_read(h5_path)
            year = int(hdf5_getters.get_year(h5)) or None
            artist = hdf5_getters.get_artist_name(h5).decode("utf-8")
            release = hdf5_getters.get_release(h5).decode("utf-8")
            title = hdf5_getters.get_title(h5).decode("utf-8")
            h5.close()
            logging.info(f"Uploading metadata for {midi_filename}")
            db.put(midi_filename, {"Year": year, "Artist": artist, "Release": release, "Title": title})

    ensure_dir_exists(outfile)
    shutil.copyfile(path, outfile)


def _get_outfile_name(path: str):
    midi_filename = path.split("/")[-1]
    return os.environ["STORAGE_PATH"] + "/out/" + midi_filename


def _process_matched():
    files = glob.glob(LMD_BASEPATH + "lmd_matched/**/*.mid", recursive=True)
    num_original = len(files)
    files = [(p, _get_outfile_name(p)) for p in files]
    unprocessed = [(p, out) for p, out in files if not exists(out)]
    logging.info(f"{len(unprocessed)} of {num_original} LMD Matched files need to still be processed...")
    for p, out in unprocessed:
        subdir = "/".join(p.split("/")[-5:-1])
        h5_path = LMD_BASEPATH + "lmd_matched_h5/" + subdir + ".h5"
        _process_midi(p, out, h5_path)


def _process_full():
    files = glob.glob(LMD_BASEPATH + "lmd_full/**/*.mid", recursive=True)
    num_original = len(files)
    files = [(p, _get_outfile_name(p)) for p in files]
    unprocessed = [(p, out) for p, out in files if not exists(out)]
    logging.info(f"{len(unprocessed)} of {num_original} LMD Full files need to still be processed...")
    for p, out in unprocessed:
        _process_midi(p, out)


def process():
    _download_lmd()
    extract_all_tar_gz(LMD_BASEPATH)

    # go over lmd_matched midi files first
    logging.info("Processing LMD Matched")
    _process_matched()

    logging.info("Processing LMD Full")
    _process_full()

    logging.info("Finished")

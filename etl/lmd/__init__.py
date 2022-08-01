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

BASEPATH = os.environ["STORAGE_PATH"] + "/big/lmd/"

files = [
    "lmd_full.tar.gz",
    "lmd_matched.tar.gz",
    "lmd_matched_h5.tar.gz"
]


def _download_lmd():
    baseurl = "http://hog.ee.columbia.edu/craffel/lmd/"
    for file in files:
        download_file_if_not_exists(baseurl + file, BASEPATH + file)


def _process_midi(path: str, h5_path=None):
    midi_filename = path.split("/")[-1]
    outfile = os.environ["STORAGE_PATH"] + "/out/" + midi_filename
    if exists(outfile):
        return
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


def _process_matched():
    files = glob.glob(BASEPATH + "lmd_matched/**/*.mid", recursive=True)
    for file in files:
        subdir = "/".join(file.split("/")[-5:-1])
        h5_path = BASEPATH + "lmd_matched_h5/" + subdir + ".h5"
        _process_midi(file, h5_path)


def _process_full():
    files = glob.glob(BASEPATH + "lmd_full/**/*.mid", recursive=True)
    for file in files:
        _process_midi(file)


def process():
    _download_lmd()
    extract_all_tar_gz(BASEPATH)

    # go over lmd_matched midi files first
    _process_matched()
    _process_full()

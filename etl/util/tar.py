import logging
import os
import tarfile

from etl.util import dir_exists


def track_progress(members):
    for member in members:
        logging.info(f"Extracting member {member}")
        yield member


def extract_all_tar_gz(path: str):
    # given path to directory, make sure all .tar.gz files are extracted
    logging.info(f"Extracting .tar.gz's in {path}")
    if not dir_exists(path):
        raise Exception(f"Path {path} does not exist!")
    for file in os.listdir(path):
        dirname = file.split(".tar.gz")[0]
        filename = os.fsdecode(file)
        if filename.endswith(".tar.gz"):
            if dir_exists(f"{path}/{dirname}"):
                logging.info(f"Skipping {filename} because it has already been extracted...")
                continue
            logging.info(f"Extracting {filename}...")
            with tarfile.open(f"{path}/{filename}", 'r') as tarball:
                tarball.extractall(path=path, members=track_progress(tarball))

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
                def is_within_directory(directory, target):
                    
                    abs_directory = os.path.abspath(directory)
                    abs_target = os.path.abspath(target)
                
                    prefix = os.path.commonprefix([abs_directory, abs_target])
                    
                    return prefix == abs_directory
                
                def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                
                    for member in tar.getmembers():
                        member_path = os.path.join(path, member.name)
                        if not is_within_directory(path, member_path):
                            raise Exception("Attempted Path Traversal in Tar File")
                
                    tar.extractall(path, members, numeric_owner=numeric_owner) 
                    
                
                safe_extract(tarball, path=path, members=track_progress(tarball))

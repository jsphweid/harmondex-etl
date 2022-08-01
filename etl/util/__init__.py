import logging
import os
from os.path import exists
import requests as requests
from pathlib import Path


def ensure_dir_exists(path: str):
    Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)


def download_file(url: str, path: str):
    logging.info(f"Downloading {url} to {path}...")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        ensure_dir_exists(path)
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def download_file_if_not_exists(url: str, path: str):
    if exists(path):
        logging.info(f"Skipping {url} because it is already downloaded...")
    else:
        download_file(url, path)


def dir_exists(path: str):
    return os.path.isdir(path)

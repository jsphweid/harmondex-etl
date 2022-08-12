import os
import glob
import logging
import hashlib
from os.path import exists
import audio_metadata
from typing import Optional
from pydub import AudioSegment

import db
import mt3

JLW_BASEPATH = os.environ["STORAGE_PATH"] + "/big/jlw/"
OUT = os.environ["STORAGE_PATH"] + "/out"


def path_is_piano(path: str) -> bool:
    for w in ["Alkan/", "Liszt/", "Hamelin/"]:
        if w in path:
            return True
    return False


def get_metadata(path: str) -> Optional[dict]:
    res = {}
    try:
        metadata = audio_metadata.load(path)
        for tag in metadata.tags:
            val = metadata.tags[tag][0]
            if type(val) == str:
                res[tag.capitalize()] = val
        return res
    except:
        return res


def process_file(h: str, path: str):
    midi_filename = f"{h}.mid"
    midi_key = f"midi/{midi_filename}"
    midi_path = f"{OUT}/{midi_key}"
    mp3_key = f"mp3/{h}.mp3"
    mp3_path = f"{OUT}/{mp3_key}"

    try:
        is_piano = path_is_piano(path)
        audio = AudioSegment.from_file(path)
        audio.export(mp3_path, format="mp3")
        mt3.transcribe(mp3_path, midi_path, is_piano)
        metadata = get_metadata(path)
        db.put(midi_filename, {
            **metadata,
            "TranscriptionModel": "magenta:mt3:ismir2021" if is_piano else "magenta:mt3:full"
        })
    except Exception as e:
        logging.error(f"Failed on: {path}")
        logging.exception(e)


def already_processed(h: str) -> bool:
    return exists(f"{OUT}/midi/{h}.mid") and exists(f"{OUT}/mp3/{h}.mp3")


def process():
    logging.info("Processing JLW personal 'dataset'")

    all_files = glob.glob(JLW_BASEPATH + "**/*.*", recursive=True)
    all_files = [(f[len(os.environ["STORAGE_PATH"])+5:], f) for f in all_files]
    all_files = [(hashlib.md5(s.encode()).hexdigest(), f) for s, f in all_files]
    unprocessed = [(h, f) for h, f in all_files if not already_processed(h)]
    
    logging.info(f"Processing {len(unprocessed)} of {len(all_files)} in jlw")
    
    for md5, path in unprocessed[0:100]: # todo tmp
        logging.info(f"Processing: {path}")
        process_file(md5, path)

    logging.info("Finished with JLW personal 'dataset'")

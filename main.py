import os

from etl import lmd


if __name__ == "__main__":
    if not os.getenv("STORAGE_PATH"):
        raise Exception("STORAGE_PATH env var is required!")
    lmd.process()

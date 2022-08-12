import os

if __name__ == "__main__":
    if not os.getenv("STORAGE_PATH"):
        raise Exception("STORAGE_PATH env var is required!")

    from etl import lmd, jlw
    jlw.process()
    lmd.process()

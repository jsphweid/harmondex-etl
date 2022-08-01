"""
job of this script is to make sure everything is downloaded, extracted
"""


def download_file(url: str):
    filename = url.split("/")[-1]
    # if
    # http://hog.ee.columbia.edu/craffel/lmd/lmd_full.tar.gz

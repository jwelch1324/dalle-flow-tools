from docarray import Document
from docarray.array.match import MatchArray
import sqlite3
import os
import io
import hashlib
import tempfile
import PIL.Image
import re
import matplotlib.pyplot as plt
import numpy as np


def hash_data(data):
    BLOCKSIZE = 65536
    hasher = hashlib.md5()
    buffer = io.BytesIO(data)
    buf = buffer.read(BLOCKSIZE)
    while len(buf) > 0:
        hasher.update(buf)
        buf = buffer.read(BLOCKSIZE)
    return hasher.hexdigest()
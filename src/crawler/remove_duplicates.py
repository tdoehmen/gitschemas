import sys
import os
import hashlib
import shutil

def chunk_reader(fobj, chunk_size=1024):
    """Generator that reads a file in chunks of bytes"""
    while True:
        chunk = fobj.read(chunk_size)
        if not chunk:
            return
        yield chunk


path = "../../data/sqlfiles/"
quarantine = "../../data/sqlfiles_duplicates/"
duplicates = []
hashes = {}
i = 0
j = 0
for dirpath, dirnames, filenames in os.walk(path):
    for filename in filenames:
        full_path = os.path.join(dirpath, filename)
        hashobj = hashlib.sha256()
        for chunk in chunk_reader(open(full_path, 'rb')):
            hashobj.update(chunk)
        file_id = (hashobj.digest(), os.path.getsize(full_path))
        duplicate = hashes.get(file_id, None)
        if duplicate:
            duplicates.append(full_path)
            j += 1
            shutil.move(full_path, quarantine+os.path.basename(full_path))
        else:
            hashes[file_id] = full_path

        if i % 10000 == 0:
            print(f"\r{j} duplicates found, after {i} files", end='')
        i += 1
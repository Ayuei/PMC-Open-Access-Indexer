from pymongo import MongoClient
import pubmed_parser as pp
import psutil 
from tqdm import tqdm
from queue import Queue
import multiprocessing as mp 
import glob
import time
import shutil
import os
from tarfile import TarFile
from collections import OrderedDict 
import unidecode
import re

SENTINEL = 1

def listener(q):
    # Update the tqdm instance based on the consumers
    pbar = tqdm(total = 30e6, desc="Processing jobs", position=1)
    for item in iter(q.get, None):
        pbar.update()

def tar_file_generator(path):
    tar = TarFile.open(path)

    for member in iter(tar.next, None):
        f = tar.extractfile(member)
        if f:
            data = f.read().decode('utf-8')
            f.close()
            yield data

COL_NAME = "PuBMED_Central_Open_Access"
DB_NAME = "Biomedical"
fname = "download_scripts/comm_use.A-B.xml.tar.gz"
pattern = re.compile(r'(\s?\[\d\])|(\s?\[\d+-{1}\d+])')

class MongoIndexer(object):
    def __init__(self, db_name, col_name):
        self.db = MongoClient()[db_name]
        self.col = self.db[col_name]

    def index_document(self, doc):
        parsed_doc = pp.parse_pubmed_paragraph(doc, all_paragraph=True)
        pmid = None
        pmc = None
        ref_ids = []
        full_text = []

        for para in parsed_doc:
            if not pmid and 'pmid' in para:
                pmid = para['pmid']
            if not pmc and 'pmc' in para:
                pmc = para['pmc']

            section = para['section']
            text = para['text']

            # We want pure text, not headings
            if section and text:
                clean_text = pattern.sub('', unidecode.unidecode(para['text']))
                full_text.append(clean_text)

            if para['reference_ids']:
                ref_ids.extend(para['reference_ids'])

        full_text = '\n\n'.join(full_text)

        _id = None

        if pmid is None:
            pmid = -1
        else:
            _id = "pmid"+str(pmid)

        if pmc is None:
            pmc = -1
        else:
            _id = "pmc"+str(pmc)

        doc_to_insert = {
                "pmid": pmid,
                "pmc": pmc,
                "ref_ids": ref_ids,
                "text": full_text,
               }

        if _id:
            doc_to_insert['_id'] = _id

        self.col.insert_one(doc_to_insert)

def worker(job_q, prog_q, indexer_q):
    while True:
        indexer = indexer_q.get()
        document = job_q.get()
        if document is None:
            break
        try:
            indexer.index_document(document)
        except KeyError:
            continue
        indexer_q.put(indexer)
        prog_q.put(SENTINEL)

    prog_q.put(None)


if __name__ == "__main__":
    num_cpus = int((psutil.cpu_count(logical=True)*0.7))
    NUM_WORKERS = num_cpus
    print(f'Starting with {num_cpus} cpus')

    progress_queue = mp.Queue()
    indexer_queue= Queue(maxsize=NUM_WORKERS)
    job_queue = mp.Queue(maxsize=NUM_WORKERS)

    supervisor = mp.Process(target=listener, args=(progress_queue,))

    for i in range(NUM_WORKERS):
        indexer_queue.put(MongoIndexer(DB_NAME, COL_NAME))

    print("Waiting for MongoDB to fork")
    time.sleep(NUM_WORKERS)

    pool = mp.Pool(NUM_WORKERS, initializer=worker, initargs=(job_queue, progress_queue,
        indexer_queue))

    for fname in glob.glob('download_scripts/*tar.gz'):
        print(f'Starting {fname}')
        for i, doc_xml in enumerate(tqdm(tar_file_generator(fname), position=0)):
            job_queue.put(doc_xml)

    for _ in range(NUM_WORKERS):
        job_queue.put(None)

    pool.close()
    pool.join()
    supervisor.join()

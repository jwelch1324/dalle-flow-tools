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
from .utils import hash_data

class QueryDatabase:
    def __init__(self, dbfile="queries.db", datastore="db_datastore"):
        self.dbfile = dbfile
        self.conn = sqlite3.connect(self.dbfile)
        self.lastcur = None
        self.datastore_path = datastore
        if not os.path.isdir(self.datastore_path):
            os.makedirs(self.datastore_path)
            self.create_buckets()
        hasher = hashlib.md5()
    
    def hash_data(self, data):
        return hash_data(data)
    
    def __hash_path(self, fhash):
        bucket = fhash[0:4]
        return os.path.join(self.datastore_path, bucket, fhash)
    
    def get_file_path(self, fhash, silent=False):
        fpath = self.__hash_path(fhash)
        if not os.path.isfile(fpath):
            raise FileExistsError("Error -- the file with the given hash does not exist in the datastore")

        return fpath
    
    def create_buckets(self):
        """
        Creates the bucket structure within the datastore
        """
        from itertools import product
        charlist = list(map(lambda x: str(x), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 'a', 'b', 'c', 'd', 'e', 'f']))
        buckets = list(product(*[charlist] * 4))
        buckets = list(map(lambda x: ''.join(x), buckets))

        for bucket in buckets:
            if not os.path.isdir(os.path.join(self.datastore_path, bucket)):
                os.makedirs(os.path.join(self.datastore_path, bucket))
        
    def initdb(self):
        try:
            self.conn.execute('''CREATE TABLE QUERIES (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            INQUERY TEXT NOT NULL,
            FILEHASH TEXT NOT NULL);''')
        except:
            pass
        
        try:
            self.conn.execute('''CREATE TABLE SESSIONS (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            SESSIONNAME TEXT NOT NULL,
            FILEHASH TEXT NOT NULL);''')
        except:
            pass

        self.conn.commit()
        
        print("Database is ready")
        
    def save_qd(self, querydoc):
        if isinstance(querydoc.da, MatchArray):
            keystr = querydoc.da[0].text
        else:
            keystr = querydoc.da.text
            
        qdbytes = querydoc.da.to_bytes()
        qdhash = self.hash_data(qdbytes)
        self.conn.execute(f"INSERT INTO QUERIES (INQUERY, FILEHASH) VALUES (\"{keystr}\",\"{qdhash}\")")
        self.conn.commit()
        
        with open(self.__hash_path(qdhash),"wb") as ofile:
            ofile.write(qdbytes)
            
    def save_session(self, session_name, qs):
        
        if self.__get_session_hash(session_name) is not None:
            raise ValueError("session name must be unique in the database")
        
        all_bytes = qs.to_bytes()
        sHash = self.hash_data(all_bytes)
        self.conn.execute(f"INSERT INTO SESSIONS (SESSIONNAME, FILEHASH) VALUES(\"{session_name}\",\"{sHash}\")")
        self.conn.commit()
        
        print(f"Session {session_name} saved to database")
        
        with open(self.__hash_path(sHash),"wb") as ofile:
            ofile.write(all_bytes)
            
    def replace_session(self, session_name, newQS):
        shash = self.__get_session_hash(session_name)
        if shash is not None:
            self.remove_session(session_name)
            
        self.save_session(session_name,newQS)

    def remove_session(self,session_name):
        shash = self.__get_session_hash(session_name)
        if shash is None:
            print(f"No Session with the name {session_name} exists in the database")
            return
        self.conn.execute(f"DELETE FROM SESSIONS WHERE FILEHASH = \"{shash}\"")
        self.conn.commit()
        print(f"Session {session_name} removed from database")
        
    def load_session(self, session_name):
        from .session import QuerySession
        shash = self.__get_session_hash(session_name)
        if shash is None:
            raise ValueError(f"session name [{session_name}] does not exist in the database")
        
        with open(self.get_file_path(shash),"rb") as infile:
            data = infile.read()
        list
        newS = QuerySession(self)
        newS.from_bytes(data)
        return newS
        
        
    def __get_session_hash(self, session_name):
        hashlist = self.conn.execute(f"SELECT * FROM SESSIONS WHERE SESSIONNAME = \"{session_name}\"").fetchall()
        if len(hashlist) == 0:
            return None
        else:
            return hashlist[0][2]
    def show_sessions(self):
        sessions = self.conn.execute(f"SELECT * FROM SESSIONS").fetchall()
        for s in sessions:
            print(f"{s[0]}:\t{s[1]}")
        
    def show_queries(self):
        self.lastcur = self.conn.execute(f"SELECT * FROM QUERIES")
        self.last_queries = self.lastcur.fetchall()
        for i in range(len(self.last_queries)):
            print(i,self.last_queries[i][1])
            
    def queries_like(self, likestr):
        return None
    
    def get_hash_from_list(self, idx):
        if self.lastcur is None:
            return None
        
        if idx >= len(self.last_queries):
            return None
        
        return self.last_queries[idx][2]
        
            
    def rebuild_doc(self, fhash, dalle_flow_endpoint="grpc://10.10.28.110:51005"):
        fpath = self.get_file_path(fhash)
        with open(fpath, 'rb') as infile:
            data = infile.read()
        newda = Document().from_bytes(data)
        return QueryDocument(da=newda,url=dalle_flow_endpoint)
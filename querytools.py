from docarray import Document
from docarray.array.match import MatchArray
import sqlite3
import os
import io
import hashlib

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
        BLOCKSIZE = 65536
        hasher = hashlib.md5()
        buffer = io.BytesIO(data)
        buf = buffer.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = buffer.read(BLOCKSIZE)
        return hasher.hexdigest()
    
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
        self.conn.execute('''CREATE TABLE QUERIES (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        INQUERY TEXT NOT NULL,
        FILEHASH TEXT NOT NULL);''')
        
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

    
class QueryDocument:
    def __init__(self,url="grpc://10.10.28.110:51005", da=None):
        self.url = url
        self.da = da
        
    def query(self, prompt):
        self.da = Document(text=prompt).post(self.url, parameters={'num_images':8}).matches
        self.da.plot_image_sprites(fig_size=(10,10),show_index=True)
        
    def diffuse(self, skip_rate = 0.5, idx = 0):
        if isinstance(self.da, MatchArray):
            newda = self.da[idx]
        else:
            newda = self.da
            
        def adddiffusetag(x):
            x.text = x.text + f" -- diffuse item[{idx}] sr[{skip_rate}]"
            
        newda = newda.post(f'{self.url}', parameters={'skip_rate': skip_rate, 'num_images': 10}, target_executor='diffusion').matches
        list(newda.map(adddiffusetag))
        newda.plot_image_sprites(fig_size=(10,10), show_index=True)
        return QueryDocument(self.url,newda)
    
    def upscale(self, idx=0):
        newda = self.da[idx].post(f'{self.url}/upscale')
        newda.display()
        return QueryDocument(self.url,newda)
    
    def to_base64_file(self,file):
        with open(file, "w") as ff:
            ff.write(self.da.to_base64())
            
    def from_base64_file(self, file):
        with open(file, "r") as ff:
            bb = str(ff.readline())
        self.da = Document().from_base64(bb)
        
    def show_tiles(self):
        self.da.plot_image_sprites(fig_size=(15,15),show_index=True)
        
    def save_image(self, outfile, idx=0):
        if isinstance(self.da, MatchArray):
            tda = self.da[idx]
        else:
            tda = self.da
            
        tda.save_uri_to_file(outfile)
        
        
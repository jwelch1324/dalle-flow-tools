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
    
class QueryDocument:
    def __init__(self,url="grpc://10.10.28.110:51005", da=None):
        self.url = url
        self.da = da
        self.parent_doc = None
        self.myhash = None
        
    def query(self, prompt):
        self.da = Document(text=prompt).post(self.url, parameters={'num_images':8}).matches
        #self.da.plot_image_sprites(fig_size=(10,10),show_index=True)
        
    def diffuse(self, skip_rate = 0.5, idx = 0):
        if isinstance(self.da, MatchArray):
            newda = self.da[idx]
        else:
            newda = self.da
            
        def adddiffusetag(x):
            x.text = x.text + f" -- diffuse item[{idx}] sr[{skip_rate}]"
            
        newda = newda.post(f'{self.url}', parameters={'skip_rate': skip_rate, 'num_images': 10}, target_executor='diffusion').matches
        list(newda.map(adddiffusetag))
        #newda.plot_image_sprites(fig_size=(10,10), show_index=True)
        return QueryDocument(self.url,newda)
    
    def get_text(self):
        if isinstance(self.da, MatchArray):
            return self.da[0].text
        else:
            return self.da.text
    
    def upscale(self, idx=0):
        def adddiffusetag(x):
            x.text = x.text + f" -- diffuse item[{idx}] sr[{skip_rate}]"
        
        newda = self.da[idx].post(f'{self.url}/upscale')
        newda.text = newda.text + f" -- upscale item[{idx}]"
        #newda.display()
        return QueryDocument(self.url,newda)
    
    def to_base64_file(self,file):
        with open(file, "w") as ff:
            ff.write(self.da.to_base64())
            
    def from_base64_file(self, file):
        with open(file, "r") as ff:
            bb = str(ff.readline())
        self.da = Document().from_base64(bb)
        
    def show_tiles(self):
        if isinstance(self.da, MatchArray):
            self.da.plot_image_sprites(fig_size=(15,15),show_index=True)
        else:
            self.da.display()
        
    def save_image(self, outfile, idx=0):
        if isinstance(self.da, MatchArray):
            tda = self.da[idx]
        else:
            tda = self.da
            
        tda.save_uri_to_file(outfile)
        
    def get_hash(self):
        if self.myhash is None:
            #Cache the hash for later calls -- once the document is generated the data should never change so the hash should never change
            qdbytes = self.da.to_bytes()
            self.myhash = hash_data(qdbytes)
        return self.myhash
        
        
        
class QueryDocNode:
    def __init__(self, doc, parent, children):
        self.doc = doc
        self.children = children
        self.parent = parent
        self.active_child = None if len(children) == 0 else children[0]
        self.tags = []

    def list_children(self):
        if self.active_child is not None:
            print(f"Active Child: {self.doc.get_text()}")

        for i in range(len(self.children)):
            c = self.children[i]
            print(f"{i} - {c.doc.get_text()}")

    def set_active_child(self, idx):
        if idx >= len(self.children):
            raise ValueError(f"requested child does not exist [{idx}]")
        self.active_child = self.children[idx]

    def add_child(self, doc):
        self.children.append(doc)
        
    def has_children(self):
        return len(self.children) > 0
    
    def get_all_child_hashes(self):
        all_hashes = []
        for cc in self.children:
            all_hashes.append(cc.doc.get_hash())
            sub_hashes = cc.get_all_child_hashes()
            all_hashes.extend(sub_hashes)
            
        return all_hashes
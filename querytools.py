from docarray import Document
from docarray.array.match import MatchArray
import sqlite3
import os
import io
import hashlib

def hash_data(data):
    BLOCKSIZE = 65536
    hasher = hashlib.md5()
    buffer = io.BytesIO(data)
    buf = buffer.read(BLOCKSIZE)
    while len(buf) > 0:
        hasher.update(buf)
        buf = buffer.read(BLOCKSIZE)
    return hasher.hexdigest()

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
        newda.text = newda.text + f" -- upscale"
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

    def list_children(self):
        if self.active_child is not None:
            print(f"Active Child: {self.cur_doc.doc.get_text()}")

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

class QuerySession:  
    
    def __init__(self, qdb:QueryDatabase, dalle_url="grpc://10.10.28.110:51005"):
        self.qdb = qdb
        self.dalle_url = dalle_url
        self.cur_doc = None
        self.document_stack = []
        self.stack_idx = None
        self.unsaved_changes = False
        
    def query(self, qstr:str):
        """ 
        query -- runs a dalle query -- 
        will replace the current document with a new one and start a fresh document stack
        """
        #self.__check_saved_changes()
        self.cur_doc = QueryDocNode(QueryDocument(self.dalle_url),None,[])
        self.document_stack = []
        self.stack_idx = 0
        self.prev_stack_idx = None
        
        self.cur_doc.doc.query(qstr)
        self.document_stack.append(self.cur_doc)
        self.unsaved_changes = True
        self.show()
        
    def __check_valid_doc(self):
        if self.cur_doc is None:
            raise TypeError("no document is currently active, run a query first")
            
    def __check_saved_changes(self):
        if self.cur_doc is not None and self.unsaved_changes:
            print("Warning: you have not saved the current document session -- either save it or issue the reset() command to discard unsaved changes before running this query")
            return
        
    def diffuse(self,skip_rate=0.5,idx=0):
        """
        diffuse will perform a diffusion operation on the item at index `idx` (defaults to zero)
        
        Parameters:
            skip_rate:float -- the skip rate to use in the diffusion
            idx:int -- the index of the document you wish to diffuse
        """
        
        self.__check_valid_doc()
        
        diffuse_doc = QueryDocNode(self.cur_doc.doc.diffuse(skip_rate, idx), self.cur_doc, [])
        
        self.cur_doc.add_child(diffuse_doc)
        self.document_stack.append(diffuse_doc)
        self.cur_doc = self.document_stack[-1]
        self.prev_stack_idx = self.stack_idx
        self.stack_idx = len(self.document_stack)-1
        self.show()
        
    def save_current(self):
        """
        saves the currently active document to the database
        """
        
        self.__check_valid_doc()
        self.qdb.save_qd(self.cur_doc.doc)
        
    def start_from_doc(self, fhash:str, ignore_unsaved=False):
        if not ignore_unsaved:
            self.__check_saved_changes()
        self.document_stack = []
        self.cur_doc = QueryDocNode(self.qdb.rebuild_doc(fhash, self.dalle_url),None,[])
        self.document_stack.append(self.cur_doc)
        return       
    
    def set_current_doc(self, doc):
        if isinstance(doc, QueryDocNode):
            self.cur_doc = doc
        else:
            self.cur_doc = QueryDocNode(doc,None,[])
        self.document_stack = [self.cur_doc]
    
    def fork(self):
        newS = QuerySession(self.qdb,self.dalle_url)
        newS.cur_doc = copy.deepcopy(self.cur_doc)
        newS.document_stack = copy.deepcopy(self.document_stack)
        newS.unsaved_changes = self.unsaved_changes
        newS.stack_idx = self.stack_idx
        newS.prev_stack_idx = self.prev_stack_idx
        return newS
    
    def prune_current_document(self):
        if self.cur_doc.doc.get_hash() == self.document_stack[0].doc.get_hash():
            print("Warning -- you are attempting to prune the root node in the graph, this will erase the entire graph -- if this is what you intend to do call reset_graph() instead")
            return
        parentNode = self.cur_doc.parent
        
        all_hashes_to_remove = self.cur_doc.get_all_child_hashes()
        all_hashes_to_remove.append(self.cur_doc.doc.get_hash())
        
        newChildren = []
        for cc in parentNode.children:
            if cc.doc.get_hash() == self.cur_doc.doc.get_hash():
                continue
            else:
                newChildren.append(cc)
        parentNode.children = newChildren
              
        # Remove the pruned node and all its children from the document_stack
        newDocStack = []
        for doc in self.document_stack:
            if doc.doc.get_hash() in all_hashes_to_remove:
                continue
            else:
                newDocStack.append(doc)
                
        self.document_stack = newDocStack
        
        print(f"Pruned {self.cur_doc.doc.get_text()}")
        print(f"Pruned a total of {len(all_hashes_to_remove)} documents from the graph and stack")
        self.cur_doc = parentNode
        print(f"Active Document: {self.cur_doc.doc.get_text()}")        
        
    def reset_graph(self):
        self.cur_doc = None
        self.document_stack = []
        
    def show(self):
        print(self.cur_doc.doc.get_text())
        self.cur_doc.doc.show_tiles()
        
    def up(self):
        self.__check_valid_doc()
        if self.cur_doc.parent is not None:
            self.cur_doc = self.cur_doc.parent
        else:
            print("no parent document available -- staying where we are")
            return
        
        print(f"Active Document: {self.cur_doc.doc.get_text()}")
            
        #TODO: Set the stack_idx and prev_stack_idx correctly when doing this up operation
    
    def show_children(self):
        self.__check_valid_doc()
        if len(self.cur_doc.children) == 0:
            print("No Children to display")
        else:
            self.cur_doc.list_children()
            
    def down(self, child_idx=0):
        if not self.cur_doc.has_children():
            print("no children on current document, cannot move down the graph")
        else:
            self.cur_doc.set_active_child(child_idx)
            self.cur_doc = self.cur_doc.active_child
            
        print(f"Active Document: {self.cur_doc.doc.get_text()}")
            
    def back(self):
        self.__check_valid_doc()
        if len(self.document_stack) < 2:
            raise ValueError("the document stack has only a single document in it -- there is nowhere to go back to")
        
        if self.stack_idx == 0:
            print("We are already at the start of the back, no where to move back to")
            return
        
        self.prev_stack_idx = self.stack_idx
        self.stack_idx -= 1
        self.cur_doc = self.document_stack[self.stack_idx]
        print(f"Active Document: {self.cur_doc.doc.get_text()}")
        
    def forward(self):
        self.__check_valid_doc()
        if self.stack_idx == len(self.document_stack)-1:
            print("already at the end of the stack cannot move forward anymore")
            return
            
        self.prev_stack_idx = self.stack_idx
        self.stack_idx += 1
        self.cur_doc = self.document_stack[self.stack_idx]
        print(f"Active Document: {self.cur_doc.doc.get_text()}")
        
    def prev(self):
        self.__check_valid_doc()
            
        tmp = self.prev_stack_idx
        self.prev_stack_idx = self.stack_idx
        self.stack_idx = tmp
        self.cur_doc = self.document_stack[self.stack_idx]
        print(f"Active Document: {self.cur_doc.doc.get_text()}")
        
    def set_stack_position(self, idx):
        if idx >= len(self.document_stack):
            raise ValueError(f"requested idx {idx} is outside the range of the document stack")
            
        self.cur_doc = self.document_stack[idx]
        print(f"Active Document: {self.cur_doc.doc.get_text()}")
        
    def goto_root(self):
        self.set_stack_position(0)
        
    def show_stack(self):
        for i in range(len(self.document_stack)):
            s = self.document_stack[i]
            print(f"{i} {s.doc.get_text()} -- children: [{len(s.children)}]")
                             
                
    def upscale(self, idx):
        self.__check_valid_doc()
        upscale_doc = QueryDocNode(self.cur_doc.doc.upscale(idx),self.cur_doc,[])
        self.cur_doc.add_child(upscale_doc)
        self.document_stack.append(upscale_doc)
        
        self.cur_doc = self.document_stack[-1]
        self.prev_stack_idx = self.stack_idx
        self.stack_idx = len(self.document_stack)-1
        self.show()
        
    def show_graph(self):
        root_node = self.document_stack[0]
        tab_n = 0
        
        def print_children(node, tab_n):
            if tab_n > 0:
                print(" ".join(["--"]*tab_n)+" "+node.doc.get_text())
            else:
                print(node.doc.get_text())
            for nn in node.children:
                print_children(nn,tab_n+1)
        
        print_children(root_node,0)
         
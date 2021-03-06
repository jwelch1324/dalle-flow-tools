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
from .database import QueryDatabase
from .document import QueryDocument

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
        
        
    def display_path(self):
        self.__check_valid_doc()
        stt = self.cur_doc.doc.get_text()
        idxs = list(map(lambda x: x.split('item')[1].strip('[').strip(']'), re.findall(r"item\[[0-9]*\]",stt)))
        idxs.reverse()
        with tempfile.TemporaryDirectory() as tdir:
            ofilepath = os.path.join(tdir,'tmp.png')
            parent = self.cur_doc.parent
            imgs = []
            for ii in idxs:
                parent.doc.da[int(ii)].save_uri_to_file(ofilepath)
                pp = PIL.Image.open(ofilepath)
                imgs.append(np.array(pp))
                parent = parent.parent
            imgs.reverse()

        fig, ax = plt.subplots(1,len(imgs),figsize=(40,40))
        for i in range(len(imgs)):
            ax[i].imshow(imgs[i])
            ax[i].set_axis_off()
        plt.show()
    
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
        
    def plot_graph(self):
        from diagrams import Diagram, Cluster
        from diagrams.custom import Custom
        import tempfile as tf
        
        with tf.TemporaryDirectory() as tdir:
            root_node = self.document_stack[0]
            root_node.doc.save_grid(os.path.join(tdir,"root.png"))
            with Diagram("Query: "+root_node.doc.get_text(), show=False, filename="fullgraph", direction="TB"):
                cc_root = Custom("root", os.path.join(tdir,"root.png"))

                #Now we need to iterate down through the graph creating images for each node
                def render_children(dn):
                    renders = []
                    for cc in dn.children:
                        fpath = os.path.join(tdir,cc.doc.get_hash()+".png")
                        cc.doc.save_grid(fpath)
                        renders.append(fpath)
                        crenders = render_children(cc) 
                        renders.append(crenders)
                    return renders
                
                rr = render_children(root_node)
                
                def connect_nodes(cctop, paths):
                    ##cctop is a diagram node for the top node of this level
                    # all non list objects connect directly to this node, all lists are subgraphs
                    ii = 0
                    while ii < len(paths):
                        tt = paths[ii]
                        ttc = paths[ii+1]
                        fhash, _ = os.path.splitext(os.path.basename(tt))
                        cc_z = Custom(str(self.__hash_pos_in_stack(fhash)),tt)
                        cctop >> cc_z
                        if len(ttc) > 0:
                            connect_nodes(cc_z,ttc)
                        ii += 2
                        
                connect_nodes(cc_root,rr)
#                 cc_grid = Custom("Grid", "./resources/grid0.png")
#                 cc_grid1 = Custom("Grid1", "./resources/grid0.png")
#                 cc_sample = Custom("Sample","./resources/sample0.png")
#                 cc_sample1 = Custom("Sampl1e","./resources/sample0.png")
#                 cc_sample2 = Custom("Sample2","./resources/sample0.png")
#                 cc_sample3 = Custom("Sample3","./resources/sample0.png")

#                 cc_grid1 >> cc_sample1
#                 cc_grid1 >> cc_sample2
#                 cc_grid1 >> cc_sample3
#                 cc_grid >> cc_sample
#                 cc_grid >> cc_grid1
         
    def __hash_pos_in_stack(self, phash):
        for i in range(len(self.document_stack)):
            if self.document_stack[i].doc.get_hash() == phash:
                return i
        return -1
    
    def to_bytes(self):
        import pickle
        # The idea here is we will pickle the root_node into bytes and save the current stack as a map of index to hash
        rootBytes = pickle.dumps(self.document_stack[0])
        
        docStackMap = {}
        
        for i in range(len(self.document_stack)):
            docStackMap[i] = self.document_stack[i].doc.get_hash()
        
        
        all_data = {
            'rootBytes':rootBytes,
            'stackMap':docStackMap,
        }
        allBytes = pickle.dumps(all_data)
        return allBytes
        
    def from_bytes(self, allBytes):
        import pickle
        
        def find_matching_doc(docHash, rdoc):
            if rdoc.doc.get_hash() == docHash:
                #print(f"Found Matching doc for hash {docHash}")
                return rdoc
            for cc in rdoc.children:
                zz = find_matching_doc(docHash, cc)
                if zz is None:
                    continue
                return zz
            return None
        
        all_data = pickle.loads(allBytes)
        rootBytes = all_data['rootBytes']
        dockStackMap = all_data['stackMap']
        loadErrors = False
        self.cur_doc = pickle.loads(rootBytes)
        for i in range(len(dockStackMap)):
            docPtr = find_matching_doc(dockStackMap[i], self.cur_doc)
            if docPtr is None:
                print(f"Could not find document with hash {docStackMap[i]}")
                loadErrors = True
            else:
                self.document_stack.append(docPtr)
        if loadErrors:
            print("Load finished with errors -- some documents may fail to render correctly")
        else:
            print("Load finished without errors")
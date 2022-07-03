# dalle-flow-tools
Set of tools that allow you to save and recall the results of dalle-flow sessions so that you can come back and explore a given session further.

# The data store and database
The querytools introduce two classes the QueryDocument() and QueryDatabase() -- the former is a wrapper around the dalle-flow docarray interface, and the latter is a simple SQLLite wrapper that manages storing session data. Additionally when the database is first instantiated it will create a datastore cache, which is an organized set of folders corresponding to the first four hex characters of an md5 hash. When a query document is saved to the database, it is converted to raw bytes, hashed, and then saved to the appropriate bucket in the datastore.

# First use of the Database
When you first create a database you need to run the `initdb` function as follows
```python
qdb = QueryDatabase()
qdb.initdb()
```

This is to create the main table in the database

# Saving / Restoring a query document
Lets say you have a query document that you want to save for later -- you can easily save it to the database using the `save_qd` function
```python
qd = QueryDocument()
qd.query('a photo of a happy puppy')
qdb.save_qd(qd)
```

Later when you want to recall the document you can use the database to find the query and rebuild the QueryDocument object
```python
qdb.show_queries()
#OUTPUT: 0 a photo of a happy puppy
```
we see that there is a stored session with the query `a photo of a happy puppy` in the database at index 0, to rebuild the document for this we just need to pass the hash to the `rebuild_doc` function
```python
#If the dalle-flow-endpoint variable is not specified it will use the default which is grpc://10.10.28.110:51005 -- you can set the default in the querytools.py file
rqd = qdb.rebuild_doc(qdb.get_hash_from_list(3),dalle_flow_endpoint="grpc://10.10.28.110:51005")
```

now `rqd` will be the same QueryDocument object as before, and you can continue to operate on the session as normal


# Show Tiles
if you run the `show_tiles` function you will get a plot of all the current images with their index in the current query doc.

# Diffusing
There is a function `diffuse` which takes as its arguments `skip_rate` and `idx` (idx is optional, and defaults to 0). This will run a diffusion on the image at index `idx` within the current document and return a new QueryDocument that contains the session information for the diffusion operation. It will also append the line `diffusion idx[#] sr[skip_rate]` to the text entry of each item in the document so that if you save it to the database you will see an extended version of the original query to make it easier to differentate it from the original doc.


# TODO 
Many things to do probably -- but the most immediate one would be adding simple CRUD like operations -- in particular removing entries that we no longer want. 





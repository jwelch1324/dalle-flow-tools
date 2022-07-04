# Query Database Usage

## Database Initialization
The first time you use the database you need to initialize it
```python
from dalle_sessions.database import QueryDatabase
qdb = QueryDatabase()
qdb.initdb()
```

The `QueryDatabase` constructor will create a datastore if you do not currently have one, the default location is within the current folder inside a subfolder called `db_datastore` -- you can change the location in the `QueryDatabase` constructor. The `initdb` function will create a `queries.db` sqlite database in the current folder with the default tables. You can also change the location by changing the database arugment in the `QueryDatabase` constructor.

## Saving Session Data to the database

See [Basic QuerySession Usage](BasicQuerySession.md) for the basics of creating a session. See [Navigating a Session Graph](NavigatingSession.md) for details on how to navigate through a session object.

### `save_session` and `load_session` 
The `QueryDatabase` class provides the utility for saving and loading session data in the database. The database is mainly just to store simple metadata, the actual session object is stored as a pickled binary file in the data store. When calling `save_session` the entire session from the root node down will be pickled and an MD5 hash of the bytes is computed. A file in the datastore which has the hash as the name is created and the bytes are written there. In the database side, a string naming the session is tied to that hash -- the session name must be unique in the database, otherwise an error will be thrown. If you wish to overwrite an old session with a new one using the same name, use `replace_session` instead. 

```python
from dalle_sessions.database import QueryDatabase
from dalle_sessions.session import QuerySession
qdb = QueryDatabase()
s = QuerySession(qdb)
s.query('a photo of an adorable kitten')
### other operations on graph

#Now we can display the current graph
s.show_graph()
```
    a photo of an adorable kitten
    -- a photo of an adorable kitten -- diffuse item[5] sr[0.6]
    -- -- a photo of an adorable kitten -- diffuse item[5] sr[0.6] -- upscale item[0]


To save our session we just call `save_session` with a unique name

```python
qdb.save_session('adorable_kitten',s)
```

    Session adorable_kitten saved to database
    

Now that the session is in the database we can see it when listing the available saved sessions
```python
qdb.show_sessions()
```

    1: adorable_kitten

```python
newS = qdb.load_session('adorable_kitten')
```

    {0: 'db17dfa85e6a7d4fde54e75ada41513a', 1: 'aeb8e81a3e2900dba6602db9f3c3fd15', 2: 'f481696c0a562a2d3f98a1a2723f6312'}
    Found Matching doc for hash db17dfa85e6a7d4fde54e75ada41513a
    Found Matching doc for hash aeb8e81a3e2900dba6602db9f3c3fd15
    Found Matching doc for hash f481696c0a562a2d3f98a1a2723f6312


What is shown here is the output of the deserializer searching for subdocuments in the graph and indicating if they were found or not. Now `newS` contains the entire graph that was saved eariler, and you can continue to experiment and explore the query space.

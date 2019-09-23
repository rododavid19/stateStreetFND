from pymongo import MongoClient
import os
import hashlib
import datetime
import pprint
import numpy as np

client = MongoClient()
db = client.test_Database
posts = db.posts
multiPost = [{
    "author": "Teo",
    "text": "Teo's first blog post!",
    "tags": ["mongodb", "python", "pymongo"],
    "date": datetime.datetime.utcnow()},
    {"author": "Julia",
     "text": "Julia's first blog post!",
     "tags": ["mongodb", "python", "pymongo"],
     "date": datetime.datetime.utcnow()},
    {"author": "Rodo",
     "text": "Rodo's first blog post!",
     "tags": ["mongodb", "python", "pymongo"],
     "date": datetime.datetime.utcnow()},
    {"author": "Saras",
     "text": "Saras' first blog post!",
     "tags": ["mongodb", "python", "pymongo"],
     "date": datetime.datetime.utcnow()},
    {"author": "Brian",
     "text": "Brian's first blog post!",
     "tags": ["mongodb", "python", "pymongo"],
     "date": datetime.datetime.utcnow()}
]
posts.insert_many(multiPost)

x = posts.delete_many({})

print(x.deleted_count, " documents deleted.")

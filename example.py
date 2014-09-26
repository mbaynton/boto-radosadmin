#!/usr/bin/python
from __future__ import print_function
import sys
import pwd
from rados.connection import CephConnection
import json

newUser = sys.argv[1]

my_radosgw_server = 's3.my.org'
my_access_key = 'MYACCESSKEYHERE'
my_secret_key = 'MYSECRETKEYHERE'

# already an s3 user?
conn = CephConnection(my_radosgw_server, my_access_key, my_secret_key, True)
existingUser = conn.getUser(newUser)
if(existingUser):
    print("User already present, not created.")
    sys.exit(0)

# cool, create them.
newUser = conn.createUser(uid=newUser, displayName=newUser)
print(json.dumps(newUser))


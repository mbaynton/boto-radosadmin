# Copyright 2014 by the Regents of the University of Minnesota
# Written by Mike Baynton <mike@mbaynton.com>
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# The Minnesota Supercomputing Institute http://www.msi.umn.edu sponsored
# the development of this software.
#
from boto.s3.connection import S3Connection, SubdomainCallingFormat
from boto.s3.bucket import Bucket
from boto.connection import AWSAuthConnection
from boto.exception import StorageResponseError
import urllib
from SOAPpy.Types import bodyType
try:
    import simplejson as json
except ImportError:
    import json

class CephConnection(S3Connection):
    def __init__(self, host, aws_access_key_id=None, aws_secret_access_key=None,
                 is_secure=True, admin_endpoint='/admin/', port=None,
                 proxy=None, proxy_port=None, proxy_user=None, proxy_pass=None, debug=0,
                 https_connection_factory=None,
                 calling_format=SubdomainCallingFormat(), path='/',
                 provider='aws', bucket_class=Bucket, security_token=None,
                 suppress_consec_slashes=True, anon=False):
        self.admin_endpoint = admin_endpoint
        S3Connection.__init__(self,
                aws_access_key_id, aws_secret_access_key,
                is_secure, port, proxy, proxy_port, proxy_user, proxy_pass,
                host,
                debug=debug, https_connection_factory=https_connection_factory,
                calling_format=calling_format,
                path=path, provider=provider, bucket_class=bucket_class,
                security_token=security_token, suppress_consec_slashes=suppress_consec_slashes,
                anon=anon)


    def getUser(self, uid=None):
        auth_param = ''
        if uid != None:
            auth_param = '&' + urllib.urlencode({'uid': uid})
        response = AWSAuthConnection.make_request(self, 'GET', self.admin_endpoint + 'user?format=json' + auth_param, None, '')
        body = response.read()
        if response.status == 200:
            return json.loads(body)
        elif response.status == 404: # no such user
            return None
        else:
            raise StorageResponseError(response.status, response.reason, body)

    # Including number of objects and used kilobytes, measured in same way as user quota enforcement.
    def getUserStats(self, uid):
        parameters = {'uid': uid}

        response = AWSAuthConnection.make_request(self, 'GET', self.admin_endpoint + 'user?stats&format=json&' + urllib.urlencode(parameters), data=urllib.urlencode(parameters))
        body = response.read()
        if response.status == 200:
            return json.loads(body)
        else:
            raise StorageResponseError(response.status, response.reason, body)


    def createUser(self, uid, displayName, email=None, key_type='s3',
            access_key=None, secret_key=None, user_caps=None,
            generate_key=True, max_buckets=None, suspended=False):
        parameters = {'uid': uid, 'display-name': displayName}
        authParameters = {'uid': uid, 'display-name': displayName}

        if email is not None:
            parameters['email'] = email
            authParameters['email'] = email
        if key_type is not None:
            parameters['key-type'] = key_type
        if access_key is not None:
            parameters['access-key'] = access_key
        if secret_key is not None:
            parameters['secret-key'] = secret_key
        if user_caps is not None:
            parameters['user-caps'] = user_caps
        if max_buckets is not None:
            parameters['max-buckets'] = max_buckets
        parameters['generate-key'] = generate_key
        parameters['suspended'] = suspended

        response = AWSAuthConnection.make_request(self, 'PUT', self.admin_endpoint + 'user?format=json&' + urllib.urlencode(authParameters), data=urllib.urlencode(parameters))
        body = response.read()
        if response.status == 200:
            return json.loads(body)
        else:
            raise StorageResponseError(response.status, response.reason, body)

    def modifyUser(self, uid, displayName=None, email=None, key_type='s3',
            access_key=None, secret_key=None, user_caps=None,
            generate_key=False, max_buckets=None, suspended=False):
        parameters = {'uid': uid}
        authParameters = {'uid': uid}

        if displayName is not None:
            parameters['display-name'] = displayName
        if email is not None:
            parameters['email'] = email
        if key_type is not None:
            parameters['key-type'] = key_type
        if access_key is not None:
            parameters['access-key'] = access_key
        if secret_key is not None:
            parameters['secret-key'] = secret_key
        if user_caps is not None:
            parameters['user-caps'] = user_caps
        if max_buckets is not None:
            parameters['max-buckets'] = max_buckets
        parameters['generate-key'] = generate_key
        parameters['suspended'] = suspended

        response = AWSAuthConnection.make_request(self, 'POST', self.admin_endpoint + 'user?format=json&' + urllib.urlencode(parameters), data=urllib.urlencode(parameters)) # because, it seems to need the params of this POST request in the URL query string...
        body = response.read()
        if response.status == 200:
            return json.loads(body)
        else:
            raise StorageResponseError(response.status, response.reason, body)


    def createKey(self, uid, key_type='s3', access_key=None, secret_key=None, generate_key=True ):
        parameters = {'uid': uid, 'key-type': key_type, 'generate-key': generate_key}
        if access_key is not None:
            parameters['access-key'] = access_key
        if secret_key is not None:
            parameters['secret-key'] = secret_key

        response = AWSAuthConnection.make_request(self, 'PUT', self.admin_endpoint + 'user?key&format=json&' + urllib.urlencode(parameters), data=urllib.urlencode(parameters))
        body = response.read()
        if response.status == 200:
            return json.loads(body)
        else:
            raise StorageResponseError(response.status, response.reason, body)


    def removeKey(self, access_key, uid=None, key_type='s3'):
        parameters = {'access-key': access_key, 'key-type': key_type}
        if uid is not None:
            parameters['uid'] = uid

        response = AWSAuthConnection.make_request(self, 'DELETE', self.admin_endpoint + 'user?key&format=json&' + urllib.urlencode(parameters), data=urllib.urlencode(parameters))
        body = response.read()
        if response.status == 200:
            return True
        else:
            raise StorageResponseError(response.status, response.reason, body)

    def getBucketInfo(self, bucket=None, uid=None, stats=None):
        parameters = {}
        if bucket is not None:
            parameters['bucket'] = bucket
        if uid is not None:
            parameters['uid'] = uid
        if stats is not None:
            parameters['stats']= stats

        response = AWSAuthConnection.make_request(self, 'GET', self.admin_endpoint + 'bucket?format=json&' + urllib.urlencode(parameters), data=urllib.urlencode(parameters))
        body = response.read()
        if response.status == 200:
            return json.loads(body)
        else:
            raise StorageResponseError(response.status, response.reason, body)

    def unlinkBucket(self, bucket, uid):
        parameters = {'bucket': bucket, 'uid': uid}

        response = AWSAuthConnection.make_request(self, 'POST', self.admin_endpoint + 'bucket?format=json&' + urllib.urlencode(parameters), data=urllib.urlencode(parameters))
        body = response.read()
        if response.status == 200:
            return True
        else:
            raise StorageResponseError(response.status, response.reason, body)

    def linkBucket(self, bucket, bucket_id, uid):
        parameters = {'bucket': bucket, 'bucket-id': bucket_id, 'uid': uid}

        response = AWSAuthConnection.make_request(self, 'PUT', self.admin_endpoint + 'bucket?format=json&' + urllib.urlencode(parameters), data=urllib.urlencode(parameters))
        body = response.read()
        if response.status == 200:
            return True
        else:
            # print (response.reason + " || " + body)
            raise StorageResponseError(response.status, response.reason, body)

    def getQuota(self, uid, quota_type):
        parameters = {'uid': uid, 'quota-type': quota_type}

        response = AWSAuthConnection.make_request(self, 'GET', self.admin_endpoint + 'user?quota&format=json&' + urllib.urlencode(parameters), data=urllib.urlencode(parameters))
        body = response.read()
        if response.status == 200:
            return json.loads(body)
        else:
            raise StorageResponseError(response.status, response.reason, body)

    def setQuota(self, uid, quota_type, max_size_kb, max_objects=-1, enabled=True):
        parameters = {'uid': uid, 'quota-type': quota_type}
        body_parameters = {'max_objects': max_objects, 'max_size_kb': max_size_kb, 'enabled': enabled}

        response = AWSAuthConnection.make_request(self, 'PUT', self.admin_endpoint + 'user?quota&format=json&' + urllib.urlencode(parameters), data=json.dumps(body_parameters))
        body = response.read()
        if response.status == 200:
            return body
        else:
            raise StorageResponseError(response.status, response.reason, body)



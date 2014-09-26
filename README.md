Offers some python methods for the administration of ceph S3 users by calling the [radosgw adminops API](http://ceph.com/docs/master/radosgw/adminops/).

Similar in concept to [dyarnell/rgwadmin](https://github.com/dyarnell/rgwadmin), but extends [boto](http://aws.amazon.com/sdk-for-python/) for message signing, connection handling, etc.

Requires boto but has no additional requirements or dependencies beyond boto's. Most Linux distros seem to have python-boto packages.

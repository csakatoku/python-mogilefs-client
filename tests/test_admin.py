# -*- coding: utf-8 -*-
from mogilefs import Client, Admin, MogileFSError

DOMAIN   = "testdomain"
TRACKERS = ["127.0.0.1:7001"]

def test_create_domain():
    moga = Admin(TRACKERS)
    assert moga.create_domain(DOMAIN)
    try:
        moga.create_domain(DOMAIN)
    except MogileFSError:
        # domain exists
        pass
    else:
        assert False, "the domain %s should exist" % DOMAIN
    assert moga.delete_domain(DOMAIN)

def test_delete_domain():
    moga = Admin(TRACKERS)
    try:
        moga.delete_domain(DOMAIN)
    except MogileFSError:
        # domain does not exist
        pass
    else:
        assert False, "the domain %s should not exist" % DOMAIN
    assert moga.create_domain(DOMAIN)
    assert moga.delete_domain(DOMAIN)


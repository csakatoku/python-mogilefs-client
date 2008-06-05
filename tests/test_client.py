# -*- coding: utf-8 -*-
from mogilefs import Client

DOMAIN   = "gumi"
TRACKERS = ["127.0.0.1:7001"]

def test_do_request():
    client = Client(domain=DOMAIN, trackers=TRACKERS)
    """
    try:
        client.do_request()
    except ValueError:
        pass
    except Exception, e:
        assert False, "ValueError expected, actual %r" % e
    else:
        assert False
        """

# -*- coding: utf-8 -*-
import httplib

__all__ = ['MogileFSError', 'MogileFSHTTPError', 'MogileFSTrackerError']

class MogileFSError(Exception):
    pass

class MogileFSTrackerError(MogileFSError):
    def __init__(self, errstr, err=None):
        self.errstr = errstr
        self.err = err

    def __str__(self):
        return self.errstr

class MogileFSHTTPError(MogileFSError):
    def __init__(self, code, headers, content):
        self.code = code
        self.headers = headers
        self.content = content

    def __str__(self):
        return 'HTTP Error %d: %s' % (self.code, httplib.response[self.code])

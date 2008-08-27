# -*- coding: utf-8 -*-
import logging
import httplib2
from httplib import REQUESTED_RANGE_NOT_SATISFIABLE
from cStringIO import StringIO

from mogilefs.exceptions import MogileFSHTTPError, MogileFSTrackerError

logger = logging

def _complain_ifclosed(closed):
    if closed:
        raise ValueError("I/O operation on closed file")

def _complain_ifreadonly(readonly):
    if readonly:
        raise ValueError("operation on read-only file")

class HttpResponse(object):
    def __init__(self, headers, content):
        self.headers = headers
        self.content = content

    def is_success(self):
        status = self.get_status()
        return status >= 200 and status < 300

    def get_status(self):
        return int(self.headers['status'])
    status = property(get_status)

    def get_content_length(self):
        try:
            return long(self.headers['content-length'])
        except (KeyError, ValueError, TypeError):
            return 0
    content_length = property(get_content_length)

class HttpFile(object):
    def __init__(self, mg, fid, key, cls, create_close_arg=None):
        self.mg = mg
        self.fid = fid
        self.key = key
        self.cls = cls
        self.create_close_arg = create_close_arg or {}
        self.conn = httplib2.Http()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        if not self._closed:
            self.close()

    def __del__(self):
        if not self._closed:
            try:
                self.close()
            except Exception, e:
                logger.debug("got an exception in __del__: %s" % str(e))

    def _request(self, path, method, *args, **kwds):
        headers, content = self.conn.request(path, method, *args, **kwds)
        res = HttpResponse(headers, content)
        if not res.is_success():
            raise MogileFSHTTPError(res.status, res.headers, res.content)
        return res

class ClientHttpFile(HttpFile):
    def __init__(self, path, backup_dests=None, overwrite=False,
                 mg=None, fid=None, devid=None, cls=None, key=None, readonly=False, create_close_arg=None, **kwds):

        super(ClientHttpFile, self).__init__(mg, fid, key, cls, create_close_arg)

        for tried_devid, tried_path in [(devid, path)] + list(backup_dests):
            self._path = tried_path

            if overwrite:
                # Ensure file overwritten/created, even if they don't print anything
                res = self._request(tried_path, "PUT", "", headers={"Content-Length": "0"})
            else:
                res = self._request(tried_path, "HEAD")

            if res.is_success():
                if overwrite:
                    self.length = 0
                else:
                    self.length = res.content_length

                self.devid = tried_devid
                self.path  = tried_path
                break
        else:
            raise MogileFSHTTPError("couldn't connect to any storage nodes")

        self.overwrite = overwrite
        self.readonly = readonly

        self._closed = 0
        self._pos    = 0
        self._eof    = 0

    def read(self, n=-1):
        _complain_ifclosed(self._closed)

        if self._eof:
            return ''

        headers = {}
        if n == 0:
            return ''
        elif n > 0:
            headers['Range'] = 'bytes=%d-%d' % (self._pos, self._pos + n - 1)
        else:
            # if n is negative, then read whole content
            pass

        try:
            res = self._request(self._path, "GET", headers=headers)
        except MogileFSHTTPError, e:
            if e.code == REQUESTED_RANGE_NOT_SATISFIABLE:
                self._eof = 1
                return ''
            else:
                raise e

        content = res.content
        self._pos += len(content)

        if n < 0:
            self._eof = 1

        return content

    def readline(self, length=None):
        raise NotImplementedError()

    def readlines(self, sizehint=0):
        raise NotImplementedError()

    def write(self, content):
        _complain_ifclosed(self._closed)
        _complain_ifreadonly(self.readonly)

        length = len(content)
        start = self._pos
        end   = self._pos + length - 1
        headers = { 'Content-Range': "bytes %d-%d/*" % (start, end),
                    }
        res = self._request(self._path, "PUT", content, headers=headers)

        if self._pos + length > self.length:
            self.length = self._pos + length

        self._pos += len(content)

    def close(self):
        if not self._closed:
            self._closed = 1
            if self.devid:
                params = { 'fid'   : self.fid,
                           'devid' : self.devid,
                           'domain': self.mg.domain,
                           'size'  : self.length,
                           'key'   : self.key,
                           'path'  : self.path,
                           }
                if self.create_close_arg:
                    params.update(self.create_close_arg)
                try:
                    self.mg.backend.do_request('create_close', params)
                except MogileFSTrackerError, e:
                    if e.err != 'empty_file':
                        raise

    def seek(self, pos, mode=0):
        _complain_ifclosed(self._closed)
        if pos < 0:
            pos = 0
        self._pos = pos

    def tell(self):
        _complain_ifclosed(self._closed)
        return self._pos

class NewHttpFile(HttpFile):
    def __init__(self, path, devid, backup_dests=None,
                 mg=None, fid=None, cls=None, key=None, create_close_arg=None, **kwds):

        super(NewHttpFile, self).__init__(mg, fid, key, cls, create_close_arg)

        self._fp = StringIO()
        self._paths = [(devid, path)] + list(backup_dests)
        self._closed = 0

    def read(self, n=-1):
        return self._fp.read(n)

    def readline(self, *args, **kwds):
        return self._fp.readline(*args, **kwds)

    def readlines(self, *args, **kwds):
        return self._fp.readlines(*args, **kwds)

    def write(self, content):
        self._fp.write(content)

    def close(self):
        if not self._closed:
            self._closed = 1

            content = self._fp.getvalue()
            self._fp.close()

            for tried_devid, tried_path in self._paths:
                try:
                    res = self._request(tried_path, "PUT", content)
                    devid = tried_devid
                    path  = tried_path
                    break
                except MogileFSHTTPError, e:
                    continue
            else:
                devid = None
                path  = None

            if devid:
                params = { 'fid'   : self.fid,
                           'domain': self.mg.domain,
                           'key'   : self.key,
                           'path'  : path,
                           'devid' : devid,
                           'size'  : len(content),
                           }
                if self.create_close_arg:
                    params.update(self.create_close_arg)
                try:
                    self.mg.backend.do_request('create_close', params)
                except MogileFSTrackerError, e:
                    if e.err != 'empty_file':
                        raise

    def seek(self, pos, mode=0):
        return self._fp.seek(pos, mode)

    def tell(self):
        return self._fp.tell()

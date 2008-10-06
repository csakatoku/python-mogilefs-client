# -*- coding: utf-8 -*-
import logging

from mogilefs.backend import Backend
from mogilefs.exceptions import MogileFSError, MogileFSTrackerError
from mogilefs.http import NewHttpFile, ClientHttpFile

logger = logging

def _complain_ifreadonly(readonly):
    if readonly:
        raise ValueError("operation on read-only client")

class Client(object):
    def __init__(self, domain, hosts, timeout=3, backend=None, readonly=False, hooks=None):
        self.readonly = bool(readonly)
        self.domain   = domain
        self.backend  = Backend(hosts, timeout)

    def run_hook(self, hookname, *args):
        pass

    def add_hook(self, hookname, *args):
        pass

    def get_last_tracker(self):
        """
        Returns a tuple of (ip, port), representing the last mogilefsd
        'tracker' server which was talked to.
        """
        return self.backend.get_last_tracker()
    last_tracker = property(get_last_tracker)

    def new_file(self, key, cls=None, bytes=0, largefile=False, create_open_arg=None, create_close_arg=None, opts=None):
        """
        - class
        - key
        - fid
        - largefile
        - create_open_arg
        - create_close_arg
        """
        self.run_hook('new_file_start', key, cls, opts)

        create_open_arg = create_open_arg or {}
        create_close_arg = create_close_arg or {}

        # fid should be specified, or pass 0 meaning to auto-generate one
        fid = 0
        params = { 'domain'    : self.domain,
                   'key'       : key,
                   'fid'       : fid,
                   'multi_dest': 1,
                   }
        if cls is not None:
            params['class'] = cls
        res = self.backend.do_request('create_open', params)
        if not res:
            raise IOError()

        # [ (devid,path), (devid,path), ... ]
        dests = []
        # determine old vs. new format to populate destinations
        if 'dev_count' not in res:
            dests.append((res['devid'], res['path']))
        else:
            for x in xrange(1, int(res['dev_count']) + 1):
                devid_key = 'devid_%d' % x
                path_key = 'path_%s' % x
                dests.append((res[devid_key], res[path_key]))

        main_dest = dests[0]
        main_devid, main_path = main_dest

        # TODO
        # create a MogileFS::NewHTTPFile object, based off of IO::File
        if not main_path.startswith('http://'):
            raise MogileFSError("This version of mogilefs.client no longer supports non-http storage URLs.")

        self.run_hook("new_file_end", key, cls, opts)

        # TODO
        if largefile:
            file_cls = ClientHttpFile
        else:
            file_cls = NewHttpFile

        return file_cls(mg=self,
                        fid=res['fid'],
                        path=main_path,
                        devid=main_devid,
                        backup_dests=dests,
                        cls=cls,
                        key=key,
                        content_length=bytes,
                        create_close_arg=create_close_arg,
                        overwrite=1,
                        )

    def edit_file(self, key, **opts):
        """
        EdiCt the file with the the given key.

        B<NOTE:> edit_file is currently EXPERIMENTAL and not recommended for
        production use. MogileFS is primarily designed for storing files
        for later retrieval, rather than editing.  Use of this function may lead to
        poor performance and, until it has been proven mature, should be
        considered to also potentially cause data loss.

        B<NOTE:> use of this function requires support for the DAV 'MOVE'
        verb and partial PUT (i.e. Content-Range in PUT) on the back-end
        storage servers (e.g. apache with mod_dav).

        Returns a seekable filehandle you can read/write to. Calling this
        function may invalidate some or all URLs you currently have for this
        key, so you should call ->get_paths again afterwards if you need
        them.

        On close of the filehandle, the new file contents will replace the
        previous contents (and again invalidate any existing URLs).

        By default, the file contents are preserved on open, but you may
        specify the overwrite option to zero the file first. The seek position
        is at the beginning of the file, but you may seek to the end to append.
        """
        _complain_ifreadonly(self.readonly)
        res = self.backend.do_request('edit_file',
                                      { 'domain': self.domain,
                                        'key'   : key,
                                        })
        oldpath = res['oldpath']
        newpath = res['newpath']
        fid     = res['fid']
        devid   = res['devid']
        cls     = res['class']

        ## TODO
        import httplib2
        conn = httplib2.Http()
        res, content = conn.request(oldpath,
                                    "MOVE",
                                    headers={ 'Destination': newpath,
                                              })
        status = int(res['status'])
        if not (res.status >= 200 and res.status < 300):
            raise IOError("failed to MOVE %s to %s" % (newpath, oldpath))

        return ClientHttpFile(mg=self, path=newpath, fid=fid, devid=devid, cls=cls,
                              key=key, overwrite=opts.get('overwrite'))

    def read_file(self, *args, **kwds):
        paths = self.get_paths(*args, **kwds)
        path = paths[0]
        backup_dests = [(None, p) for p in paths[1:]]
        return ClientHttpFile(path=path, backup_dests=backup_dests, readonly=1)

    def get_paths(self, key, noverify=1, zone='alt', pathcount=None):
        self.run_hook('get_paths_start', key)

        extra_params = {}
        params = { 'domain'  : self.domain,
                   'key'     : key,
                   'noverify': noverify and 1 or 0,
                   'zone'    : zone,
                   }
        params.update(extra_params)
        try:
            res = self.backend.do_request('get_paths', params)
            paths = [res["path%d" % x] for x in xrange(1, int(res["paths"])+1)]
        except MogileFSTrackerError, e:
            if e.err == 'unknown_key':
                paths = []
            else:
                raise e

        self.run_hook('get_paths_end', key)
        return paths

    def get_file_data(self, key, timeout=10):
        """
        given a key, returns a string containing the contents of the file.
        TODO:
          - supports timeout
        """
        fp = self.read_file(key, noverify=1)
        try:
            content = fp.read()
            return content
        finally:
            fp.close()

    def rename(self, from_key, to_key):
        _complain_ifreadonly(self.readonly)
        self.backend.do_request('rename',
                                { 'domain'  : self.domain,
                                  'from_key': from_key,
                                  'to_key'  : to_key,
                                  })
        return True

    def list_keys(self, prefix=None, after=None, limit=None):
        params = { 'domain': self.domain,
                   }
        if prefix:
            params['prefix'] = prefix
        if after:
            params['after'] = after
        if limit:
            params['limit'] = limit

        res = self.backend.do_request('list_keys', params)
        resafter = res['next_after']
        reslist = []
        for x in xrange(1, int(res['key_count'])+1):
            reslist.append(res['key_%d' % x])
        return reslist

    def foreach_key(self, *args, **kwds):
        raise NotImplementedError()

    def sleep(self, duration):
        """
        just makes some sleeping happen.  first and only argument is number of
        seconds to instruct backend thread to sleep for.
        """
        self.backend.do_request("sleep",
                                { 'duration': duration,
                                  })
        return True

    def set_pref_ip(self, *ips):
        """
        Weird option for old, weird network architecture.  Sets a mapping
        table of preferred alternate IPs, if reachable.  For instance, if
        trying to connect to 10.0.0.2 in the above example, the module would
        instead try to connect to 10.2.0.2 quickly first, then then fall back
        to 10.0.0.2 if 10.2.0.2 wasn't reachable.
        expects as argument a tuple of ("standard-ip", "preferred-ip")
        """
        self.backend.set_pref_ip(*ips)

    def store_file(self, key, fp, cls=None, **opts):
        """
        Wrapper around new_file, print, and close.

        Given a key, class, and a filehandle or filename, stores the file
        contents in MogileFS.  Returns the number of bytes stored on success,
        undef on failure.
        """
        _complain_ifreadonly(self.readonly)

        self.run_hook('store_file_start', key, cls, opts)

        try:
            output = self.new_file(key, cls, largefile=1, **opts)
            bytes = 0
            while 1:
                buf = fp.read(1024 * 16)
                if not buf:
                    break
                bytes += len(buf)
                output.write(buf)

            self.run_hook('store_file_end', key, cls, opts)
        finally:
            # finally
            fp.close()
            output.close()

        return bytes

    def store_content(self, key, content, cls=None, **opts):
        """
        Wrapper around new_file, print, and close.  Given a key, class, and
        file contents (scalar or scalarref), stores the file contents in
        MogileFS. Returns the number of bytes stored on success, undef on
        failure.
        """
        _complain_ifreadonly(self.readonly)

        self.run_hook('store_content_start', key, cls, opts)

        output = self.new_file(key, cls, None, **opts)
        try:
            output.write(content)
        finally:
            output.close()

        self.run_hook('store_content_end', key, cls, opts)

        return len(content)

    def delete(self, key):
        _complain_ifreadonly(self.readonly)

        self.backend.do_request('delete',
                                { 'domain': self.domain,
                                  'key'   : key,
                                  })
        return True

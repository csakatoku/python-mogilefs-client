# -*- coding: utf-8 -*-
import logging

from mogilefs.exceptions import MogileFSTrackerError
from mogilefs.backend import Backend

logger = logging

def _complain_ifreadonly(readonly):
    if readonly:
        raise ValueError('the operation is not allowed')

class Admin(object):
    def __init__(self, hosts, backend=None, readonly=False, timeout=None, hooks=None):
        self.readonly = bool(readonly)
        self.backend = Backend(hosts, timeout)
        self._hosts = hosts

    def replicate_row(self):
        self.backend.do_request("replicate_row")

    def get_hosts(self, hostid=None):
        if hostid:
            params = { 'hostid': hostid }
        else:
            params = None
        res = self.backend.do_request("get_hosts", params)

    def get_devices(self, devid=None):
        if devid:
            params = { 'devid': devid }
        else:
            params = None
        res = self.backend.do_request("get_devices", params)
        ret = []
        for x in xrange(1, int(res['devices'])+1):
            device = {}
            for k in ('devid', 'hostid', 'status', 'observed_state', 'utilization'):
                device[k] = res.get('dev%d_%s' % (x, k))

            for k in ('mb_total', 'mb_used', 'weight'):
                value = res.get('dev%d_%s' % (x, k))
                if value:
                    device[k] = int(value)
                else:
                    device[k] = None
            ret.append(device)
        return ret

    def list_fids(self, fromfid, tofid):
        """
        get raw information about fids, for enumerating the dataset
           ( from_fid, to_fid )
        returns:
           { fid => { dict with keys: domain, class, devcount, length, key } }
        """
        res = self.backend.do_request('list_fids',
                                      { 'from': fromfid,
                                        'to'  : tofid,
                                        })
        ret = {}
        for x in xrange(1, int(res['fid_count'])+1):
            key = 'fid_%d_fid' % x
            ret[key] = dict([(k, res['fid_%d_%s' % (x, k)]) for k in ('key', 'length', 'class', 'domain', 'devcount')])
        return ret

    def clear_cache(self, fromfid, tofid):
        params = {}
        res = self.backend.do_request('clear_cache', params)

    def get_stats(self):
        params = { 'all': 1 }
        res = self.backend.do_request('stats', params)

        ret = {}
        # get replication statistics
        if 'replicationcount' in res:
            replication = ret.setdefault('replication', {})
            for x in xrange(1, int(res['replicationcount'])+1):
                domain = res.get('replication%ddomain' % x, '')
                cls = res.get('replication%dclass' % x, '')
                devcount = res.get('replication%ddevcount' % x, '')
                fields = res.get('replication%dfields' % x)
                (replication.setdefault(domain, {}).setdefault(cls, {}))[devcount] = fields

        # get file statistics
        if 'filescount' in res:
            files = ret.setdefault('files', {})
            for x in xrange(1, int(res['filescount'])+1):
                domain = res.get('files%ddomain' % x, '')
                cls = res.get('files%dclass' % x, '')
                (files.setdefault(domain, {}))[cls] = res.get('files%dfiles' % x)

        # get device statistics
        if 'devicescount' in res:
            devices = ret.setdefault('devices', {})
            for x in xrange(1, int(res['devicescount'])+1):
                key = res.get('devices%did' % x, '')
                devices[key] = { 'host'  : res.get('devices%dhost' % x),
                                 'status': res.get('devices%dstatus' % x),
                                 'files' : res.get('devices%dfiles' % x),
                                 }

        if 'fidmax' in res:
            ret['fids'] = { 'max': res['fidmax'],
                            }

        # return the created response
        return ret

    def get_domains(self):
        """
        get a dict of the domains we know about in the format of
           { domain_name : { class_name => mindevcount, class_name => mindevcount, ... }, ... }
        """
        res = self.backend.do_request('get_domains')
        ## KeyError, ValueError, TypeError
        domain_length = int(res['domains'])
        ret = {}
        for x in xrange(1, domain_length+1):
            domain_name = res['domain%d' % x]
            ret.setdefault(domain_name, {})
            class_length = int(res['domain%dclasses' % x])
            for y in xrange(1, class_length+1):
                k = 'domain%dclass%dname' % (x, y)
                v = 'domain%dclass%dmindevcount' % (x, y)
                ret[domain_name][res[k]] = int(res[v])
        return ret

    def create_domain(self, domain):
        """
        create a new domain
        """
        if self.readonly:
            return
        res = self.backend.do_request('create_domain',
                                      { 'domain': domain })
        return res['domain'] == domain

    def delete_domain(self, domain):
        """
        delete a domain
        """
        _complain_ifreadonly(self.readonly)
        res = self.backend.do_request('delete_domain',
                                      { 'domain': domain })
        return res['domain'] == domain

    def create_class(self, domain, cls, mindevcount):
        """
        create a class within a domain
        """
        try:
            return self._modify_class('create', domain, cls, mindevcount)
        except MogileFSTrackerError, e:
            if e.err != 'class_exists':
                raise e

    def update_class(self, domain, cls, mindevcount):
        """
        update a class's mindevcount within a domain
        """
        return self._modify_class('update', domain, cls, mindevcount)

    def delete_class(self, domain, cls):
        """
        delete a class
        """
        _complain_ifreadonly(self.readonly)
        res = self.backend.do_request("delete_class",
                                      { 'domain': domain,
                                        'class' : cls,
                                        })
        return res['class'] == cls

    def create_host(self, host, ip, port, status=None):
        params = { 'host': host,
                   'ip'  : ip,
                   'port': port,
                   }
        if status:
            params['status'] = status
        return self._modify_host('create', params)

    def update_host(self, host, ip=None, port=None, status=None):
        params = { 'host': host,
                   }
        if ip:
            params['ip'] = ip
        if port:
            params['port'] = port
        if status:
            params['status'] = status
        return self._modify_host('update', params)

    def delete_host(self, host):
        self.backend.do_request("delete_host", { 'host': host })

    def create_device(self, hostname, devid, hostip=None, state=None):
        params = { 'hostname': hostname,
                   'devid'   : devid,
                   }
        if hostip:
            params['hostip'] = hostip
        if state:
            params['state'] = state
        res = self.backend.do_request('create_device', params)

    def update_device(self, host, device, status=None, weight=None):
        if status:
            self.change_device_state(host, device, status)

        if weight:
            self.change_device_weight(host, device, weight)

    def change_device_state(self, host, device, state):
        """
        change the state of a device; pass in the hostname of the host the
        device is located on, the device id number, and the state you want
        the host to be set to.
        """
        res = self.backend.do_request('set_state',
                                      { 'host'  : host,
                                        'device': device,
                                        'state' : state,
                                        })

    def change_device_weight(self, host, device, weight):
        """
        change the weight of a device by passing in the hostname and
        the device id
        """
        if not isinstance(weight, (int, long)):
            raise ValueError('argument weight muse be an integer')
        res = self.backend.do_request('set_weight',
                                      { 'host': host,
                                        'device': device,
                                        'weight': weight,
                                        })

    def slave_list(self):
        raise NotImplemetedError()

    def slave_add(self):
        raise NotImplemetedError()

    def slave_modify(self):
        raise NotImplemetedError()

    def slave_delete(self):
        raise NotImplemetedError()

    def fsck_start(self):
        self.backend.do_request("fsck_start")

    def fsck_stop(self):
        self.backend.do_request("fsck_stop")

    def fsck_reset(self, policy_only, startpos):
        return self.backend.do_request("fsck_reset",
                                       { 'policy_only': policy_only,
                                         'startpos'   : startpos,
                                         })

    def fsck_clearlog(self):
        self.backend.do_request("fsck_clearlog")

    def fsck_status(self):
        return self.backend.do_request("fsck_status")

    def fsck_log_rows(self):
        raise NotImplemetedError()

    def set_server_settings(self, key, value):
        res = self.backend.do_request("set_server_setting",
                                      { 'key': key,
                                        'value': value,
                                        })

    def server_settings(self):
        res = self.backend.do_request("server_settings")
        if not res:
            return
        ret = {}
        print res
        for x in xrange(1, int(res["key_count"])+1):
            key = res.get("key_%d" % x, '')
            value = res.get("value_%d" % x, '')
            ret[key] = value
        return ret

    def _modify_class(self, verb, domain, cls, mindevcount):
        _complain_ifreadonly(self.readonly)
        res = self.backend.do_request("%s_class" % verb,
                                      { 'domain': domain,
                                        'class' : cls,
                                        'mindevcount': mindevcount })
        return res['class'] == cls

    def _modify_host(self, verb, params):
        _complain_ifreadonly(self.readonly)
        res = self.backend.do_request("%s_host" % verb, params)
        #return res['hostname'] == host

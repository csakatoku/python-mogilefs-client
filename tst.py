# -*- coding: utf-8 -*-
import mogilefs

trackers = ["127.0.0.1:7001"]

def spam():
    mogfs = mogilefs.Client(domain="gumi", trackers=trackers)
    print mogfs.get_paths("factor") or mogfs.errstr()

    try:
        mogfs.store_content("spam", "normal", "SPAM EGG HAM")
        print mogfs.get_file_data("spam")
        print "Rename", mogfs.rename("spam", "ham")
    except Exception, e:
        print e

    print mogfs.delete("spam")
    print mogfs.delete("ham")

    print mogfs.store_file("egg2",
                           "normal",
                           "/home/chihiro_sakatoku/iwazumogana/django/tmp/farm2.static.flickr.com/0ed73a8b4c930b37de85e5aff6bfd6215273b929.jpg")
    print mogfs.get_paths("egg2")

    fp = mogfs.read_file("egg2")
    print len(fp.read())

def egg():
    mogadm = mogilefs.Admin(trackers=trackers)
    #print mogadm.get_hosts()
    #print mogadm.get_devices()
    #print mogadm.get_freespace()
    print mogadm.get_stats()

if __name__ == '__main__':
    spam()

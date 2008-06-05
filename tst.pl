use strict;
use warnings;
use MogileFS::Client;

my $mogc = MogileFS::Client->new(
    domain => "gumi",
    hosts  => ["127.0.0.1:7001"],
    );
$mogc->store_file(
    "egg",
    "normal",
    "/home/chihiro_sakatoku/iwazumogana/django/tmp/farm2.static.flickr.com/0ed73a8b4c930b37de85e5aff6bfd6215273b929.jpg",
    ) or die $mogc->errstr;

print $mogc->get_paths("egg");


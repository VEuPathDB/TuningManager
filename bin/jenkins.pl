#!/usr/bin/perl

# jenkins.pl
#
# helper script for managing Jenkins tuning-manager jobs
#
# more info about Jenkins tuning manager on api wiki:
#  https://wiki.apidb.org/index.php/tuningManager

use strict;

use JSON;
use Data::Dumper;

# get jobs JSON from Jenkins
my $jsonFile = "/tmp/tm.$$.json";
system("curl --config ~/.jenkins/tm.auth https://tm.apidb.org/api/json?pretty=true -o $jsonFile");
my $json_text;
    open(my $fh, '<', $jsonFile) or die "cannot open file $jsonFile";
    {
        local $/;
        $json_text = <$fh>;
    }
    close($fh);
my $perl_scalar = decode_json $json_text;
system ("rm $jsonFile");

# foreach my $job (@jobs) {

foreach my $jobHash (@{$perl_scalar->{jobs}}) {
  my $job = $jobHash->{name};

  # only certain jobs
  # next unless $job =~ m/inc.full/;

  # list jobs
  print "$job\n";

  # download config as tm.<project>.xml
  # print "curl --config ~/.jenkins/tm.auth https://tm.apidb.org/job/$job/config.xml -o tm.$job.xml\n";

  # delete project
  # print "curl -v -X POST --config ~/.jenkins/tm.auth -H 'Content-Type: text/xml' 'https://tm.apidb.org/job/$job/doDelete'\n";

  # replace config with XML file named by the "data-binary" param
  # print "curl -v -X POST --config ~/.jenkins/tm.auth --data-binary \@newIncFull.xml -H 'Content-Type: text/xml' 'https://tm.apidb.org/job/$job/config.xml'\n";

}

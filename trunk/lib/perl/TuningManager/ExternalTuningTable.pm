package TuningManager::TuningManager::ExternalTuningTable;

# @ISA = qw( TuningManager::TuningManager::Table );

use strict;
use TuningManager::TuningManager::Log;

sub new {
    my ($class,
	$name,               # name of database table
        $dblink,
        $dbh,                # database handle
        $doUpdate,           # are we updating, not just checking, the db?
        $housekeepingSchema, # schema for tables such as TuningTable
       )
	= @_;

    my $self = {};

    bless($self, $class);
    $self->{name} = $name;
    $self->{dbh} = $dbh;
    $self->{housekeepingSchema} = $housekeepingSchema;

    if ($dblink) {
      $dblink = '@' . $dblink;
    }
    $self->{dblink} = $dblink;

    # retrieve timestamp and tuning-table name. If there's no record, return current time and null as name
    my $sql = <<SQL;
       select to_char(nvl(timestamp, sysdate), 'yyyy-mm-dd hh24:mi:ss') as timestamp, name
       from dual, $housekeepingSchema.TuningTable$dblink
       where replace(dummy, 'X', lower('$self->{name}')) = lower(name(+))
SQL
    my $stmt = $dbh->prepare($sql);
    $stmt->execute()
      or TuningManager::TuningManager::Log::addErrorLog("\n" . $dbh->errstr . "\n");
    my ($timestamp, $storedName) = $stmt->fetchrow_array();
    $stmt->finish();

    TuningManager::TuningManager::Log::addLog("WARNING No TuningTable record for " . $self->getName())
	if (! $storedName);

    # test for existance
    $dbh->{PrintError} = 0;
    my $stmt = $dbh->prepare(<<SQL);
    select count(*) from $self->{name} $self->{dblink} where rownum=1
SQL
    $dbh->{PrintError} = 1;
    if (!$stmt) {
      $self->{exists} = 0;
    } else {
      $self->{exists} = 1;
    };

    $self->{timestamp} = $timestamp;
    return $self;
}

sub getTimestamp {
    my ($self) = @_;

    return $self->{timestamp};
}

sub getName {
  my ($self) = @_;

  return $self->{name} . $self->{dblink};
}

sub exists {
  my ($self) = @_;

  return $self->{exists};
}


1;

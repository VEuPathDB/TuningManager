package TuningManager::TuningManager::ExternalTable;


# @ISA = qw( TuningManager::TuningManager::Table );


use strict;
use Data::Dumper;
use TuningManager::TuningManager::Log;
use TuningManager::TuningManager::Utils;

my $currentDate;

sub new {
  my ($class,
    $name,               # name of database table
    $dblink,             # dblink (if any) needed to access table
    $noTrigger,          # optional attribute to prevent creation of date trigger
    $dbh,                # database handle
    $doUpdate,           # are we updating, not just checking, the db?
    $housekeepingSchema, # where do my overhead tables live?
  )
    = @_;

  my $self = {};

  bless($self, $class);
  $self->{name} = $name;
  $self->{dbh} = $dbh;
  $self->{noTrigger} = $noTrigger;
  $self->{housekeepingSchema} = $housekeepingSchema;

  if ($dblink) {
    $dblink = '@' . $dblink;
  }
  $self->{dblink} = $dblink;

  my ($schema, $table) = split(/\./, $name);
  $self->{schema} = $schema;
  $self->{table} = $table;

  # check that this table exists in the database
  my $sql = <<SQL;
select count(*) from (
 select table_schema, table_name from information_schema.tables
 where table_schema = lower('$schema') and table_name = lower('$table')
union
  select table_schema, table_name from information_schema.views
 where table_schema = lower('$schema') and table_name = lower('$table')
) tv;
SQL
  my $stmt = $dbh->prepare($sql);
  $stmt->execute()
    or TuningManager::TuningManager::Log::addErrorLog("\n" . $dbh->errstr . "\n" . $stmt->{sql});
  my ($count) = $stmt->fetchrow_array();
  $stmt->finish();
  $self->{exists} = $count;

  TuningManager::TuningManager::Log::addErrorLog("$self->{name} does not exist")
    if !$count;

  $self->checkTrigger($doUpdate);

  return $self;
}


sub getTimestamp {
  my ($self) = @_;

  return $self->{timestamp} if defined $self->{timestamp};

  my $dbh = $self->{dbh};
  my $dblink = $self->{dblink};

  my $debug = TuningManager::TuningManager::Log::getDebugFlag();

  # get the last modified date for this table
  my $sql = <<SQL;
    SELECT coalesce(to_char(max(modification_date), 'yyyy-mm-dd hh24:mi:ss'), '2000-01-01 00:00:00')
      , count(*)
    FROM $self->{name}$dblink
SQL
  my $stmt = $dbh->prepare($sql)
    or TuningManager::TuningManager::Log::addErrorLog("\n" . $dbh->errstr . "\n" . $sql) ;

  TuningManager::TuningManager::Utils::sqlBugWorkaroundExecute($dbh, $stmt)
    or TuningManager::TuningManager::Log::addErrorLog("\n" . $dbh->errstr . "\n" . $stmt->{sql});
  my ($max_mod_date, $row_count) = $stmt->fetchrow_array();
  $stmt->finish();

  # get stored ExternalDependency info for this table
  my $housekeepingSchema = $self->{housekeepingSchema};
  $sql = <<SQL;
SELECT to_char(max_mod_date, 'yyyy-mm-dd hh24:mi:ss')
    , row_count
    , to_char(timestamp, 'yyyy-mm-dd hh24:mi:ss')
FROM $housekeepingSchema.TuningMgrExternalDependency$dblink
WHERE name = upper('$self->{name}')
SQL
  my $stmt = $dbh->prepare($sql)
    or TuningManager::TuningManager::Log::addErrorLog("\n" . $dbh->errstr . "\n" . $stmt->{sql});

  $stmt->execute()
    or TuningManager::TuningManager::Log::addErrorLog("\n" . $dbh->errstr . "\n" . $stmt->{sql});
  my ($stored_max_mod_date, $stored_row_count, $timestamp) = $stmt->fetchrow_array();
  $stmt->finish();

  # compare stored and calculated table stats
  if ($max_mod_date eq $stored_max_mod_date && $row_count == $stored_row_count) {
    # stored stats still valid
    $self->{timestamp} = $timestamp;
    TuningManager::TuningManager::Log::addLog("    Stored timestamp ($timestamp) still valid for $self->{name}")
      if $debug;
  } else {
    # table has changed; tell the world, set timestamp high, and update TuningMgrExternalDependency
    if (!defined $stored_row_count) {
      TuningManager::TuningManager::Log::addLog("    No TuningMgrExternalDependency record for $self->{name}");
    } elsif ($row_count != $stored_row_count) {
      TuningManager::TuningManager::Log::addLog("    Number of rows has changed for $self->{name}");
    } elsif ($max_mod_date ne $stored_max_mod_date) {
      TuningManager::TuningManager::Log::addLog("    max(modification_date) has changed for $self->{name}");
    } else {
      TuningManager::TuningManager::Log::addErrorLog("checking state of external dependency $self->{name}");
    }
    $self->{timestamp} = $self->getCurrentDate();
    TuningManager::TuningManager::Log::addLog("    Setting timestamp to \"$self->{timestamp}\" for $self->{name}");

    if ($timestamp) {
      # ExternalDependency record exists; update it
      TuningManager::TuningManager::Log::addLog("    Stored timestamp ($timestamp) no longer valid for $self->{name}");
      $sql = <<SQL;
        update $housekeepingSchema.TuningMgrExternalDependency$dblink
        set (max_mod_date, timestamp, row_count) =
          (select to_date('$max_mod_date', 'yyyy-mm-dd hh24:mi:ss'), now(), $row_count)
        where name = upper('$self->{name}')
SQL
    } else {
      # no ExternalDependency record; insert one
      TuningManager::TuningManager::Log::addLog("    No stored timestamp found for $self->{name}");
      $sql = <<SQL;
        insert into $housekeepingSchema.TuningMgrExternalDependency$dblink
                    (name, max_mod_date, timestamp, row_count)
        select upper('$self->{name}'), to_date('$max_mod_date', 'yyyy-mm-dd hh24:mi:ss'), now(), $row_count
SQL
    }

    my $stmt = $dbh->prepare($sql)
      or TuningManager::TuningManager::Log::addErrorLog("\n" . $dbh->errstr . "\n" . $stmt->{sql});
    $stmt->execute()
      or TuningManager::TuningManager::Log::addErrorLog("\n" . $dbh->errstr . "\n" . $stmt->{sql});
    $stmt->finish();
  }

  TuningManager::TuningManager::Log::addLog("    $self->{name} has timestamp \"$self->{timestamp}\"");

  return $self->{timestamp};
}

sub getName {
  my ($self) = @_;

  return $self->{name};
}

sub exists {
  my ($self) = @_;

  return $self->{exists};
}

sub checkTrigger {
  my ($self, $doUpdate) = @_;

  my $dbh = $self->{dbh};

  # do nothing if the noTrigger attribute is set
  return if $self->{noTrigger} eq "true";

  # don't mess with triggers if we're looking at a remote table
  return if $self->{dblink};

  # is this a table, a view, a materialized view, a synonym, or what?
  my $schema = $self->{schema};
  my $table = $self->{table};

  my $stmt = $dbh->prepare(<<SQL);
 select table_type from information_schema.tables
 where table_schema = lower('$schema') and table_name = lower('$table')
SQL
  $stmt->execute()
    or TuningManager::TuningManager::Log::addErrorLog("\n" . $dbh->errstr . "\n" . $stmt->{sql});
  my ($objectType) = $stmt->fetchrow_array();
  $stmt->finish();

  # if this is a view, find the underlying table
  if ($objectType eq "VIEW") {
    my $stmt = $dbh->prepare(<<SQL);
 select view_definition from information_schema.views
 where table_schema = lower('$schema') and table_name = lower('$table')
SQL
    $stmt->execute()
      or TuningManager::TuningManager::Log::addErrorLog("\n" . $dbh->errstr . "\n" . $stmt->{sql});
    my ($viewText) = $stmt->fetchrow_array();
    $stmt->finish();

    $viewText =~ m/[.\r\n]*\bfrom\b\s*(\w*)\.(\w*)[.\r\n]*/i;
    $schema = $1; $table = $2;
  }

  # check for a trigger
  my $stmt = $dbh->prepare(<<SQL);
 select trigger_name, action_statement from information_schema.triggers
 where event_object_schema = lower('$schema') and event_object_table = lower('$table')
SQL
  $stmt->execute()
    or TuningManager::TuningManager::Log::addErrorLog("\n" . $dbh->errstr . "\n" . $stmt->{sql});

  my $gotModDateTrigger;
  while (my ($triggerName, $triggerText) = $stmt->fetchrow_array()) {

    if ($triggerText =~ m/modification_date/i) {
      $gotModDateTrigger = 1;
    } else {
      TuningManager::TuningManager::Log::addLog("Trigger $triggerName, on $schema.$table doesn't update modification_date");
    }
  }
  $stmt->finish();

  # if it doesn't exist and -doUpdate is not set, complain
  TuningManager::TuningManager::Log::addLog("$schema.$table has no trigger to keep modification_date up to date.")
    if (!$gotModDateTrigger && !$doUpdate);

  # if it doesn't exist and -doUpdate is set, create it
  if (!$gotModDateTrigger && $doUpdate) {
    my $triggerName = $table . "_md_tg";
    $triggerName =~ s/[aeiou]//gi;
    TuningManager::TuningManager::Log::addLog("Creating trigger $triggerName to maintain modification_date column of " . $self->{name});
    my $sqlReturn = $dbh->do(<<SQL);
CREATE OR REPLACE TRIGGER $triggerName
    BEFORE UPDATE OR INSERT ON $schema.$table FOR EACH ROW
EXECUTE FUNCTION apidb.trigger_fct_update_modification_date();
SQL
    if (!defined $sqlReturn) {
      TuningManager::TuningManager::Log::addErrorLog("\n" . $dbh->errstr . "\n" . $stmt->{sql});
    }
  }
}

sub getCurrentDate {
  my ($self) = @_;

  return $self->{currentDate}
    if $self->{currentDate};

  my $dbh = $self->{dbh};


  my $stmt = $dbh->prepare(<<SQL);
SELECT to_char(now(), 'yyyy-mm-dd hh24:mi:ss')
SQL

  $stmt->execute()
    or TuningManager::TuningManager::Log::addErrorLog("\n" . $dbh->errstr . "\n");
  ($self->{currentDate}) = $stmt->fetchrow_array();
  $stmt->finish();

  return $self->{currentDate};
}

1;

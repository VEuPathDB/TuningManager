package TuningManager::TuningManager::InternalTable;

use TuningManager::TuningManager::TableSuffix;
use TuningManager::TuningManager::Utils;
use TuningManager::TuningManager::Log qw(addLog addErrorLog addLogPreamble);

# @ISA = qw( TuningManager::TuningManager::Table );


use strict;
use Data::Dumper;
my $maxRebuildMinutes;

sub new {
  my ($class, $name, $internalDependencyNames, $externalDependencyNames,
    $externalTuningTableDependencyNames, $intermediateTables, $ancillaryTables, $sqls,
    $perls, $unionizations, $programs, $dbh, $debug,
    $alwaysUpdate, $prefixEnabled, $maxRebuildMinutesParam, $instance, $propfile, $schema,
    $password, $subversionDir, $dblink, $housekeepingSchema, $logTableName, $alwaysUpdateAll)
    = @_;

  my $self = {};
  $maxRebuildMinutes = $maxRebuildMinutesParam;

  bless($self, $class);
  $self->{name} = $name;
  $self->{schema} = $schema;
  $self->{internalDependencyNames} = $internalDependencyNames;
  $self->{externalDependencyNames} = $externalDependencyNames;
  $self->{externalTuningTableDependencyNames} = $externalTuningTableDependencyNames;
  $self->{intermediateTables} = $intermediateTables;
  $self->{ancillaryTables} = $ancillaryTables;
  $self->{sqls} = $sqls;
  $self->{perls} = $perls;
  $self->{unionizations} = $unionizations;
  $self->{programs} = $programs;
  $self->{debug} = $debug;
  $self->{dblink} = $dblink;
  $self->{internalDependencies} = [];
  $self->{externalDependencies} = [];
  $self->{externalTuningTableDependencies} = [];
  $self->{debug} = $debug;
  $self->{alwaysUpdate} = $alwaysUpdate;
  $self->{alwaysUpdateAll} = $alwaysUpdateAll;
  $self->{prefixEnabled} = $prefixEnabled;
  $self->{instance} = $instance;
  $self->{propfile} = $propfile;
  $self->{password} = $password;
  $self->{subversionDir} = $subversionDir;
  $self->{housekeepingSchema} = $housekeepingSchema;
  $self->{logTableName} = $logTableName;

  if ($name =~ /\./) {
    $self->{qualifiedName} = $name;
  } else {
    $self->{qualifiedName} = $schema . "." . $name;
  }

  # get timestamp, status, last_check, and definition from database
  my $sql = <<SQL;
       select to_char(timestamp, 'yyyy-mm-dd hh24:mi:ss') as timestamp,
              status,
              to_char(last_check, 'yyyy-mm-dd hh24:mi:ss') as last_check,
       definition
       from $housekeepingSchema.TuningTable
       where lower(name) = lower('$self->{qualifiedName}')
SQL

  my $stmt = $dbh->prepare($sql);
  $stmt->execute()
    or addErrorLog("\n" . $dbh->errstr . "\n" . $stmt->{sql});
  my ($timestamp, $dbStatus, $lastCheck, $dbDef) = $stmt->fetchrow_array();
  $stmt->finish();
  $self->{timestamp} = $timestamp;
  $self->{lastCheck} = $lastCheck;
  $self->{dbDef} = $dbDef;
  $self->{dbStatus} = $dbStatus;

  addLog("retrieved status \"$dbStatus\", timestamp \"$timestamp\", lastCheck \"$lastCheck\" for $self->{qualifiedName}");

  return $self;
}

sub getSqls {
  my ($self) = @_;

  return $self->{sqls};
}

sub getPerls {
  my ($self) = @_;

  return $self->{perls};
}

sub getUnionizations {
  my ($self) = @_;

  return $self->{unionizations};
}

sub getPrograms {
  my ($self) = @_;

  return $self->{programs};
}

sub getInternalDependencyNames {
  my ($self) = @_;

  return $self->{internalDependencyNames};
}

sub getExternalDependencyNames {
  my ($self) = @_;

  return $self->{externalDependencyNames};
}

sub getExternalTuningTableDependencyNames {
  my ($self) = @_;

  return $self->{externalTuningTableDependencyNames};
}

sub getInternalDependencies {
  my ($self) = @_;

  return $self->{internalDependencies};
}

sub getExternalDependencies {
  my ($self) = @_;

  return $self->{externalDependencies};
}

sub getExternalTuningTableDependencies {
  my ($self) = @_;

  return $self->{externalTuningTableDependencies};
}

sub getTimestamp {
  my ($self) = @_;

  return $self->{timestamp};
}

sub getLastCheck {
  my ($self) = @_;

  return $self->{lastCheck};
}

sub getState {
  my ($self, $doUpdate, $dbh, $purgeObsoletes, $prefix, $filterValue) = @_;

  return $self->{state} if defined $self->{state};

  addLog("checking $self->{name}  (creation timestamp: " . $self->getTimestamp() .
    ", last_check: $self->{lastCheck})");

  my $needUpdate;
  my $broken;
  my $tableStatus; # to store in TuningTable
  my $storedDefinitionChange;

  if ($prefix && $self->{prefixEnabled} ne "true") {
    addLog("    $self->{name} is not prefix-enabled; setting prefix to null for check/update.");
    $prefix = undef;
  }

  my $defString = $self->getDefString();

  # check if the definition is different (or none is stored)
  if (!$self->{dbDef}) {
    addLog("    no TuningTable record exists in database for $self->{name} -- update needed.");
    $needUpdate = 1;
    $storedDefinitionChange = 1;
  } elsif ($self->{dbDef} ne $self->getDefString()) {
    addLog("    stored TuningTable record differs from current definition for $self->{name} -- update needed.");
    $needUpdate = 1;
    $storedDefinitionChange = 1;
    addLog("stored:\n-------\n" . $self->{dbDef} . "\n-------")
      if $self->{debug};
    addLog("current:\n-------\n" . $self->getDefString() . "\n-------")
      if $self->{debug};
  } elsif ($self->{dbStatus} ne "up-to-date") {
    addLog("    stored TuningTable record for $self->{name} has status \"" . $self->{dbStatus} . "\" -- update needed.");
    $needUpdate = 1;
  }

  # check internal dependencies
  foreach my $dependency (@{$self->getInternalDependencies()}) {
    my $dependencyName = $dependency->getName();
    addLog("    depends on tuning table " . $dependencyName);

    unless ($defString =~ /$dependencyName/i ) {
      addLog("        WARNING: tuning table $self->{name} declares a dependency on the tuning table $dependencyName, but does not appear to reference it");
    }

    # increase log-file indentation for recursive call
    TuningManager::TuningManager::Log::increaseIndent();
    my $childState = $dependency->getState($doUpdate, $dbh, $purgeObsoletes, $prefix, $filterValue);
    TuningManager::TuningManager::Log::decreaseIndent();

    if ($childState eq "neededUpdate") {
      $needUpdate = 1;
      addLog("    $self->{name} needs update because it depends on " . $dependency->getName() . ", which was found to be out of date.");
    }
    if ($dependency->getTimestamp() gt $self->getLastCheck()) {
      $needUpdate = 1;
      addLog("    $self->{name} (creation timestamp: " . $self->getTimestamp() . ", last check: " . $self->getLastCheck() . ") needs update because it depends on " . $dependency->getName() . " (creation timestamp: " . $dependency->getTimestamp() . ")");
    }
    if ($childState eq "broken") {
      $broken = 1;
      addLog("    $self->{name} is broken because it depends on " . $dependency->getName() . ", which is broken.");
    }
  }

  # check external dependencies
  foreach my $dependency (@{$self->getExternalDependencies()}) {
    my $dependencyName = $dependency->getName();
    addLog("    depends on external table \"$dependencyName\"");

    unless ($defString =~ /$dependencyName/i ) {
      addLog("        WARNING: tuning table $self->{name} declares a dependency on external table $dependencyName, but does not appear to reference it");
    }

    if ($dependency->getTimestamp() gt $self->{lastCheck}) {
      $needUpdate = 1;
      addLog("    $self->{name} (creation timestamp: " . $self->getTimestamp() .
        ", last_check: $self->{lastCheck}) is older than observation timestamp of " .
        $dependency->getName() . " (" . $dependency->getTimestamp() .
        ") -- update needed.")
        if $self->getTimestamp();
    }
  }

  # check external tuning-table dependencies
  if ($self->getExternalTuningTableDependencies()) {
    foreach my $dependency (@{$self->getExternalTuningTableDependencies()}) {
      addLog("    depends on external tuning table " . $dependency->getName());
      if ($dependency->getTimestamp() gt $self->{lastCheck}) {
        $needUpdate = 1;
        addLog("    last check date of $self->{name} ($self->{lastCheck}) is older than creation timestamp of " . $dependency->getName() . " (" . $dependency->getTimestamp() . ") -- update needed.");
      }
    }
  }

  # try querying the table; if it can't be SELECTed from, it should be rebuilt
  $dbh->{PrintError} = 0;
  my $stmt = $dbh->prepare(<<SQL);
    select count(1) from $self->{name} where limit 1
SQL
  $dbh->{PrintError} = 1;
  if (!$stmt) {
    addLog("    query against $self->{name} failed -- update needed.");
    $needUpdate = 1;
  }

  if ($self->{alwaysUpdate}) {
    addLog("    " . $self->{name} . " must be updated because has the alwaysUpdate attribute.");
    $needUpdate = 1;
  }

  if ($self->{alwaysUpdateAll}) {
    addLog("    " . $self->{name} . " must be updated because the global alwaysUpdate flag is set.");
    $needUpdate = 1;
  }

  $tableStatus = "up-to-date";
  $tableStatus = "needs update"
    if $needUpdate;

  if ( ($doUpdate and $needUpdate) or $self->{alwaysUpdate} or ($doUpdate and $prefix)) {
    if ($prefix && !$self->{prefixEnabled}) {
      addErrorLog("attempt to update tuning table " . $self->{name} . ". This table does not have the prefixEnabled attribute, but the tuning manager was run with the -prefix parameter set.");
      $broken = 1;
    } elsif (!$broken) {
      my $updateResult = $self->update($dbh, $purgeObsoletes, $prefix, $filterValue, $storedDefinitionChange);
      if ($updateResult eq "broken") {
        $broken = 1;
        $tableStatus = "update failed";
      } else {
        $tableStatus = "up-to-date";
      }

      $needUpdate = 0
        if $updateResult eq "up-to-date";
    }
  }

  TuningManager::TuningManager::Log::setUpdateNeededFlag()
    if ($needUpdate or $prefix);

  if ($broken) {
    $self->{state} = "broken";
    TuningManager::TuningManager::Log::setErrorsEncounteredFlag();
  } elsif ($needUpdate or $prefix) {
    $self->{state} = "neededUpdate";
  } else {
    $self->{state} = "up-to-date";
  }

  addLog("    $self->{name} found to be \"$self->{state}\"");

  # update the stored record for this tuning table
  # unless this is a prefix run or we just rebuilt it
  $self->setStatus($dbh, $tableStatus)
    unless $prefix;

  return $self->{state};
}

sub update {
  my ($self, $dbh, $purgeObsoletes, $prefix, $filterValue, $storedDefinitionChange) = @_;

  my $startTime = time;

  my $startTimeString = getDatabaseTime($dbh);

  TuningManager::TuningManager::Log::setUpdatePerformedFlag()
    unless $self->{alwaysUpdate};

  my $suffix = TuningManager::TuningManager::TableSuffix::getSuffix($dbh, , $self->{housekeepingSchema} );

  my $dateString = `date`;
  chomp($dateString);
  addLog("    Rebuilding tuning table " . $self->{name} . " on $dateString");

  # how many steps of each type?
  my $stepCount;
  my @stepsByType;
  if ($self->{unionizations}) {
    $stepCount += scalar(@{$self->{unionizations}});
    push(@stepsByType, scalar(@{$self->{unionizations}}) . " unionization(s)");
  }
  if ($self->{sqls}) {
    $stepCount += scalar(@{$self->{sqls}});
    push(@stepsByType, scalar(@{$self->{sqls}}) . " SQL statement(s)");
  }
  if ($self->{perls}) {
    $stepCount += scalar(@{$self->{perls}});
    push(@stepsByType, scalar(@{$self->{perls}}) . " Perl programs(s)");
  }
  if ($self->{programs}) {
    $stepCount += scalar(@{$self->{programs}});
    push(@stepsByType, scalar(@{$self->{programs}}) . " external program(s)");
  }

  if ($self->{debug}) {
    addLog("    $stepCount step(s) needed. (" . join(", ", @stepsByType) . ")");
  }

  $self->dropIntermediateTables($dbh, $prefix);

  my $updateError;

  foreach my $unionization (@{$self->{unionizations}}) {

    last if $updateError;

    addLog("running unionization to build $self->{name}\n")
      if $self->{debug};

    my $stepStartTime = time;
    $self->unionize($unionization, $dbh);
    (my $oneline = substr($unionization, 0, 60)) =~ s/\s+/ /g;
    addLog((time - $stepStartTime) . " seconds to run the unionization beginning \"$oneline\"")
      if ($self->{debug} and $stepCount > 1);
  }

  foreach my $sql (@{$self->{sqls}}) {

    if ($sql =~ /]]>/) {
      addErrorLog("SQL contains embedded CDATA clause -- possible XML parse error. SQL -->>" . $sql . "<<--");
    }

    last if $updateError;

    my $sqlCopy = $sql;

    # use numeric suffix to make db object names unique
    $sqlCopy =~ s/&1/$suffix/g;

    # substitute prefix macro
    $sqlCopy =~ s/&prefix/$prefix/g;

    # substitute filterValue macro
    $sqlCopy =~ s/&filterValue/$filterValue/g;

    # substitute dblink macro
    my $dblink = $self->{dblink};
    $sqlCopy =~ s/&dblink/$dblink/g;

    addLog("running sql of length "
      . length($sqlCopy)
      . " to build $self->{name}:\n
      .$sqlCopy"
    ) if $self->{debug};

    my $stepStartTime = time;
    $updateError = 1 if !TuningManager::TuningManager::Utils::sqlBugWorkaroundDo($dbh, $sqlCopy);

    (my $oneline = substr($sqlCopy, 0, 60)) =~ s/\s+/ /g;
    addLog((time - $stepStartTime) . " seconds to run the SQL statement beginning \"$oneline\"")
      if ($self->{debug} and $stepCount > 1);

    # TODO Add this back later
    # if ($dbh->errstr =~ /ORA-01652/) {
    #   addLog("Setting out-of-space flag, so notification email is sent.");
    #   TuningManager::TuningManager::Log::setOutOfSpaceMessage($dbh->errstr);
    # }

  }

  foreach my $perl (@{$self->{perls}}) {
    last if $updateError;

    my $perlCopy = $perl;
    $perlCopy =~ s/&1/$suffix/g;  # use suffix to make db object names unique

    # substitute prefix macro
    $perlCopy =~ s/&prefix/$prefix/g;
    $perlCopy =~ s/&filterValue/$filterValue/g;

    addLog("running perl of length " . length($perlCopy) . " to build $self->{name}::\n$perlCopy")
      if $self->{debug};
    my $stepStartTime = time;
    eval $perlCopy;
    (my $oneline = substr($perlCopy, 0, 60)) =~ s/\s+/ /g;
    addLog((time - $stepStartTime) . " seconds to run the Perl program beginning \"$oneline\"")
      if ($self->{debug} and $stepCount > 1);

    if ($@) {
      $updateError = 1;
      addErrorLog("Error \"$@\" encountered executing Perl statement beginning:\n" . substr($perlCopy, 1, 100) );
    }
  }

  my $debug;
  $debug = " -debug " if $self->{debug};

  foreach my $program (@{$self->{programs}}) {
    addLog("Warning: The tuning table $self->{name} is updated by an external program but does not have the alwaysUpdate attribute.")
      unless $self->{alwaysUpdate};

    last if $updateError;

    my $commandLine = $program->{commandLine}
      . " -instance '" . $self->{instance} . "'"
      . " -propfile '" . $self->{propfile} . "'"
      . " -schema '" . $self->{schema} . "'"
      . " -suffix '" . $suffix . "'"
      . " -prefix '" . $prefix . "'"
      . " -filterValue '" . $filterValue . "'"
      . $debug
      . " 2>&1 ";

    addLog("running program with command line \"$commandLine\" to build $self->{name}");

    my $stepStartTime = time;
    open(PROGRAM, $commandLine . "|");
    while (<PROGRAM>) {
      my $line = $_;
      chomp($line);
      addLog($line);
    }
    close(PROGRAM);
    my $exitCode = $? >> 8;

    addLog("finished running program, with exit code $exitCode");

    addLog((time - $stepStartTime) . " seconds to run the command line \"$commandLine\"")
      if ($self->{debug} and $stepCount > 1);

    if ($exitCode) {
      addErrorLog("unable to run standalone program:\n$commandLine");
      $updateError = 1;
    }
  }

  return "broken" if $updateError;
  $self->{lastCheck} = $startTimeString;

  $self->dropIntermediateTables($dbh, $prefix, 'warn on nonexistence');

  my $buildDuration = time - $startTime;

  my $tableSize = $self->getTableSize($dbh, $suffix, $prefix);
  my ($tableMissing, $recordCount) = getRecordCount($dbh, $self->{name} . $suffix, $prefix);
  addLog("    $buildDuration seconds to rebuild tuning table "
    . $self->{name} . " with record count of " . $recordCount
    . " and total size of " . $tableSize . " (including all indexes and ancillary tables)");
  if ($maxRebuildMinutes) {
    addErrorLog("table rebuild took longer than $maxRebuildMinutes minute maximum.")
      if ($buildDuration > $maxRebuildMinutes * 60)
  }

  my $unchanged;

  if (!$prefix && !$storedDefinitionChange && !($self->{alwaysUpdateAll})
    && $self->{dbStatus} eq "up-to-date") {
    my $startCompare = time;
    $unchanged = $self->matchesPredecessor($suffix, $dbh);
    my $compareDuration = time - $startCompare;
    addLog("    $compareDuration seconds to compare " . $self->{name}
      . " (and any ancillary tables) with previous version. returned unchanged flag of: "
      . $unchanged);
  }

  if ($unchanged){
    addLog("dropping unneeded new version(s)");

    # drop unused main table
    if (!$dbh->do("drop table " . $self->{name} . $suffix)) {
      addErrorLog("error dropping unneeded new tuning table:" . $dbh->errstr);
    }

    # drop unused ancillary tables
    foreach my $ancillary (@{$self->{ancillaryTables}}) {
      if (!$dbh->do("drop table " . $ancillary->{name} . $suffix)) {
	addErrorLog("error dropping unneeded new ancillary table:" . $dbh->errstr);
      }
    }

    return "up-to-date";
  } else {
    # publish main table
    $self->publish($self->{name}, $suffix, $dbh, $purgeObsoletes, $prefix) or return "broken";

    # publish ancillary tables
    foreach my $ancillary (@{$self->{ancillaryTables}}) {
      addLog("publishing ancillary table " . $ancillary->{name});
      $self->publish($ancillary->{name}, $suffix, $dbh, $purgeObsoletes, $prefix) or return "broken";
    }

    # update in-memory creation timestamp
    $self->{timestamp} = $startTimeString;

    # store definition
    if (!$prefix) {
      addErrorLog("unable to store table definition")
        if $self->storeDefinition($dbh, $startTimeString);
    }

    # TODO SKIP LOG TABLE FOR THE TIME BEING
    # TuningManager::TuningManager::Log::logRebuild($dbh, $self->{name}, $buildDuration,
    #   $self->{instance}, $recordCount, $self->{logTableName}, $self->{housekeepingSchema})
    #   if !$prefix;

    return "neededUpdate"
  }
}

sub getDatabaseTime {
  my ($dbh) = @_;

  my $stmt = $dbh->prepare(<<SQL) or addErrorLog("\n" . $dbh->errstr . "\n");
    select to_char(now(), 'yyyy-mm-dd hh24:mi:ss')
SQL

  $stmt->execute() or addErrorLog("\n" . $dbh->errstr . "\n" . $stmt->{sql});
  my ($timeString) = $stmt->fetchrow_array();
  $stmt->finish();

  return $timeString;
}

sub getRecordCount {

  my ($dbh, $name, $prefix) = @_;
  my $recordCount = 0;
  my $tableNotThere = 0;

  $dbh->{PrintError} = 0;
  my $stmt = $dbh->prepare(<<SQL);
    select count(1) from $prefix$name
SQL

  if (!$stmt) {
    $tableNotThere = 1;
  } else {
    $stmt->execute()
      or addErrorLog("\n" . $dbh->errstr . "\n" . $stmt->{sql});
    ($recordCount) = $stmt->fetchrow_array();
    $stmt->finish();
  }
  $dbh->{PrintError} = 1;

  return ($tableNotThere, $recordCount);
}

sub storeDefinition {
  my ($self, $dbh, $startTimeString) = @_;

  my $housekeepingSchema = $self->{housekeepingSchema};

  my $sql = <<SQL;
       delete from $housekeepingSchema.TuningTable
       where lower(name) = lower('$self->{qualifiedName}')
SQL

  my $stmt = $dbh->prepare($sql);
  $stmt->execute()
    or addErrorLog("\n" . $dbh->errstr . "\n");
  $stmt->finish();

  my $sql = <<SQL;
       INSERT INTO $housekeepingSchema.TuningTable
          (name, timestamp, definition, status, last_check)
       VALUES (?, to_timestamp(?, 'yyyy-mm-dd hh24:mi:ss'), ?, 'up-to-date', to_timestamp(?, 'yyyy-mm-dd hh24:mi:ss'))
SQL

  my $stmt = $dbh->prepare($sql);

  if (!$stmt->execute($self->{qualifiedName}, $startTimeString, $self->getDefString(), $startTimeString)) {
    addErrorLog("\n" . $dbh->errstr . "\n" . $stmt->{sql});
    return "fail";
  }

  $stmt->finish();

  addLog("writing new record for tuning table \"$self->{qualifiedName}\" with status \"up-to-date\"");
  return;
}

sub getDefString {
  my ($self) = @_;

  return $self->{defString} if $self->{defString};

  my $defString;

  my $sqls = $self->getSqls();
  $defString = join(" ", @{$sqls}) if $sqls;

  my $perls = $self->getPerls();
  $defString .= join(" ", @{$perls}) if $perls;

  my $unionizations = $self->getUnionizations();
  $defString .= Dumper(@{$unionizations}) if $unionizations;

  my $programs = $self->getPrograms();
  $defString .= Dumper(@{$programs}) if $programs;

  $self->{defString} = $defString;

  return $defString;
}

sub getName {
  my ($self) = @_;

  return $self->{name};
}

sub addExternalDependency {
  my ($self, $dependency) = @_;

  push(@{$self->{externalDependencies}}, $dependency);
}

sub addExternalTuningTableDependency {
  my ($self, $dependency) = @_;

  push(@{$self->{externalTuningTableDependencies}}, $dependency);
}

sub addInternalDependency {
  my ($self, $dependency) = @_;

  push(@{$self->{internalDependencies}}, $dependency);
}

sub hasDependencyCycle {
  my ($self, $ancestorsRef) = @_;

  my $cycleFound;

  # log error if $self is earliest ancestor
  if ($ancestorsRef->[0] eq $self->{name}) {
    addErrorLog("ERROR: cycle of dependencies: " .
      join(" -> ", @{$ancestorsRef}) .
      " -> " . $self->{name});
    return 1;
  }

  # stop recursing if $self is ANY ancestor
  foreach my $ancestor (@{$ancestorsRef}) {
    return 1 if $ancestor eq $self->{name};
  }

  push(@{$ancestorsRef}, $self->{name});
  foreach my $child (@{$self->getInternalDependencies()}) {
    $cycleFound = 1
      if $child->hasDependencyCycle($ancestorsRef);
  }

  pop(@{$ancestorsRef});
  return $cycleFound;
}

sub dropIntermediateTables {
  my ($self, $dbh, $prefix, $warningFlag) = @_;

  foreach my $intermediate (@{$self->{intermediateTables}}) {
    addLog("    must drop intermediate table $prefix$intermediate->{name}");

    my $sql = <<SQL;
       drop table $prefix$intermediate->{name}
SQL

    $dbh->{PrintError} = 0;
    my $stmt = $dbh->prepare($sql);
    my $sqlReturn = $stmt->execute();
    $stmt->finish();
    $dbh->{PrintError} = 1;

    addLog("WARNING: intermediate table "
      . $intermediate->{name}
      . " was not created during the update of "
      . $self->{name})
      if ($warningFlag and !defined $sqlReturn);
  }

}

sub publish {
  my ($self, $tuningTableName, $suffix, $dbh, $purgeObsoletes, $prefix) = @_;
  my $housekeepingSchema = $self->{housekeepingSchema};
  my $schema = $self->{schema};

  # grant select privilege on new table
  my $sql = "GRANT SELECT ON $prefix$tuningTableName$suffix TO PUBLIC";

  my $stmt = $dbh->prepare($sql);
  my $grantRtn = $stmt->execute();
  if (!$grantRtn) {
    addErrorLog("GRANT SELECT on new table fails with error " . $dbh->errstr . "\n"  . $stmt->{sql});
    addErrorLog("check that $tuningTableName has a CREATE TABLE statement that includes the suffix macro, \"&1\", and (for a prefix-enabled table) the prefix macro, \"&prefix\"");
    return 0;
  }
  $stmt->finish();

  # get name of old table (for subsequent purging). . .
  my ($oldTable, $explicitSchema, $table);

  if ($tuningTableName =~ /\./) {
    ($explicitSchema, $table) = split(/\./, $tuningTableName);
  } else {
    $table = $tuningTableName;
  }

  $sql = <<SQL;
      SELECT t.table_schema || '.' || t.table_name
      FROM information_schema.tables t, information_schema.views v
      WHERE v.table_schema = t.table_schema
        AND t.table_type = 'BASE TABLE'
        AND t.table_schema = lower(?)
        AND v.table_name = ?
        AND t.table_name LIKE ? || '%'
        AND v.view_definition LIKE '%' || t.table_name || '%'
SQL

  my $stmt = $dbh->prepare($sql);
  $stmt->execute("$schema", lc "$prefix$table", lc "$prefix$table")
    or addErrorLog("\n" . $dbh->errstr . "\n");
  $oldTable = $stmt->fetchrow_array();
  $stmt->finish();

  # update VIEW
  $sql =  <<SQL;
    DO \$\$
    BEGIN
      CREATE OR REPLACE VIEW $prefix$table AS SELECT * FROM $prefix$table$suffix;
      GRANT SELECT ON $prefix$table TO GUS_R";
    EXCEPTION
    WHEN SQLSTATE '42P16' THEN
      DROP VIEW $prefix$table;
      CREATE OR REPLACE VIEW $prefix$table AS SELECT * FROM $prefix$table$suffix;
      GRANT SELECT ON $prefix$table TO GUS_R";
    END;
    \$\$ LANGUAGE PLPGSQL;
SQL

  my $viewRtn = $dbh->do($sql);

  if (defined $viewRtn) {
    if ($oldTable){
      if ($purgeObsoletes) {
        addLog("    purging obsolete table " . $oldTable);
        $dbh->do("DROP TABLE $oldTable CASCADE") or addErrorLog("\n" . $dbh->errstr . "\n");
      } else {
        # . . . or just mark it obsolete
        my $sql = <<SQL;
         INSERT INTO $housekeepingSchema.ObsoleteTuningTable (name, timestamp)
         VALUES ($oldTable , now())
SQL
        my $stmt = $dbh->prepare($sql);
        $stmt->execute() or addErrorLog("\n" . $dbh->errstr . "\n" . $stmt->{sql});
        $stmt->finish();
      }
    }
  } else{
    addErrorLog("\n" . $dbh->errstr . "\n");
  }

  # Run stored procedure to analyze new table
  $dbh->do("ANALYZE $schema.$prefix$table$suffix")
    or addErrorLog("\n" . $dbh->errstr . "\n");

  return $viewRtn
}

sub unionize {
  my ($self, $union, $dbh) = @_;

  $union->{name} = $self->{name}
    if !$union->{name};

  my ($coltypeRef, $columnsRef, $columnSetRef, $sourceNumber, $fromsRef)
    = $self->getColumnInfo($dbh, $union);

  my %coltype = %{$coltypeRef};
  my %columnSet = %{$columnSetRef};
  my @columns = @{$columnsRef};
  my @froms = @{$fromsRef};

  # build create table
  my @unionMembers; # array of query statements to be UNIONed
  $sourceNumber = 0;


  foreach my $source (@{$union->{source}}) {

    $sourceNumber++;

    my @selectees;  # array of terms for the SELECT clause
    my $notAllNulls = 0; # TRUE if at least one column is really there (else skip the whole unionMember)

    foreach my $column (@columns) {

      if ($columnSet{$sourceNumber}->{$column}) {
        $notAllNulls = 1;
        push(@selectees, $column);
      } else {
        push(@selectees, 'cast (null as ' . $coltype{$column} . ') as ' . $column);
      }
    }
    push(@unionMembers, 'select ' . join(', ', @selectees) . "\nfrom ". $froms[$sourceNumber])
      if $notAllNulls;
  }

  unless(scalar @{$union->{source}} == scalar @unionMembers) {
    addErrorLog("The number of <source> does not equal the number of sql statments to be unioned for " . $self->{name});
    die;
  }

  my $suffix = TuningManager::TuningManager::TableSuffix::getSuffix($dbh);

  my $createTable = "create table $union->{name}$suffix as\n"
    . join("\nunion\n", @unionMembers);

  addLog("creating union table with following statement:\n$createTable") if $self->{debug};
  runSql($dbh, $createTable);
}

#TODO UPDATE THIS BEFORE RUNNING UNIONIZATIONS
sub getColumnInfo {
  my ($self, $dbh, $union) = @_;

  my %coltype;
  my @columns;
  my %columnSet;
  my $sourceNumber;
  my @froms;

  foreach my $source (@{$union->{source}}) {

    $sourceNumber++;

    my $dblink = $source->{dblink};
    $dblink = "@" . $dblink
      if $dblink;
    my $table = $source->{name};

    my $tempTable;

    if ($source->{query}) {
      my $queryString = $source->{query}[0];
      $tempTable = $self->{schema} . "." . 'UnionizerTemp';
      $table = $tempTable;
      runSql($dbh, 'create table ' . $tempTable . ' as ' . $queryString, $self->{debug});
      $froms[$sourceNumber] = '(' . $queryString . ')';
    } else {
      $table = $union->{name} if !$table;
      $froms[$sourceNumber] = "$table$dblink";
    }

    my ($owner, $simpleTable) = split(/\./, $table);

    my $sql = <<SQL;
         select column_name, data_type, char_col_decl_length, column_id
         from all_tab_columns$dblink
         where owner=upper('$owner')
           and table_name=upper('$simpleTable')
         union
         select tab.column_name, tab.data_type, tab.char_col_decl_length,
                tab.column_id
         from all_synonyms$dblink syn, all_tab_columns$dblink tab
         where syn.table_owner = tab.owner
           and syn.table_name = tab.table_name
           and syn.owner=upper('$owner')
           and syn.synonym_name=upper('$simpleTable')
         order by column_id
SQL
    print "$sql\n\n" if $self->{debug};

    my $stmt = $dbh->prepare($sql);
    $stmt->execute();

    while (my ($columnName, $dataType, $charLen, $column_id) = $stmt->fetchrow_array()) {

      # add this to the list of columns and store its datatype declaration
      if (! $coltype{$columnName}) {
        push(@columns, $columnName);
        if ($dataType eq "VARCHAR2") {
          $coltype{$columnName} = 'VARCHAR2('.$charLen.')';
        } else {
          $coltype{$columnName} = $dataType;
        }
      }

      # note that this table has this column
      $columnSet{$sourceNumber}->{$columnName} = 1;
    }
    $stmt->finish();

    runSql($dbh, 'drop table ' . $tempTable) if ($tempTable);
  }

  return (\%coltype, \@columns, \%columnSet, $sourceNumber, \@froms);

}

sub runSql {
  my ($dbh, $sql, $debug) = @_;

  print "$sql\n\n" if $debug;

  my $stmt = $dbh->prepare($sql);
  $stmt->execute() or die "failed executing SQL statement \"$sql\"\n";
  $stmt->finish();
}

sub setStatus {
  my ($self, $dbh, $status) = @_;
  my $housekeepingSchema = $self->{housekeepingSchema};

  my $sql = <<SQL;
       update $housekeepingSchema.TuningTable
       set status = ?,
           check_os_user = ?,
           last_check = to_timestamp(?, 'yyyy-mm-dd hh24:mi:ss')
       where lower(name) = lower(?)
SQL

  my $stmt = $dbh->prepare($sql);

  addLog("setting status of tuning table \""
    . $self->{qualifiedName} . "\" to \""
    . $status . "\"");

  my $osUser = `whoami`;
  chomp($osUser);

  if (!$stmt->execute($status, $osUser, $self->{lastCheck}, $self->{qualifiedName})) {
    addErrorLog("\n" . $dbh->errstr . "\n");
    return "fail";
  }

  $stmt->finish();

  return;
}

sub matchesPredecessor {
  my ($self, $suffix, $dbh) = @_;

  my %liveRowCount;

  # get live-table row counts
  my ($predecessorMissing, $rowCount) = getRecordCount($dbh, $self->{name});
  return 0 if $predecessorMissing;
  $liveRowCount{$self->{name}} = $rowCount;

  foreach my $ancillary (@{$self->{ancillaryTables}}) {
    my ($predecessorMissing, $rowCount) = getRecordCount($dbh, $ancillary->{name});
    return 0 if $predecessorMissing;
    $liveRowCount{$ancillary->{name}} = $rowCount;
  }

  # compare counts of new tables
  my ($tableMissing, $rowCount) = getRecordCount($dbh, $self->{name} . $suffix);
  return 0 if $rowCount != $liveRowCount{$self->{name}};

  foreach my $ancillary (@{$self->{ancillaryTables}}) {
    my ($tableMissing, $rowCount) = getRecordCount($dbh, $ancillary->{name} . $suffix);
    return 0 if $rowCount != $liveRowCount{$ancillary->{name}};
  }

  # since row counts match, compare tables.
  return 0 if tablesDiffer($dbh, $self->{name},  $self->{name} . $suffix, $liveRowCount{$self->{name}});

  foreach my $ancillary (@{$self->{ancillaryTables}}) {
    return 0 if tablesDiffer($dbh, $ancillary->{name},  $ancillary->{name} . $suffix, $liveRowCount{$ancillary->{name}});
  }

  # every test passed, so they match
  return 1;
}

sub tablesDiffer {
  my ($dbh, $table1, $table2, $rowCount) = @_;

  my $intersectQuery = "SELECT count(1) FROM (SELECT * FROM $table1 INTERSECT SELECT * FROM $table2) t1";
  $dbh->{PrintError} = 0;
  my $stmt = $dbh->prepare($intersectQuery) or addLog("\n" . $dbh->errstr . "\n");

  if (!$stmt->execute()) {
    addLog("\n" . $dbh->errstr . "\n");
    $dbh->{PrintError} = 1;
    return 1;
  }

  my $intersectCount = $stmt->fetchrow_array();

  $stmt->finish();
  $dbh->{PrintError} = 1;
}

sub getTableSize {
  my ($self, $dbh, $suffix, $prefix) = @_;

  my $stmt = $dbh->prepare(<<SQL) or addErrorLog("\n" . $dbh->errstr . "\n");
SELECT pg_total_relation_size(?)
SQL

  my $total_space;
  my $table_name;


  $table_name = $self->{schema} . "." . $prefix . $self->{name} . $suffix;
  $stmt->execute($table_name) or addErrorLog("\n" . $dbh->errstr . "\n");
  ($total_space) = $stmt->fetchrow_array();
  $total_space = 0 unless $total_space;
  $stmt->finish();

  foreach my $ancillary (@{$self->{ancillaryTables}}) {
    $table_name = $self->{schema} . "." . $prefix . $ancillary->{name} . $suffix;
    $stmt->execute($table_name) or addErrorLog("\n" . $dbh->errstr . "\n");
    my ($space) = $stmt->fetchrow_array();
    $stmt->finish();
    $total_space += $space;
  }

  my $formatStmt = $dbh->prepare(<<SQL) or addErrorLog("\n" . $dbh->errstr . "\n");
SELECT pg_size_pretty(?)
SQL

  $formatStmt->execute($total_space) or addErrorLog("\n" . $dbh->errstr . "\n");
  my ($formattedSpace) = $stmt->fetchrow_array();

  return $formattedSpace;
}


1;

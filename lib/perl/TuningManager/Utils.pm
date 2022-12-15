package TuningManager::TuningManager::Utils;


use DBI;
use TuningManager::TuningManager::Log;

sub sqlBugWorkaroundDo {

  my ($dbh, $sql) = @_;

  my $attempts = 0;
  my $SQL_RETRIES = 10;
  my $thisSqlWorked;
  my $sqlReturn;

  do {

    $attempts++;
    TuningManager::TuningManager::Log::addLog("retrying -- \$attemps $attempts, \$SQL_RETRIES $SQL_RETRIES")
      if $attempts > 1;

    my $debug = TuningManager::TuningManager::Log::getDebugFlag();

    my ($sec,$min,$hour,$mday,$mon,$year) = localtime();
    my $timestamp = sprintf('%02d:%02d:%02d', $hour, $min, $sec);
    TuningManager::TuningManager::Log::addLog("executing sql at $timestamp")
      if $debug;

    $sqlReturn = $dbh->do($sql);

    TuningManager::TuningManager::Log::addLog("sql returned \"$sqlReturn\"; \$dbh->errstr = \"" . $dbh->errstr . "\"")
      if $debug ;

    if (defined $sqlReturn) {
      $thisSqlWorked = 1;
    } else {
      $thisSqlWorked = 0;
      TuningManager::TuningManager::Log::addErrorLog("\n" . $dbh->errstr . "\n");
    }


  } until $thisSqlWorked
    or ($dbh->errstr !~ /ORA-03135/)
    or ($attempts == $SQL_RETRIES);

  return $sqlReturn;
}

sub sqlBugWorkaroundExecute {

  my ($dbh, $stmt) = @_;

  my $attempts = 0;
  my $SQL_RETRIES = 10;
  my $thisSqlWorked;
  my $sqlReturn;

  do {

    $attempts++;
    addLog("retrying -- \$attemps $attempts, \$SQL_RETRIES $SQL_RETRIES")
      if $attempts > 1;

    my $debug = TuningManager::TuningManager::Log::getDebugFlag();

    my ($sec,$min,$hour,$mday,$mon,$year) = localtime();
    my $timestamp = sprintf('%02d:%02d:%02d', $hour, $min, $sec);
    TuningManager::TuningManager::Log::addLog("executing sql at $timestamp")
      if $debug;

    eval {
      $sqlReturn = $stmt->execute();
    };

    # log any errors inside eval
    TuningManager::TuningManager::Log::addErrorLog($@)
      if $@;

    TuningManager::TuningManager::Log::addLog("sql returned \"$sqlReturn\"; \$dbh->errstr = \"" . $dbh->errstr . "\"")
      if $debug;

    if (defined $sqlReturn) {
      $thisSqlWorked = 1;
    } else {
      $thisSqlWorked = 0;
      TuningManager::TuningManager::Log::addErrorLog("\n" . $dbh->errstr . "\n");
    }


  } until $thisSqlWorked
    or ($dbh->errstr !~ /ORA-03135/)
    or ($attempts == $SQL_RETRIES);

  return $sqlReturn;
}

sub getDbHandle {
  my ($instance, $username, $password, $schema) = @_;
  my $props;

  my $dsn = "dbi:Pg:" . $instance;
  my $dbh = DBI->connect(
    $dsn,
    $username,
    $password,
    { PrintError => 1, RaiseError => 0}
  ) or die "Can't connect to the database: $DBI::errstr\n";

  print "db info:\n  dsn=$instance\n  login=$username\n\n" if $debug;
  $dbh->do("SET search_path TO $schema") or die ("This doesn't quite work");
  return $dbh;
}

1;

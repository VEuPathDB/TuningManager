package TuningManager::TuningManager::TableSuffix;

use TuningManager::TuningManager::Log;

BEGIN {

  # The variable $suffix is declared inside a BEGIN block.  This makes it behave
  # like a Java "static" variable, whose state persists from one invocation to
  # another.

  my $suffix;

  sub getSuffix {
    my ($dbh, $housekeepingSchema) = @_;
    if (!defined $suffix) {
      my $sql = <<SQL;
       SELECT nextval('$housekeepingSchema.TuningManager_sq')
SQL

      my $stmt = $dbh->prepare($sql);
      $stmt->execute() or TuningManager::TuningManager::Log::addErrorLog($dbh->errstr);
      ($suffix) = $stmt->fetchrow_array();
      $stmt->finish();

      TuningManager::TuningManager::Log::addLog("Creating tuning tables with the suffix $suffix");
    }
    return $suffix;
  }

  sub suffixDefined {
    return (defined $suffix);
  }
}

1;

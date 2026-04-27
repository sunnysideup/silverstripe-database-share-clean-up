<?php

namespace Sunnysideup\DatabaseShareCleanUp\Api;

use RuntimeException;
use SilverStripe\ORM\DB;
use Sunnysideup\Flush\FlushNowImplementor;

class DatabaseActions
{
    /**
     * @var string[]
     */
    protected const TEXT_FIELDS = [
        'varchar',
        'text',
        'mediumtext',
    ];

    /**
     * @var string[]
     */
    protected const DATE_FIELDS = [
        'date',
    ];

    protected $forReal = false;

    protected $debug = false;

    protected static $tableList = [];

    protected static $fieldsForTable = [];

    public function setForReal(bool $bool)
    {
        $this->forReal = $bool;
    }

    public function setDebug(bool $bool)
    {
        $this->debug = $bool;
    }

    public function emptyVersionedTable(string $tableName, ?bool $leaveLastVersion = false): bool
    {
        $specialCase = in_array($tableName, ['ChangeSet', 'ChangeSetItem', 'ChangeSetItem_ReferencedBy'], true);
        if (str_ends_with($tableName, '_Versions') || $specialCase) {
            $nonVersionedTable = substr($tableName, 0, strlen($tableName) - 9);
            if ($this->hasTable($nonVersionedTable) || $specialCase) {
                $this->truncateTable($tableName);
                if ($leaveLastVersion) {
                    $fields = $this->getAllFieldsForOneTable($nonVersionedTable);
                    $fields = array_combine($fields, $fields);
                    foreach ($fields as $fieldName) {
                        if (! ($this->hasField($tableName, $fieldName) && $this->hasField($nonVersionedTable, $fieldName))) {
                            unset($fields[$fieldName]);
                        }
                    }

                    $fields['ID'] = 'RecordID';
                    unset($fields['Version']);
                    $fields['VERSION_NUMBER_HERE'] = 'Version';
                    $sql = '
                        INSERT INTO "' . $tableName . '" ("' . implode('", "', $fields) . '")
                        SELECT "' . implode('", "', array_keys($fields)) . '" FROM "' . $nonVersionedTable . '";';
                    $sql = str_replace('"VERSION_NUMBER_HERE"', '1', $sql);
                    $this->debugFlush('Copying unversioned from ' . $nonVersionedTable . ' into ' . $tableName, 'info');
                    $this->executeSql($sql);
                }

                return true;
            }

            FlushNowImplementor::do_flush('ERROR: could not find: ' . $nonVersionedTable, 'bad');
        }

        return false;
    }

    public function deleteObsoleteTables(string $tableName): bool
    {
        if (str_starts_with($tableName, '_obsolete_')) {
            $this->deleteTable($tableName);

            return true;
        }

        return false;
    }

    public function deleteTable(string $tableName)
    {
        $this->debugFlush('Deleting ' . $tableName . ' as it is not required', 'deleted');
        $sql = 'DROP TABLE "' . $tableName . '";';
        $this->executeSql($sql);
    }

    public function truncateTable(string $tableName)
    {
        $this->debugFlush('Emptying ' . $tableName, 'changed');
        $sql = 'TRUNCATE TABLE "' . $tableName . '"; ';
        $this->executeSql($sql);
    }

    public function truncateField(string $tableName, string $fieldName, ?int $limit = 99999999, ?bool $silent = false): bool
    {
        if ($this->isTextField($tableName, $fieldName) || $this->isDateField($tableName, $fieldName)) {
            if (false === $silent) {
                $this->debugFlush('Emptying ' . $tableName . '.' . $fieldName, 'obsolete');
            }

            $sortStatement = $this->getSortStatement($tableName);
            $sql = '
                UPDATE "' . $tableName . '"
                SET "' . $fieldName . '" = \'\'
                ' . $sortStatement . '
                LIMIT ' . $limit;
            $this->executeSql($sql);

            return true;
        }

        $this->debugFlush('Skipping emptying ' . $tableName . '.' . $fieldName . ' as this is not a text field', 'info');

        return false;
    }

    private function getRandomCharExpr(): string
    {
        return "SUBSTR('0123456789abcdefghijklmnopqrstuvwxyz',(RAND()*35)+1,1)";
    }

    private function r(int $count = 1): string
    {
        $r = $this->getRandomCharExpr();
        return implode(', ', array_fill(0, $count, $r));
    }

    private function updateSql(string $tableName, string $fieldName, string $expr): string
    {
        return '
        UPDATE "' . $tableName . '"
        SET "' . $fieldName . '" = ' . $expr . '
        WHERE "' . $fieldName . '" IS NOT NULL AND "' . $fieldName . '" <> \'\'';
    }

    protected function anonymiseEmail(string $tableName, string $fieldName): void
    {
        // e.g. abc123@xy.zz
        $r = $this->getRandomCharExpr();
        $expr = 'CONCAT(' . $this->r(5) . ", '@', " . $this->r(3) . ", '.', " . $this->r(2) . ')';
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseUsername(string $tableName, string $fieldName): void
    {
        $this->anonymiseEmail($tableName, $fieldName);
    }

    protected function anonymiseFirstName(string $tableName, string $fieldName): void
    {
        // e.g. Abcdef
        $upper = "SUBSTR('ABCDEFGHIJKLMNOPQRSTUVWXYZ',(RAND()*25)+1,1)";
        $expr = 'CONCAT(' . $upper . ', ' . $this->r(5) . ')';
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseSurname(string $tableName, string $fieldName): void
    {
        $this->anonymiseFirstName($tableName, $fieldName);
    }

    protected function anonymiseLastName(string $tableName, string $fieldName): void
    {
        $this->anonymiseFirstName($tableName, $fieldName);
    }

    protected function anonymiseCity(string $tableName, string $fieldName): void
    {
        $upper = "SUBSTR('ABCDEFGHIJKLMNOPQRSTUVWXYZ',(RAND()*25)+1,1)";
        $expr = 'CONCAT(' . $upper . ', ' . $this->r(6) . ')';
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseSuburb(string $tableName, string $fieldName): void
    {
        $this->anonymiseCity($tableName, $fieldName);
    }

    protected function anonymiseAddress(string $tableName, string $fieldName): void
    {
        // e.g. 42 Abcdef St
        $upper = "SUBSTR('ABCDEFGHIJKLMNOPQRSTUVWXYZ',(RAND()*25)+1,1)";
        $suffixes = "'St', 'Rd', 'Ave', 'Ln', 'Cres', 'Pl'";
        $expr = 'CONCAT(
        FLOOR(RAND()*999)+1, \' \',
        ' . $upper . ',
        ' . $this->r(5) . ',
        \' \',
        ELT(FLOOR(RAND()*6)+1, ' . $suffixes . ')
    )';
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseStreet(string $tableName, string $fieldName): void
    {
        // e.g. Abcdef Street
        $upper = "SUBSTR('ABCDEFGHIJKLMNOPQRSTUVWXYZ',(RAND()*25)+1,1)";
        $suffixes = "'Street', 'Road', 'Avenue', 'Lane', 'Crescent', 'Place'";
        $expr = 'CONCAT(
        ' . $upper . ',
        ' . $this->r(5) . ',
        \' \',
        ELT(FLOOR(RAND()*6)+1, ' . $suffixes . ')
    )';
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseAddress2(string $tableName, string $fieldName): void
    {
        // e.g. Apt 42
        $types = "'Apt', 'Suite', 'Unit', 'Floor'";
        $expr = 'CONCAT(ELT(FLOOR(RAND()*4)+1, ' . $types . "), ' ', FLOOR(RAND()*99)+1)";
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseCompany(string $tableName, string $fieldName): void
    {
        // e.g. Abcdef Ltd
        $upper = "SUBSTR('ABCDEFGHIJKLMNOPQRSTUVWXYZ',(RAND()*25)+1,1)";
        $suffixes = "'Ltd', 'Inc', 'Co', 'Group', 'Holdings'";
        $expr = 'CONCAT(
        ' . $upper . ',
        ' . $this->r(5) . ',
        \' \',
        ELT(FLOOR(RAND()*5)+1, ' . $suffixes . ')
    )';
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseCompanyName(string $tableName, string $fieldName): void
    {
        $this->anonymiseCompany($tableName, $fieldName);
    }

    protected function anonymisePhone(string $tableName, string $fieldName): void
    {
        // e.g. 09-456-7890
        $expr = "CONCAT('0', FLOOR(RAND()*7)+2, '-', FLOOR(RAND()*900)+100, '-', FLOOR(RAND()*9000)+1000)";
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseMobile(string $tableName, string $fieldName): void
    {
        // e.g. 021-456-7890
        $prefixes = "'021', '022', '027', '028'";
        $expr = sprintf("CONCAT(ELT(FLOOR(RAND()*4)+1, %s), '-', FLOOR(RAND()*900)+100, '-', FLOOR(RAND()*9000)+1000)", $prefixes);
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseFax(string $tableName, string $fieldName): void
    {
        $prefixes = "'03', '04', '06', '07', '09'";
        $expr = sprintf("CONCAT(ELT(FLOOR(RAND()*5)+1, %s), '-', FLOOR(RAND()*900)+100, '-', FLOOR(RAND()*9000)+1000)", $prefixes);
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseDob(string $tableName, string $fieldName): void
    {
        $sql = '
        UPDATE "' . $tableName . '"
        SET "' . $fieldName . '" = DATE_FORMAT(
            DATE_ADD("1970-01-01", INTERVAL FLOOR(RAND()*12783) DAY),
            \'%d/%m/%Y\'
        )
        WHERE "' . $fieldName . '" IS NOT NULL AND "' . $fieldName . '" <> \'\'';
        $this->executeSql($sql);
    }


    protected function anonymiseDateOfBirth(string $tableName, string $fieldName): void
    {
        $sql = '
        UPDATE "' . $tableName . '"
        SET "' . $fieldName . '" = DATE_ADD("1970-01-01", INTERVAL FLOOR(RAND()*12783) DAY)
        WHERE "' . $fieldName . '" IS NOT NULL AND "' . $fieldName . '" <> \'\'';
        $this->executeSql($sql);
    }

    protected function anonymiseIp(string $tableName, string $fieldName): void
    {
        $expr = "CONCAT(
        FLOOR(RAND()*253)+1, '.',
        FLOOR(RAND()*255), '.',
        FLOOR(RAND()*255), '.',
        FLOOR(RAND()*253)+1
    )";
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }


    protected function anonymiseIpAddress(string $tableName, string $fieldName): void
    {
        $this->anonymiseIp($tableName, $fieldName);
    }

    protected function anonymiseProxy(string $tableName, string $fieldName): void
    {
        $this->anonymiseIp($tableName, $fieldName);
    }

    public function removeOldRowsFromTable(string $tableName, float $percentageToKeep)
    {
        $this->debugFlush('Deleting ' . (100 - round($percentageToKeep * 100, 2)) . '% of the Rows in ' . $tableName, 'obsolete');
        $limit = $this->turnPercentageIntoLimit($tableName, $percentageToKeep);
        $sortStatement = $this->getSortStatement($tableName);
        $sql = '
            DELETE FROM "' . $tableName . '"
            ' . $sortStatement . '
            LIMIT ' . $limit;
        $this->executeSql($sql);
    }

    public function removeOldColumnsFromTable(string $tableName, string $fieldName, float $percentageToKeep): bool
    {
        $this->debugFlush('Emptying ' . (100 - round($percentageToKeep * 100, 2)) . '% from ' . $tableName . '.' . $fieldName, 'obsolete');
        $limit = $this->turnPercentageIntoLimit($tableName, $percentageToKeep);

        return $this->truncateField($tableName, $fieldName, $limit, $silent = true);
    }

    public function getAllTables(?bool $fresh = true): array
    {
        if ($fresh || 0 === count(self::$tableList)) {
            self::$tableList = DB::table_list();
        }

        return self::$tableList;
    }

    public function getAllFieldsForOneTable(string $tableName): array
    {
        return array_keys($this->getAllFieldsForOneTableDetails($tableName));
    }

    public function getAllFieldsForOneTableDetails(string $tableName): array
    {
        if (! isset(self::$fieldsForTable[$tableName])) {
            self::$fieldsForTable[$tableName] = [];
            if ($this->hasTable($tableName)) {
                self::$fieldsForTable[$tableName] = DB::field_list($tableName);
            }
        }

        return self::$fieldsForTable[$tableName];
    }

    public function isEmptyTable(string $tableName): bool
    {
        if ($this->tableExists($tableName)) {
            return 0 === $this->countRows($tableName);
        }

        return true;
    }

    public function countRows(string $tableName): int
    {
        return (int) DB::query('SELECT COUNT(*) FROM "' . $tableName . '";')->value();
    }

    public function tableExists(string $tableName): bool
    {
        return (bool) DB::query("SHOW TABLES LIKE '" . $tableName . "';")->value();
    }

    public function getTableSizeInMegaBytes(string $tableName): float
    {
        return floatval(DB::query('
            SELECT  round(((data_length + index_length ) / 1024 / 1024), 2) as C
            FROM information_schema.TABLES
            WHERE
                table_schema = \'' . DB::get_conn()->getSelectedDatabase() . '\'
                AND table_name = \'' . $tableName . '\';
        ')->value());
    }

    public function getColumnSizeInMegabytes(string $tableName, string $fieldName): float
    {
        return floatval(DB::query('
            SELECT round(sum(char_length("' . $fieldName . '")) / 1024 / 1024)
            FROM "' . $tableName . '";
        ')->value());
    }

    protected function isTextField(string $tableName, string $fieldName): bool
    {
        return $this->isSomeTypeOfField($tableName, $fieldName, self::TEXT_FIELDS);
    }

    protected function isDateField(string $tableName, string $fieldName): bool
    {
        return $this->isSomeTypeOfField($tableName, $fieldName, self::DATE_FIELDS);
    }

    protected function isSomeTypeOfField(string $tableName, string $fieldName, array $typeStrings): bool
    {
        $details = $this->getAllFieldsForOneTableDetails($tableName);
        if (isset($details[$fieldName])) {
            foreach ($typeStrings as $test) {
                if (0 === stripos(strtolower($details[$fieldName]), (string) $test)) {
                    return true;
                }
            }
        } else {
            FlushNowImplementor::do_flush('ERROR: could not find: ' . $tableName . '.' . $fieldName, 'bad');
        }

        return false;
    }

    protected function turnPercentageIntoLimit(string $tableName, float $percentageToKeep): int
    {
        $count = DB::query('SELECT COUNT("ID") FROM "' . $tableName . '"')->value();
        $count = intval($count);

        return (int) round($percentageToKeep * $count);
    }

    protected function executeSql(string $sql)
    {
        $this->debugFlush('Running <pre>' . $sql . '</pre>', 'info');
        if ($this->forReal) {
            DB::query($sql);
            $this->debugFlush(' ... done', 'green');
        } else {
            $this->debugFlush(' ... not exectuted!', 'info');
        }
    }

    protected function getSortStatement(string $tableName): string
    {
        if ($this->hasField($tableName, 'ID')) {
            return 'ORDER BY "ID" ASC';
        }

        return '';
    }

    protected function hasField(string $tableName, string $fieldName): bool
    {
        return (bool) DB::get_schema()->hasField($tableName, $fieldName);
    }

    protected function hasTable(string $tableName): bool
    {
        return (bool) DB::get_schema()->hasTable($tableName);
    }

    protected function debugFlush(string $message, string $type)
    {
        if ($this->debug) {
            FlushNowImplementor::do_flush($message, $type);
        }
    }

    /**
     * Calls the appropriate anonymise method on the databaseActions object.
     */
    public function CallAnonymiseMethod(string $fieldType, string $tableName, string $fieldName): bool
    {
        $method = 'anonymise' . $fieldType;
        if (!method_exists($this, $method)) {
            throw new RuntimeException(sprintf("Anonymisation method '%s' does not exist on ", $method) . static::class);
        }

        $this->$method($tableName, $fieldName);
        return true;
    }
}

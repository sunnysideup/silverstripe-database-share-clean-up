<?php

namespace Sunnysideup\DatabaseShareCleanUp\Api;

use SilverStripe\ORM\DB;
use Sunnysideup\Flush\FlushNow;
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
        $specialCase = in_array($tableName, ['ChangeSet', 'ChangeSetItem', 'ChangeSetItem_ReferencedBy']);
        echo 'CCCC';
        if ('_Versions' === substr((string) $tableName, -9) || $specialCase) {
            echo 'AAA';
            $nonVersionedTable = substr((string) $tableName, 0, strlen( (string) $tableName) - 9);
            echo 'BBB';
            if ($this->hasTable($nonVersionedTable) ||  $specialCase) {
                $this->truncateTable($tableName);
                if ($leaveLastVersion) {
                    $fields = $this->getAllFieldsForOneTable($nonVersionedTable);
                    $fields = array_combine($fields, $fields);
                    foreach ($fields as $fieldName) {
                        if (!($this->hasField($tableName, $fieldName) && $this->hasField($nonVersionedTable, $fieldName))) {
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
        if (0 === strpos($tableName, '_obsolete_')) {
            echo 'BBBB' . $tableName;
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

    public function anonymiseField(string $tableName, string $fieldName): bool
    {
        if ($this->isTextField($tableName, $fieldName)) {
            $this->debugFlush('Anonymising ' . $tableName . '.' . $fieldName, 'repaired');
            // $sortStatement = $this->getSortStatement($tableName);
            $r = "SUBSTR('0123456789abcdefghihjlmnopqrstuvwxyz',(RAND()*35)+1,1)";
            $sql = '
                UPDATE "' . $tableName . '"
                SET "' . $fieldName . '" = CONCAT(' . $r . ', ' . $r . ', ' . $r . ", '@', " . $r . ', ' . $r . ", '.', " . $r . ')
                WHERE "' . $fieldName . '" IS NOT NULL AND "' . $fieldName . '" <> \'\'';
            $this->executeSql($sql);

            return true;
        }
        if ($this->isDateField($tableName, $fieldName)) {
            $this->debugFlush('Anonymising ' . $tableName . '.' . $fieldName, 'repaired');
            // $sortStatement = $this->getSortStatement($tableName);
            // randomise by three years
            $sql = '
                UPDATE "' . $tableName . '"
                SET "' . $fieldName . '" = DATE_ADD("' . $fieldName . '", INTERVAL ((1 - ROUND((RAND()))*2)*999) DAY)
                WHERE "' . $fieldName . '" IS NOT NULL';
            $this->executeSql($sql);

            return true;
        }
        $this->debugFlush('Skipping anonymising ' . $tableName . '.' . $fieldName . ' as this is not a text field', 'info');

        return false;
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
        if (!isset(self::$fieldsForTable[$tableName])) {
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
        return DB::query('SHOW TABLES LIKE \'' . $tableName . '\';')->value() ? true : false;
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
                if (0 === stripos(strtolower($details[$fieldName]), $test)) {
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
}

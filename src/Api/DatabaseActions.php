<?php

namespace Sunnysideup\DatabaseShareCleanUp\Api;
use Sunnysideup\DatabaseShareCleanUp\Api\DatabaseInfo;

use SilverStripe\ORM\DB;
use Sunnysideup\Flush\FlushNow;

class DatabaseActions extends DatabaseInfo
{

    protected static $forReal = false;

    protected static $debug = false;


    public function setForReal(bool $bool)
    {
        $this->forReal = $bool;
    }

    public function setDebug(bool $bool)
    {
        $this->debug = $bool;
    }

    public function emptyVersionedTable(string $tableName): bool
    {
        if ('_Versions' === substr($tableName, -9)) {
            $nonVersionedTable = substr($tableName, 0, strlen($tableName) - 9);
            if ($this->hasTable($nonVersionedTable)) {
                $this->truncateTable($tableName);
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

                return true;
            }
            FlushNow::do_flush('ERROR: could not find: ' . $nonVersionedTable, 'bad');
        }

        return false;
    }

    public function deleteObsoleteTables(string $tableName): bool
    {
        if (0 === strpos($tableName, '_obsolete_')) {
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
        if ($this->isTextField($tableName, $fieldName)) {
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

    /**
     * we use this so that we can truncate the oldest x %% of table
     * @param  string $tableName
     * @return string
     */
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
            FlushNow::do_flush($message, $type);
        }
    }
}

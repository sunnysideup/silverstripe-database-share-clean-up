<?php

namespace Sunnysideup\DatabaseShareCleanUp\Api;
use Sunnysideup\DatabaseShareCleanUp\Api\DatabaseInfo;

use SilverStripe\ORM\DB;
use Sunnysideup\Flush\FlushNow;

class DatabaseActions extends DatabaseInfo
{

    protected static $forReal = false;

    protected static $debug = false;

    protected static $runner = null;


    public function setForReal(bool $bool)
    {
        self::$forReal = $bool;
    }

    public function setDebug(bool $bool)
    {
        self::$debug = $bool;
    }

    public function setRunner($runner)
    {
        self::$runner = $runner;
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
        if (self::$forReal) {
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

    protected function debugFlush(string $message, string $type)
    {
        if (self::$debug) {
            FlushNow::do_flush($message, $type);
        }
    }
}

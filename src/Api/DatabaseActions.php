<?php

namespace Sunnysideup\DatabaseShareCleanUp\Api;

use SilverStripe\ORM\DB;
use Sunnysideup\Flush\FlushNow;

class DatabaseActions
{
    protected $forReal = false;

    protected $debug = false;

    public function setForReal(bool $bool)
    {
        $this->forReal = $bool;
    }
    public function setDebug(bool $bool)
    {
        $this->debug = $bool;
    }

    public function deleteAllObsoleteTables()
    {
        $tables = $this->getAllTables();
        foreach($tables as $table) {
            if(strpos($table, '_obsolete_') === 0) {
                FlushNow::do_flush('Deleting '.$table .' as it is obsolete', 'deleted');
                $sql = 'DROP TABLE "'.$table.'";';
                $this->executeSql($sql);
            }
        }
    }

    public function truncateTable(string $tableName)
    {
        FlushNow::do_flush('Emptying ' . $tableName, 'bad');
        $sql = 'TRUNCATE TABLE "' . $tableName . '"; ';
        $this->executeSql($sql);
    }

    public function truncateField(string $tableName, string $fieldName, ?int $limit = 99999999, ?bool $silent = false)
    {
        if($silent === false) {
            FlushNow::do_flush('Emptying ' . $tableName . '.' . $fieldName, 'bad');
        }
        $sortStatement = $this->getSortStatement($tableName);
        $r = 'SUBSTR(\'0123456789abcdefghihjlmnopqrstuvwxyz\',(RAND()*35)+1,1)';
        $sql = '
            UPDATE "' . $tableName . '"
            SET "' . $fieldName . '" = \'\'
            '.$sortStatement.'
            LIMIT ' . $limit;
        $this->executeSql($sql);
    }

    public function anonymiseField(string $tableName, string $fieldName, ?int $limit = 99999999, ?bool $silent = false)
    {
        if($silent === false) {
            FlushNow::do_flush('Anonymising ' . $tableName . '.' . $fieldName, 'bad');
        }
        $sortStatement = $this->getSortStatement($tableName);
        $r = 'SUBSTR(\'0123456789abcdefghihjlmnopqrstuvwxyz\',(RAND()*35)+1,1)';
        $sql = '
            UPDATE "' . $tableName . '"
            SET "' . $fieldName . '" = CONCAT('.$r.', '.$r.', '.$r.', \'@\', '.$r.', '.$r.', \'.\', '.$r.')
            '.$sortStatement.'
            LIMIT ' . $limit;
        $this->executeSql($sql);
    }

    public function removeOldRowsFromTable(string $tableName, float $percentageToKeep)
    {
        FlushNow::do_flush('Deleting Stale Rows ' . (100 - round($percentageToKeep * 100, 2)) . '% from '.$tableName, 'bad');
        $limit = $this->turnPercentageIntoLimit($tableName, $percentageToKeep);
        $sortStatement = $this->getSortStatement($tableName);
        $sql = '
            DELETE FROM "' . $tableName . '"
            '.$sortStatement.'
            LIMIT ' . $limit;
        $this->executeSql($sql);
    }

    public function removeOldColumnsFromTable(string $tableName, string $fieldName, float $percentageToKeep)
    {
        FlushNow::do_flush('Emptying ' . (100 - round($percentageToKeep * 100, 2)) . '% from '.$tableName . '.' . $fieldName, 'bad');
        $limit = $this->turnPercentageIntoLimit($tableName, $percentageToKeep);
        $this->truncateField($tableName, $fieldName, $limit, $silent = true);
    }

    public function getAllTables(): array
    {
        return DB::table_list();
    }

    public function getAllFieldsForOneTable(string $tableName): array
    {
        return array_keys(DB::field_list($tableName));
    }

    public function getTableSizeInMegaBytes(string $tableName): float
    {
        return floatval(DB::query('
            SELECT  round(((data_length + index_length) / 1024 / 1024), 2) as SIZE
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

    protected function turnPercentageIntoLimit(string $tableName, $percentageToKeep): int
    {
        $count = DB::query('SELECT COUNT(*) FROM "' . $tableName . '"')->value();

        return (int) round($percentageToKeep * $count);
    }

    protected function executeSql(string $sql)
    {
        if ($this->debug) {
            FlushNow::do_flush('Running <pre>' . $sql . '</pre>');
        }
        if ($this->forReal) {
            DB::query($sql);
            if($this->debug) {
                FlushNow::do_flush(' ... done', 'green');
            }
        } else {
            if($this->debug) {
                FlushNow::do_flush(' ... not exectuted!', 'green');
            }
        }
    }

    protected function getSortStatement(string $tableName) : string
    {
        if (DB::get_schema()->hasField($tableName, 'ID')) {
            return 'ORDER BY "ID" ASC';
        } else {
            return '';
        }
    }

}

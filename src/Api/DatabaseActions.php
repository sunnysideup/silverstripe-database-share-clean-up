<?php

namespace Sunnysideup\DatabaseShareCleanUp\Api;

use SilverStripe\ORM\DB;
use Sunnysideup\Flush\FlushNow;

class DatabaseActions
{
    protected $forReal = false;

    public function setForReal(bool $bool)
    {
        $this->forReal = $bool;
    }

    public function truncateTable(string $tableName)
    {
        $sql = 'TRUNCATE TABLE "' . $tableName . '"; ';
        $this->executeSql($sql);
    }

    public function truncateField(string $tableName, string $fieldName, ?int $limit = 99999999)
    {
        $removeString = 'removed@' . uniqid() . '.' . uniqid();
        $sql = '
            UPDATE "' . $tableName . '"
            SET "' . $fieldName . '" = \'' . $removeString . '\'
            ORDER BY "ID",
            LIMIT ' . $limit;
        $this->execute($sql);
    }

    public function getAllTables(): array
    {
        return DB::table_list();
    }

    public function getAllFieldsForOneTable(string $tableName): array
    {
        return DB::field_list($tableName);
    }

    public function getTableSizeInMegaBytes(string $tableName): float
    {
        DB::query('
            SELECT  round(((data_length + index_length) / 1024 / 1024), 2) as SIZE
            FROM information_schema.TABLES
            WHERE
                table_schema = \'' . DB::getConn()->getSelectedDatabase() . '\'
                AND table_name = \'' . $tableName . '\';
        ')->value();
    }

    public function getColumnSizeInMegabytes(string $tableName, string $fieldName): float
    {
        DB::query('
            SELECT round(sum(char_length(\'' . $tableName . '\')) / 1024 / 1024)
            FROM \'' . $tableName . '\';
        ')->value();
    }

    public function removeOldRowsFromTable(string $tableName, float $percentageToKeep)
    {
        $limit = $this->turnPercentageIntoLimit($tableName, $percentageToKeep);
        $sql = '
            DELETE FROM "' . $tableName . '"
            ORDER BY "ID" ASC
            LIMIT ' . $limit;
        $this->executeSql($sql);
    }

    public function removeOldColumnsFromTable(string $tableName, string $fieldName, float $percentageToKeep)
    {
        $limit = $this->turnPercentageIntoLimit($tableName, $percentageToKeep);
        $this->truncateField($tableName, $fieldName, $limit);
    }

    protected function turnPercentageIntoLimit(string $tableName, $percentageToKeep): int
    {
        $count = DB::query('SELECT COUNT(*) FROM "' . $tableName . '"')->value();

        return (int) round($percentageToKeep * $count);
    }

    protected function executeSql(string $sql)
    {
        FlushNow::do_flush('Running <pre>' . $sql . '</pre>');
        if ($this->forReal) {
            DB::query($sql);
            FlushNow::do_flush(' ... done', 'green');
        } else {
            FlushNow::do_flush(' ... not exectuted!', 'green');
        }
    }
}

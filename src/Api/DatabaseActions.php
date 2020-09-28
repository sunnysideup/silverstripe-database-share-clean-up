<?php

namespace Sunnysideup\DatabaseShareCleanUp\Api;
use SilverStripe\ORM\DB;
use Sunnysideup\Flush\FlushNow;
use SilverStripe\Control\HTTPRequest;
use SilverStripe\Core\Config\Configurable;
use SilverStripe\Core\Extensible;
use SilverStripe\Core\Injector\Injectable;

class DatabaseActions
{

    protected $forReal = false;

    public function setForReal(bool $bool)
    {
        $this->forReal = $bool;
    }

    public function truncateTable(string $tableName)
    {
        $sql = 'TRUNCATE TABLE "'.$tableName.'"; ';
        $this->executeSql($sql);
    }

    public function truncateField(string $tableName, string $fieldName, ?int $limit = 99999999)
    {
        $removeString = 'removed@'.uniqid() . '.' . uniqid();
        $sql = '
            UPDATE "'.$tableName.'"
            SET "'.$fieldName.'" = \''.$removeString.'\'
            ORDER BY "ID",
            LIMIT ' . $limit;
        $this->execute($sql);
    }


    public function getAllTables() : array
    {
        return DB::table_list();
    }


    public function getAllFieldsForOneTable(string $tableName) : array
    {
        return DB::field_list($tableName);
    }

    public function getTableSizeInMegaBytes(string $tableName) : float
    {
        DB::query('
            SELECT  round(((data_length + index_length) / 1024 / 1024), 2) as SIZE
            FROM information_schema.TABLES
            WHERE
                table_schema = \''.DB::conn()->getSelectedDatabase().'\'
                AND table_name = \''.$tableName.'\';
        ')->value();
    }

    public function getColumnSizeInMegabytes(string $tableName, string $fieldName) : float
    {
        DB::query('
            SELECT sum(char_length(\''.$tableName.'\'))
            FROM \''.$tableName.'\';;
        ')->value();

    }

    public function removeOldRowsFromTable(string $tableName, float $percentageToKeep)
    {
        $limit = $this->turnPercentageIntoLimit($tableName);
        $sql = '
            DELETE FROM "'.$tableName.'"
            ORDER BY "ID" ASC
            LIMIT '.$limit;
        $this->executeSql($sql);
    }

    public function removeOldColumnsFromTable(string $tableName, string $fieldName, float $percentageToKeep)
    {
        $limit = $this->turnPercentageIntoLimit($tableName);
        $this->truncateField($tableName, $fieldName, $limit);
    }

    protected function turnPercentageIntoLimit(string $tableName) : int
    {
        $count = DB::query('SELECT COUNT(*) FROM "'.$tableName.'"')->value();

        return round($percentageToKeep * $count);
    }


    protected function executeSql(string $sql)
    {
        FlushNow::do_flush('Running <pre>'.$sql.'</pre>');
        if($this->forReal) {
            DB::query($sql);
            FlushNow::do_flush(' ... done', 'green');
        } else {
            FlushNow::do_flush(' ... not exectuted!', 'green');
        }
    }

}

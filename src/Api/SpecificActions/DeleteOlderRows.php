<?php
namespace Sunnysideup\DatabaseShareCleanUp\Api\SpecificActions;

use Sunnysideup\DatabaseShareCleanUp\Api\DatabaseInfo;
use Sunnysideup\DatabaseShareCleanUp\Api\DatabaseActions;

use SilverStripe\ORM\DB;

use SilverStripe\Core\Config\Config;
use Sunnysideup\Flush\FlushNow;

class DeleteOlderRows extends DatabaseActions
{

    private static $post_fix_archive_tables = '_ARCHIVE';

    private static $tables_to_archive = [
        'LoginAttempt',
    ];

    public function archiveSelectedTables($tableName, string $agoString)
    {
        $toArchiveList = Config::inst()->get(self::class, 'tables_to_archive');
        if(in_array($tableName, $toArchiveList)) {
            $this->setTable($tableName);
            $this->copyTable($tableName);
            $this->moveRecords($tableName, $agoString);
        }

    }


    public function deleteBeforeDate(string $classNameOrTableName, ?string $dateAgoString = '-10 years', ?bool $archivOnly = false) : bool
    {
        $className = $this->getClassNameFromTableName($$classNameOrTableName);
        $results = [
            'Before' => 0,
            'After' => 0,
            'Notes' => [],
        ];
        $mainTable = $this->getTableForClassName($className);
        $classTables = [$mainTable];
        $ago = date('Y-m-d h:i:s', $dateAgoString);
        $results['Notes'][] = "Looking for all records that are older than {$ago}";
        foreach ($this->getSubTables($className) as $tableName) {
            $this->debugFlush("... DELETING OLD ENTRIES FROM {$tableName}", 'deleted');
            if($tableName === $mainTable) {
                $where = " \"LastEdited\" < '{$ago}'";
                $sql = "
                    DELETE {$tableName}.* FROM \"{$tableName}\" WHERE {$where};"
                ;
            } else {
                $sql = '
                    DELETE "'.$tableName.'".*
                    FROM "'.$tableName.'"
                        RIGHT JOIN "'.$mainTable.'"
                            ON "'.$tableName.'"."ID" = "'.$mainTable.'"."ID"
                    WHERE "'.$mainTable.'"."ID" IS NULL;

                ';
            }
            DB::query($sql);
            $this->debugFlush("... ".DB::get_conn()->affectedRows()." rows removed", 'deleted');
        }
        return true;
    }

    public function removeOldRowsFromTable(string $tableName, float $percentageToKeep, ?bool $archivOnly = false) : bool
    {
        $this->debugFlush('Deleting ' . (100 - round($percentageToKeep * 100, 2)) . '% of the Rows in ' . $tableName, 'obsolete');
        $limit = $this->turnPercentageIntoLimit($tableName, $percentageToKeep);
        $sortStatement = $this->getSortStatement($tableName);
        $sql = '
            DELETE FROM "' . $tableName . '"
            ' . $sortStatement . '
            LIMIT ' . $limit;
        $this->executeSql($sql);
        return true;
    }

    public function removeOldColumnsFromTable(string $tableName, string $fieldName, float $percentageToKeep, ?bool $archivOnly = false): bool
    {
        $this->debugFlush('Emptying ' . (100 - round($percentageToKeep * 100, 2)) . '% from ' . $tableName . '.' . $fieldName, 'obsolete');
        $limit = $this->turnPercentageIntoLimit($tableName, $percentageToKeep);

        return $this->truncateField($tableName, $fieldName, $limit, $silent = true);
    }



    protected function getArchiveTable(string $tableName) : string
    {
        return $tableName . Config::inst()->get(self::class, 'post_fix_archive_tables');
    }

    protected function copyTable(string $tableName)
    {
        $oldTable = $tableName;
        $newTable = $this->getArchiveTable($tableName);
        if(! $this->hasTable($newTable)) {
            DB::query('CREATE TABLE "'.$newTable.'" LIKE "'.$oldTable.'";');
        }
    }

    protected function getCutOffTimestamp(string $agoString) : int
    {
        return time() - strtotime($agoString);
    }

    protected function moveRecords(string $tableName, string $agoString)
    {
        $oldTable = $tableName;
        $newTable = $this->getArchiveTable($tableName);
        $where = ' WHERE UNIX_TIMESTAMP("'.$oldTable.'"."LastEdited")  > 0 AND UNIX_TIMESTAMP("'.$oldTable.'"."LastEdited") < '. $this->getCutOffTimestamp($agoString);
        $count = DB::query('SELECT COUNT(*) FROM "'.$oldTable.'" '.$where)->value();
        DB::alteration_message('Archiving '.$count.' records from '.$oldTable.' to '.$newTable,'created');
        DB::query('INSERT INTO "'.$newTable.'" SELECT * FROM "'.$oldTable.'" '.$where);
        DB::query('DELETE FROM "'.$oldTable.'" '.$where);
    }

}

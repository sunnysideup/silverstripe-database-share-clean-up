<?php
namespace Sunnysideup\DatabaseShareCleanUp\Api\SpecificActions;

use Sunnysideup\DatabaseShareCleanUp\Api\DatabaseInfo;
use Sunnysideup\DatabaseShareCleanUp\Api\DatabaseActions;

use SilverStripe\ORM\DB;
use Sunnysideup\Flush\FlushNow;

class DeleteOlderRows extends DatabaseActions
{


    public function deleteBeforeDate(string $classNameOrTableName, ?string $dateAgoString = '-10 years')
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
        $results['After'] = DB::query('
            SELECT COUNT("ID") FROM "'.$mainTable.'"
        ')->value();
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
}

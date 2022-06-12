<?php
namespace Sunnysideup\DatabaseShareCleanUp\Api\SpecificActions;

use Sunnysideup\DatabaseShareCleanUp\Api\DatabaseInfo;
use Sunnysideup\DatabaseShareCleanUp\Api\DatabaseActions;

use SilverStripe\ORM\DB;
use Sunnysideup\Flush\FlushNow;

class VersionedTables extends DatabaseActions
{

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

}

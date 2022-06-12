<?php

namespace Sunnysideup\DatabaseShareCleanUp\Api;

use SilverStripe\Control\Director;
use SilverStripe\Control\HTTPRequest;
use SilverStripe\Dev\BuildTask;
use Sunnysideup\DatabaseShareCleanUp\Api\DatabaseActions;
use Sunnysideup\DatabaseShareCleanUp\Api\DatabaseInfo;

use Sunnysideup\DatabaseShareCleanUp\Api\SpecificActions\Anonymiser;
use Sunnysideup\DatabaseShareCleanUp\Api\SpecificActions\DeleteOlderRows;
use Sunnysideup\DatabaseShareCleanUp\Api\SpecificActions\VersionedTables;
use Sunnysideup\Flush\FlushNow;
use SilverStripe\Core\Config\Configurable;
use SilverStripe\Core\Extensible;
use SilverStripe\Core\Injector\Injectable;

class DatabaseCleanupRunner
{
    use Injectable;
    use Configurable;
    use Extensible;

    private static $tables_to_delete_forever = [];

    private static $tables_to_be_cleaned = [
        'LoginAttempt',
    ];

    private static $fields_to_be_cleaned = [];

    private static $field_table_comboes_to_be_cleaned = [];

    private static $tables_to_keep = [];

    private static $fields_to_keep = [
        'ClassName',
        'Created',
        'LastEdited',
    ];

    private static $field_table_combos_to_keep = [];

    private static $max_table_size_in_mb = 20;

    private static $max_column_size_in_mb = 2;

    protected $data = [];

    protected $forReal = false;

    protected $debug = false;

    protected $anonymise = false;

    protected $removeObsolete = false;

    protected $removeOldVersions = false;

    protected $emptyFields = false;

    protected $removeRows = false;

    protected $selectedTablesOnly = false;

    protected $selectedTableList = [];

    protected $beforeDate = '';

    /**
     * configs from statics
     * @var [type]
     */
    protected
        $maxTableSize,
        $maxColumnSize,
        $tablesToDeleteForever,
        $tablesToKeep,
        $fieldsToKeep,
        $fieldTableCombosToKeep,
        $tablesToBeCleaned,
        $fieldsToBeCleaned ,
        $tableFieldCombosToBeCleaned
    ;

    public function setVar($name, $value)
    {
        $this->$name = $value;
        return $this;
    }

    public function getVar($name)
    {
        return $this->$name;
    }

    private static $dependencies = [
        'databaseActions' => '%$' . DatabaseActions::class,
        'databaseActionsOlderRows' => '%$' . DeleteOlderRows::class,
        'databaseActionsAnonymiser' => '%$' . Anonymiser::class,
        'databaseActionsVersionedTables' => '%$' . VersionedTables::class,
    ];

    /**
     * @var DatabaseActions
     */
    private $databaseActions;

    /**
     * @var DeleteOlderRows
     */
    private $databaseActionsAnonymiser;

    /**
     * @var Anonymiser
     */
    private $databaseActionsOlderRows;

    /**
     * @var VersionedTables
     */
    private $databaseActionsVersionedTables;

    public function setDatabaseActions($databaseActions)
    {
        $this->databaseActions = $databaseActions;

        return $this;
    }

    public function setDatabaseActionsOlderRows($databaseActionsOlderRows)
    {
        $this->databaseActionsOlderRows = $databaseActionsOlderRows;

        return $this;
    }

    public function setDatabaseActionsAnonymiser($databaseActionsAnonymiser)
    {
        $this->databaseActionsAnonymiser = $databaseActionsAnonymiser;

        return $this;
    }

    public function setDatabaseActionsVersionedTables($databaseActionsVersionedTables)
    {
        $this->databaseActionsVersionedTables = $databaseActionsVersionedTables;

        return $this;
    }

    protected function runInner() : array
    {
        $this->setVars();

        $tables = $this->databaseActions->getAllTables();
        foreach ($tables as $tableName) {
            $this->data[$tableName] = [
                'TableName' => $tableName,
                'SizeAfter' => 0,
                'SizeBefore' => 0,
                'Actions' => [],
            ];

            // skip tables to keep
            if (in_array($tableName, $this->tablesToKeep, true)) {
                if ($this->debug) {
                    $this->data[$tableName]['Actions'][] = 'Skipped because it is in list of tables to keep.';
                }

                continue;
            }

            // skip empty tables
            if ($this->databaseActions->isEmptyTable($tableName)) {
                if ($this->debug) {
                    $this->data[$tableName]['Actions'][] = 'Skipped because table is empty.';
                }

                continue;
            }
            // get size
            $this->data[$tableName]['SizeBefore'] = $this->databaseActions->getTableSizeInMegaBytes($tableName);

            // skip not selected ones
            if ($this->selectedTablesOnly && ! in_array($tableName, $this->selectedTableList, true)) {
                $this->data[$tableName]['Actions'][] = 'Skipped because it is not a selected table.';

                continue;
            }

            // delete tables to delete forever
            if (in_array($tableName, $this->tablesToDeleteForever, true)) {
                $outcome = $this->databaseActions->deleteTable($tableName);
                if ($outcome) {
                    $this->data[$tableName]['Actions'][] = 'DELETING FOREVER.';
                }

                continue;
            }

            // remove sobsolete tables
            if ($this->removeObsolete) {

                $outcome = $this->databaseActions->databaseActionsVersionedTables($tableName);
                if ($outcome) {
                    $this->data[$tableName]['Actions'][] = 'Deleted because it is obsolete.';
                }

                continue;
            }

            $this->tableMutations($tableName);

            $this->runFields($tableName);

            $this->removeRows($tableName);

            $this->data[$tableName]['SizeAfter'] = $this->databaseActions->getTableSizeInMegaBytes($tableName);
        }
        return $this->data;
    }


    protected function tableMutations(string $tableName)
    {
        // remove old vresions
        if ($this->removeOldVersions) {
            $outcome = $this->databaseActions->emptyVersionedTable($tableName);
            if ($outcome) {
                $this->data[$tableName]['Actions'][] = 'Remove all and replace with one entry for each record.';
            }
        }

        // anonimise
        if ($this->anonymise) {
            $outcome = $this->databaseActionsAnonymiser->AnonymiseTable($tableName);
            if ($outcome) {
                $this->data[$tableName]['Actions'][] = 'Anonymised Table.';
            }
        }
    }

    protected function runFields(string $tableName)
    {

        //get fields
        $fields = $this->databaseActions->getAllFieldsForOneTable($tableName);

        foreach ($fields as $fieldName) {
            if ('ID' === substr($fieldName, -2)) {
                if ($this->debug) {
                    $this->data[$tableName]['Actions'][] = ' ... ' . $fieldName . ': skipping!';
                }

                continue;
            }
            if (in_array($fieldName, $this->fieldsToKeep, true)) {
                if ($this->debug) {
                    $this->data[$tableName]['Actions'][] = ' ... ' . $fieldName . ': skipping (field is marked as KEEP)!';
                }

                continue;
            }

            $combo = $tableName . '.' . $fieldName;
            if (in_array($combo, $this->fieldTableCombosToKeep, true)) {
                if ($this->debug) {
                    $this->data[$tableName]['Actions'][] = ' ... ' . $fieldName . ': skipping (table.field is marked as KEEP).';
                }

                continue;
            }
            if ($this->anonymise) {
                $outcome = $this->databaseActionsAnonymiser->AnonymiseTableField($tableName, $fieldName);
                if ($outcome) {
                    $this->data[$tableName]['Actions'][] = ' ... ' . $fieldName . ': anonymised.';
                }
            }
            if ($this->emptyFields) {
                $columnSize = $this->databaseActions->getColumnSizeInMegabytes($tableName, $fieldName);
                $test1 = $columnSize > $this->maxColumnSize;
                $test2 = in_array($fieldName, $this->fieldsToBeCleaned, true);
                $test3 = in_array($combo, $this->tableFieldCombosToBeCleaned, true);
                if ($test1 || $test2 || $test3) {
                    $percentageToKeep = $test2 || $test3 ? 0 : $this->maxColumnSize / $columnSize;
                    $outcome = $this->databaseActionsOlderRows->removeOldColumnsFromTable($tableName, $fieldName, $percentageToKeep);
                    if ($outcome) {
                        $this->data[$tableName]['Actions'][] = ' ... ' . $fieldName . ': Removed most rows.';
                    }
                }
            }
        }
    }

    protected function removeRows(string $tableName)
    {

        // clean table
        if ($this->beforeDate) {
            $this->databaseActionsOlderRows->deleteBeforeDate($tableName, $this->beforeDate);
        }
        elseif ($this->removeRows) {
            $removeAllRows = in_array($tableName, $this->tablesToBeCleaned, true);
            if ($removeAllRows) {
                $this->databaseActionsOlderRows->removeOldRowsFromTable($tableName, 0.01);
                $this->data[$tableName]['Actions'][] = 'Removed most rows.';
            } else {
                $tableSize = $this->databaseActions->getTableSizeInMegaBytes($tableName);
                if ($tableSize > $this->maxTableSize) {
                    $percentageToKeep = $this->maxTableSize / $tableSize;
                    $this->databaseActionsOlderRows->removeOldRowsFromTable($tableName, $percentageToKeep);
                    $this->data[$tableName]['Actions'][] = 'Removed old rows.';
                }
            }
        }
    }

    protected function setVars()
    {
        $this->databaseActions->setForReal($this->forReal);
        $this->databaseActions->setDebug($this->debug);

        $this->maxTableSize = $this->Config()->get('max_table_size_in_mb');
        $this->maxColumnSize = $this->Config()->get('max_column_size_in_mb');

        $this->tablesToDeleteForever = $this->Config()->get('tables_to_delete_forever');

        $this->tablesToKeep = $this->Config()->get('tables_to_keep');
        $this->fieldsToKeep = $this->Config()->get('fields_to_keep');
        $this->fieldTableCombosToKeep = $this->Config()->get('field_table_combos_to_keep');

        $this->tablesToBeCleaned = $this->Config()->get('tables_to_be_cleaned');
        $this->fieldsToBeCleaned = $this->Config()->get('fields_to_be_cleaned');
        $this->tableFieldCombosToBeCleaned = $this->Config()->get('field_table_comboes_to_be_cleaned');
    }
}

<?php

namespace Sunnysideup\DatabaseShareCleanUp\Api;

use SilverStripe\Control\Director;
use SilverStripe\Control\HTTPRequest;
use SilverStripe\Dev\BuildTask;
use Sunnysideup\DatabaseShareCleanUp\Api\DatabaseActions;
use Sunnysideup\DatabaseShareCleanUp\Api\DatabaseInfo;

use Sunnysideup\DatabaseShareCleanUp\Api\SpecificActions\Anonymiser;
use Sunnysideup\DatabaseShareCleanUp\Api\SpecificActions\DeleteOlderRows;
use Sunnysideup\Flush\FlushNow;
use SilverStripe\Core\Config\Configurable;
use SilverStripe\Core\Extensible;
use SilverStripe\Core\Injector\Injectable;

class DatabaseCleanupRunner
{
    use Injectable;
    use Configurable;
    use Extensible;


    /**
     * Set a custom url segment (to follow dev/tasks/).
     *
     * @config
     *
     * @var string
     */
    private static $segment = 'database-share-clean-up';

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

    private static $dependencies = [
        'databaseActions' => '%$' . DatabaseActions::class,
        'databaseActionsOlderRows' => '%$' . DeleteOlderRows::class,
        'databaseActionsAnonymiser' => '%$' . Anonymiser::class,
    ];

    /**
     * @var DatabaseActions
     */
    private $databaseActions;

    /**
     * @var DeleteOlderRows
     */
    private $databaseActionsOlderRows;

    /**
    * @var Anonymiser
    */
    private $databaseActionsAnonymiser;


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


    protected $forReal = false;

    protected $anonymise = false;

    protected $removeObsolete = false;

    protected $removeOldVersions = false;

    protected $debug = false;

    protected $emptyFields = false;

    protected $removeRows = false;

    protected $selectedTables = false;

    protected $selectedTableList = [];

    protected $data = [];

    protected function runInner() : array
    {

        $this->databaseActions->setForReal($this->runner->set('forReal);
        $this->databaseActions->setDebug($this->runner->set('debug);

        $maxTableSize = $this->Config()->get('max_table_size_in_mb');
        $maxColumnSize = $this->Config()->get('max_column_size_in_mb');

        $tablesToDeleteForever = $this->Config()->get('tables_to_delete_forever');

        $tablesToKeep = $this->Config()->get('tables_to_keep');
        $fieldsToKeep = $this->Config()->get('fields_to_keep');
        $fieldTableCombosToKeep = $this->Config()->get('field_table_combos_to_keep');

        $tablesToBeCleaned = $this->Config()->get('tables_to_be_cleaned');
        $fieldsToBeCleaned = $this->Config()->get('fields_to_be_cleaned');
        $tableFieldCombosToBeCleaned = $this->Config()->get('field_table_comboes_to_be_cleaned');

        $tables = $this->databaseActions->getAllTables();
        foreach ($tables as $tableName) {
            $this->data[$tableName] = [
                'TableName' => $tableName,
                'SizeAfter' => 0,
                'SizeBefore' => 0,
                'Actions' => [],
            ];

            if (in_array($tableName, $tablesToKeep, true)) {
                if ($this->debug) {
                    $this->data[$tableName]['Actions'][] = 'Skipped because it is in list of tables to keep.';
                }

                continue;
            }
            if ($this->databaseActions->isEmptyTable($tableName)) {
                if ($this->debug) {
                    $this->data[$tableName]['Actions'][] = 'Skipped because table is empty.';
                }

                continue;
            }
            $this->data[$tableName]['SizeBefore'] = $this->databaseActions->getTableSizeInMegaBytes($tableName);
            if ($this->selectedTables && ! in_array($tableName, $this->selectedTableList, true)) {
                $this->data[$tableName]['Actions'][] = 'Skipped because it is not a selected table.';

                continue;
            }

            if ($this->removeObsolete) {
                if (in_array($tableName, $tablesToDeleteForever, true)) {
                    $this->databaseActions->deleteTable($tableName);
                    $this->data[$tableName]['Actions'][] = 'DELETING FOREVER.';

                    continue;
                }
                $outcome = $this->databaseActions->deleteObsoleteTables($tableName);
                if ($outcome) {
                    $this->data[$tableName]['Actions'][] = 'Deleted because it is obsolete.';
                }
            }

            if ($this->removeOldVersions) {
                $outcome = $this->databaseActions->emptyVersionedTable($tableName);
                if ($outcome) {
                    $this->data[$tableName]['Actions'][] = 'Remove all and replace with one entry for each record.';
                }
            }

            if ($this->anonymise) {
                $outcome = $this->databaseActionsAnonymiser->AnonymiseTable($tableName);
                if ($outcome) {
                    $this->data[$tableName]['Actions'][] = 'Anonymised Table.';
                }
            }
            //get fields
            $fields = $this->databaseActions->getAllFieldsForOneTable($tableName);

            foreach ($fields as $fieldName) {
                if ('ID' === substr($fieldName, -2)) {
                    if ($this->debug) {
                        $this->data[$tableName]['Actions'][] = ' ... ' . $fieldName . ': skipping!';
                    }

                    continue;
                }
                if (in_array($fieldName, $fieldsToKeep, true)) {
                    if ($this->debug) {
                        $this->data[$tableName]['Actions'][] = ' ... ' . $fieldName . ': skipping (field is marked as KEEP)!';
                    }

                    continue;
                }

                $combo = $tableName . '.' . $fieldName;
                if (in_array($combo, $fieldTableCombosToKeep, true)) {
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
                    $test1 = $columnSize > $maxColumnSize;
                    $test2 = in_array($fieldName, $fieldsToBeCleaned, true);
                    $test3 = in_array($combo, $tableFieldCombosToBeCleaned, true);
                    if ($test1 || $test2 || $test3) {
                        $percentageToKeep = $test2 || $test3 ? 0 : $maxColumnSize / $columnSize;
                        $outcome = $this->databaseActions->removeOldColumnsFromTable($tableName, $fieldName, $percentageToKeep);
                        if ($outcome) {
                            $this->data[$tableName]['Actions'][] = ' ... ' . $fieldName . ': Removed most rows.';
                        }
                    }
                }
            }

            // clean table
            if ($this->removeRows) {
                $removeAllRows = in_array($tableName, $tablesToBeCleaned, true);
                if ($removeAllRows) {
                    $this->databaseActionsOlderRows->removeOldRowsFromTable($tableName, 0.01);
                    $this->data[$tableName]['Actions'][] = 'Removed most rows.';
                } else {
                    $tableSize = $this->databaseActions->getTableSizeInMegaBytes($tableName);
                    if ($tableSize > $maxTableSize) {
                        $percentageToKeep = $maxTableSize / $tableSize;
                        $this->databaseActionsOlderRows->removeOldRowsFromTable($tableName, $percentageToKeep);
                        $this->data[$tableName]['Actions'][] = 'Removed old rows.';
                    }
                }
            }
            $this->data[$tableName]['SizeAfter'] = $this->databaseActions->getTableSizeInMegaBytes($tableName);
        }
        return $this->data;
    }
}

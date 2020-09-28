<?php

namespace Sunnysideup\DatabaseShareCleanUp;

use SilverStripe\Control\Director;
use SilverStripe\Control\HTTPRequest;
use SilverStripe\Dev\BuildTask;

use Sunnysideup\DatabaseShareCleanUp\Api\Anonymiser;
use Sunnysideup\DatabaseShareCleanUp\Api\DatabaseActions;
use Sunnysideup\Flush\FlushNow;

class CleanUp extends BuildTask
{
    /**
     * @var bool If set to FALSE, keep it from showing in the list
     * and from being executable through URL or CLI.
     */
    protected $enabled = true;

    /**
     * @var string Shown in the overview on the {@link TaskRunner}
     * HTML or CLI interface. Should be short and concise, no HTML allowed.
     */
    protected $title = 'Cleanup and anonymise database - CAREFUL! Data will be deleted.';

    /**
     * @var string Describe the implications the task has,
     * and the changes it makes. Accepts HTML formatting.
     */
    protected $description = 'Goes through database and deletes data that may expose personal information and bloat database.';

    protected $forReal = false;

    private static $fields_to_be_cleaned = [];

    private static $tables_to_be_cleaned = [
        'SearchHistory'
    ];

    private static $tables_to_keep = [];

    private static $fields_to_keep = [
        'ClassName',
        'Created',
        'LastEdited',
    ];

    private static $field_table_combos_to_keep = [];

    private static $days_back = 100;

    private static $max_table_size_in_mb = 20;

    private static $max_column_size_in_mb = 5;

    private static $dependencies = [
        'anonymiser' => '%$' . Anonymiser::class,
        'database' => '%$' . DatabaseActions::class,
    ];

    private $anonymiser;

    public function setAnonymiser($a)
    {
        $this->anonymiser = $a;
    }


    private $database;

    public function setDatabase($b)
    {
        $this->database = $b;
    }


    /**
     * Set a custom url segment (to follow dev/tasks/)
     *
     * @config
     * @var string
     */
    private static $segment = 'database-share-clean-up';

    /**
     * @return bool
     */
    public function isEnabled()
    {
        return Director::isDev();
    }

    /**
     * Implement this method in the task subclass to
     * execute via the TaskRunner
     *
     * @param HTTPRequest $request
     */
    public function run($request)
    {

        if ($request->getVar('forreal') === 'yes') {
            $this->forReal = true;
            FlushNow::do_flush('<h1>Running in For Real Mode! (for real mode)</h1>', 'bad');
        } else {
            FlushNow::do_flush('<h1>Please add ?forreal=yes to run for real!</h1>', 'good');
        }

        $this->anonymiser->setDatabaseActions($this->database);
        $this->database->setForReal($this->forReal);

        $maxTableSize = $this->Config()->get('max_table_size_in_mb');
        $maxColumnSize = $this->Config()->get('max_column_size_in_mb');
        $tablesToKeep = $this->Config()->get('tables_to_keep');
        $fieldsToKeep = $this->Config()->get('fields_to_keep');
        $fieldTableCombosToKeep = $this->Config()->get('field_table_combos_to_keep');

        $tablesToDelete = $this->Config()->get('tables_to_be_cleaned');
        $fieldsToBeCleaned = $this->Config()->get('fields_to_be_cleaned');


        $tables = $this->database->getAllTables();
        foreach ($tables as $tableName) {
            if (in_array($tableName, $tablesToKeep, true)) {
                continue;
            }

            //get fields
            $fields = $this->database->getAllFieldsForOneTable($tableName);

            //anonymise
            $this->anonymiser->AnonymiseTable($tableName, $fields, $this->forReal);

            foreach ($fields as $fieldName) {
                if(strpos($fieldName, 'ID') !== false) {
                    continue;
                }
                $combo = $tableName . '.' . $fieldName;
                if (in_array($fieldName, $fieldsToKeep, true)) {
                    continue;
                }
                if (in_array($combo, $fieldTableCombosToKeep, true)) {
                    continue;
                }
                $columnSize = $this->database->getColumnSizeInMegabytes($tableName, $fieldName);
                if ($columnSize > $maxColumnSize || in_array($fieldName, $fieldsToBeCleaned, true)) {
                    // $percentageToKeep = $maxColumnSize / $columnSize;
                    $this->database->removeOldColumnsFromTable($tableName, $fieldName, 0.05);
                }
            }

            // clean table
            $tableSize = $this->database->getTableSizeInMegaBytes($tableName);
            if ($tableSize > $maxTableSize || in_array($tableName, $tablesToDelete, true)) {
                $percentageToKeep = $maxTableSize / $tableSize;
                $this->database->removeOldRowsFromTable($tableName, $percentageToKeep);
            }
        }
    }
}

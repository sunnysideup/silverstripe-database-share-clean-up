<?php

namespace Sunnysideup\DatabaseShareCleanUp;

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

    private static $tables_to_be_cleaned = [];

    private static $tables_to_keep = [];

    private static $fields_to_keep = [];

    private static $field_table_combos_to_keep = [];

    private static $days_back = 100;

    private static $max_table_size_in_mb = 5;

    private static $max_column_size_in_mb = 1;

    private static $dependencies = [
        'anonymiser' => '%$' . Anonymiser::class,
        'database' => '%$' . DatabaseActions::class,
    ];

    private $anonymiser;

    private $database;

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
        $this->anonymiser->setDatabaseActions($this->database);
        $this->database->setForReal($this->forReal);

        $maxTableSize = $this->Config()->get('max_table_size_in_mb');
        $maxColumnSize = $this->Config()->get('max_column_size_in_mb');
        $tablesToKeep = $this->Config()->get('tables_to_keep');
        $fieldsToKeep = $this->Config()->get('fields_to_keep');
        $fieldTableCombosToKeep = $this->Config()->get('field_table_combos_to_keep');
        if ($request->getVar('forreal') === 'yes') {
            $this->forReal = true;
            FlushNow::do_flush('Running in For Real Mode! (for real mode)', 'bad');
        } else {
            FlushNow::do_flush('Please add ?forreal=yes to run for real!', 'good');
        }
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
                $combo = $tableName . '.' . $fieldName;
                if (in_array($tableName, $fieldsToKeep, true)) {
                    continue;
                }
                if (in_array($combo, $fieldTableCombosToKeep, true)) {
                    continue;
                }
                $columnSize = $this->database->getColumnSizeInMegabytes($tableName, $fieldName);
                if ($maxColumnSize > $columnSize) {
                    $percentageToKeep = $maxColumnSize / $columnSize;
                    $this->database->removeOldColumnsFromTable($tableName, $fieldName, $percentageToKeep);
                }
            }

            // clean table
            $tableSize = $this->database->getTableSizeInMegaBytes($tableName);
            if ($tableSize > $maxTableSize) {
                $percentageToKeep = $maxTableSize / $tableSize;
                $this->database->removeOldRowsFromTable($tableName, $percentageToKeep);
            }
        }
    }
}

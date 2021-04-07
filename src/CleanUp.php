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
     * @var bool if set to FALSE, keep it from showing in the list
     *           and from being executable through URL or CLI
     */
    protected $enabled = true;

    /**
     * @var string Shown in the overview on the {@link TaskRunner}
     *             HTML or CLI interface. Should be short and concise, no HTML allowed.
     */
    protected $title = 'Cleanup and anonymise database - CAREFUL! Data will be deleted.';

    /**
     * @var string Describe the implications the task has,
     *             and the changes it makes. Accepts HTML formatting.
     */
    protected $description = 'Goes through database and deletes data that may expose personal information and bloat database.';

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
        'anonymiser' => '%$' . Anonymiser::class,
        'database' => '%$' . DatabaseActions::class,
    ];

    private $anonymiser;

    private $database;

    /**
     * Set a custom url segment (to follow dev/tasks/).
     *
     * @config
     *
     * @var string
     */
    private static $segment = 'database-share-clean-up';

    public function setAnonymiser($anonymiser)
    {
        $this->anonymiser = $anonymiser;
        return $this;
    }

    public function setDatabase($database)
    {
        $this->database = $database;
        return $this;
    }

    /**
     * @return bool
     */
    public function isEnabled()
    {
        return Director::isDev();
    }

    /**
     * Implement this method in the task subclass to
     * execute via the TaskRunner.
     *
     * @param HTTPRequest $request
     */
    public function run($request)
    {
        $this->anonymise = (bool) $request->getVar('anonymise');
        $this->removeObsolete = (bool) $request->getVar('removeobsolete');
        $this->removeOldVersions = (bool) $request->getVar('removeoldversions');
        $this->removeRows = (bool) $request->getVar('removerows');
        $this->emptyFields = (bool) $request->getVar('emptyfields');

        $this->selectedTables = (bool) $request->getVar('selectedtables');
        $this->debug = (bool) $request->getVar('debug');
        $this->forReal = (bool) $request->getVar('forreal');
        if ($this->forReal) {
            $this->debug = true;
        }
        $this->selectedTableList = $request->getVar('selectedtablelist') ?? [];

        $this->anonymiser->setDatabaseActions($this->database);
        $this->database->setForReal($this->forReal);
        $this->database->setDebug($this->debug);

        if ($this->forReal) {
            FlushNow::do_flush('<h3>Running in FOR REAL mode</h3>', 'bad');
        } else {
            FlushNow::do_flush('<h3>Not runing FOR REAL</h3>', 'good');
        }
        if ($this->anonymise) {
            $this->anonymiser->AnonymisePresets();
        }

        $this->createForm();
        $this->runInner();
        $this->createTable();
    }

    protected function runInner()
    {
        $maxTableSize = $this->Config()->get('max_table_size_in_mb');
        $maxColumnSize = $this->Config()->get('max_column_size_in_mb');

        $tablesToDeleteForever = $this->Config()->get('tables_to_delete_forever');

        $tablesToKeep = $this->Config()->get('tables_to_keep');
        $fieldsToKeep = $this->Config()->get('fields_to_keep');
        $fieldTableCombosToKeep = $this->Config()->get('field_table_combos_to_keep');

        $tablesToBeCleaned = $this->Config()->get('tables_to_be_cleaned');
        $fieldsToBeCleaned = $this->Config()->get('fields_to_be_cleaned');
        $tableFieldCombosToBeCleaned = $this->Config()->get('field_table_comboes_to_be_cleaned');

        $tables = $this->database->getAllTables();
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
            if ($this->database->isEmptyTable($tableName)) {
                if ($this->debug) {
                    $this->data[$tableName]['Actions'][] = 'Skipped because table is empty.';
                }

                continue;
            }
            $this->data[$tableName]['SizeBefore'] = $this->database->getTableSizeInMegaBytes($tableName);
            if ($this->selectedTables && ! in_array($tableName, $this->selectedTableList, true)) {
                $this->data[$tableName]['Actions'][] = 'Skipped because it is not a selected table.';

                continue;
            }

            if ($this->removeObsolete) {
                if (in_array($tableName, $tablesToDeleteForever, true)) {
                    $this->database->deleteTable($tableName);
                    $this->data[$tableName]['Actions'][] = 'DELETING FOREVER.';

                    continue;
                }
                $outcome = $this->database->deleteObsoleteTables($tableName);
                if ($outcome) {
                    $this->data[$tableName]['Actions'][] = 'Deleted because it is obsolete.';
                }
            }

            if ($this->removeOldVersions) {
                $outcome = $this->database->emptyVersionedTable($tableName);
                if ($outcome) {
                    $this->data[$tableName]['Actions'][] = 'Remove all and replace with one entry for each record.';
                }
            }

            if ($this->anonymise) {
                $outcome = $this->anonymiser->AnonymiseTable($tableName);
                if ($outcome) {
                    $this->data[$tableName]['Actions'][] = 'Anonymised Table.';
                }
            }
            //get fields
            $fields = $this->database->getAllFieldsForOneTable($tableName);

            foreach ($fields as $fieldName) {
                if ('ID' === substr($fieldName, -2)) {
                    if ($this->debug) {
                        $this->data[$tableName]['Actions'][] = ' ... ' . $fieldName . ': skipping!';
                    }

                    continue;
                }
                if (in_array($fieldName, $fieldsToKeep, true)) {
                    if ($this->debug) {
                        $this->data[$tableName]['Actions'][] = ' ... ' . $fieldName . ': skipping!';
                    }

                    continue;
                }

                $combo = $tableName . '.' . $fieldName;
                if (in_array($combo, $fieldTableCombosToKeep, true)) {
                    if ($this->debug) {
                        $this->data[$tableName]['Actions'][] = ' ... ' . $fieldName . ': skipping.';
                    }

                    continue;
                }
                if ($this->anonymise) {
                    $outcome = $this->anonymiser->AnonymiseTableField($tableName, $fieldName);
                    if ($outcome) {
                        $this->data[$tableName]['Actions'][] = ' ... ' . $fieldName . ': anonymised.';
                    }
                }
                if ($this->emptyFields) {
                    $columnSize = $this->database->getColumnSizeInMegabytes($tableName, $fieldName);
                    $test1 = $columnSize > $maxColumnSize;
                    $test2 = in_array($fieldName, $fieldsToBeCleaned, true);
                    $test3 = in_array($combo, $tableFieldCombosToBeCleaned, true);
                    if ($test1 || $test2 || $test3) {
                        $percentageToKeep = $test2 || $test3 ? 0 : $maxColumnSize / $columnSize;
                        $outcome = $this->database->removeOldColumnsFromTable($tableName, $fieldName, $percentageToKeep);
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
                    $this->database->removeOldRowsFromTable($tableName, 0.01);
                    $this->data[$tableName]['Actions'][] = 'Removed most rows.';
                } else {
                    $tableSize = $this->database->getTableSizeInMegaBytes($tableName);
                    if ($tableSize > $maxTableSize) {
                        $percentageToKeep = $maxTableSize / $tableSize;
                        $this->database->removeOldRowsFromTable($tableName, $percentageToKeep);
                        $this->data[$tableName]['Actions'][] = 'Removed old rows.';
                    }
                }
            }
            $this->data[$tableName]['SizeAfter'] = $this->database->getTableSizeInMegaBytes($tableName);
        }
    }

    protected function createForm()
    {
        $anonymise = $this->anonymise ? 'checked="checked"' : '';
        $removeObsolete = $this->removeObsolete ? 'checked="checked"' : '';
        $removeOldVersions = $this->removeOldVersions ? 'checked="checked"' : '';
        $removeRows = $this->removeRows ? 'checked="checked"' : '';
        $emptyFields = $this->emptyFields ? 'checked="checked"' : '';
        $selectedTables = $this->selectedTables ? 'checked="checked"' : '';
        $forReal = $this->forReal ? 'checked="checked"' : '';
        $debug = $this->debug ? 'checked="checked"' : '';
        echo <<<html
        <h3>All sizes in Megabytes</h3>
        <form method="get">
            <div style="
                background-color: pink;
                width: 300px;
                padding: 1vw;
                position: fixed;
                right: 0;
                top: 0;
                bottom: 0;
                border: 1px solid red;
            ">
                <h4>What actions to take?</h4>
                <div class="field" style="padding: 10px;">
                    <input type="checkbox" name="anonymise" {$anonymise} />
                    <label>anonymise</label>
                </div>
                <div class="field" style="padding: 10px;">
                    <input type="checkbox" name="removeoldversions" {$removeOldVersions} />
                    <label>remove versioned table entries</label>
                </div>
                <div class="field" style="padding: 10px;">
                    <input type="checkbox" name="removeobsolete" {$removeObsolete} />
                    <label>remove obsolete tables</label>
                </div>

                <div class="field" style="padding: 10px;">
                    <input type="checkbox" name="emptyfields" {$emptyFields} />
                    <label>empty large fields</label>
                </div>

                <div class="field" style="padding: 10px;">
                    <input type="checkbox" name="removerows" {$removeRows} />
                    <label>remove old rows if there are too many (not recommended)</label>
                </div>

                <hr />
                <h4>How to apply?</h4>
                <div class="field" style="padding: 10px;">
                    <input type="checkbox" name="selectedtables" {$selectedTables} />
                    <label>apply to selected tables only?</label>
                </div>
                <div class="field" style="padding: 10px;">
                    <input type="checkbox" name="forreal" {$forReal} />
                    <label>do it for real?</label>
                </div>
                <hr />
                <h4>See more info?</h4>
                <div class="field" style="padding: 10px;">
                    <input type="checkbox" name="debug" {$debug} />
                    <label>debug</label>
                </div>
                <hr />
                <div class="field" style="padding: 10px;">
                    <input type="submit" value="let's do it!" />
                </div>
            </div>


html;
    }

    protected function createTable()
    {
        $tbody = '';
        $totalSizeBefore = 0;
        $totalSizeAfter = 0;
        usort(
            $this->data,
            function ($a, $b) {
                return $a['SizeAfter'] <=> $b['SizeAfter'];
            }
        );
        foreach ($this->data as $data) {
            $totalSizeBefore += $data['SizeBefore'];
            $totalSizeAfter += $data['SizeAfter'];
            $actions = '';
            if (count($data['Actions'])) {
                $actions = '
                        <ul>
                            <li>
                            ' . implode('</li><li>', $data['Actions']) . '
                            </li>
                        </ul>';
            }
            $tableList = empty($this->selectedTableList[$data['TableName']]) ? '' : 'checked="checked"';
            $tbody .= '
                <tr>
                    <td>
                        <input type="checkbox" name="selectedtablelist[]" value="' . $data['TableName'] . '" ' . $tableList . ' />
                    </td>
                    <td>
                        ' . $data['TableName'] . '
                        ' . $actions . '
                    </td>
                    <td style="text-align: center;">
                        ' . $data['SizeBefore'] . '
                    </td>
                    <td style="text-align: center;">
                        ' . $data['SizeAfter'] . '
                    </td>
                </tr>';
        }
        $tfoot = '
                <tr>
                    <th>
                        &nbsp;
                    </th>
                    <th>
                        TOTAL
                    </th>
                    <th>
                        ' . $totalSizeBefore . '
                    </th>
                    <th>
                        ' . $totalSizeAfter . '
                    </th>
                </tr>
        ';
        echo '
        <table border="1" style="width: calc(100% - 380px);">
            <thead>
                <tr>
                    <th width="5%">
                        Select
                    </th>
                    <th width="65%">
                        Table Name
                    </th>
                    <th style="text-align: center;" width="15%">
                        Before
                    </th>
                    <th style="text-align: center;" width="15%">
                        After
                    </th>
                </tr>
            </thead>
            <tfoot>' . $tfoot . '</tfoot>
            <tbody>' . $tbody . '</tbody>
        </table>
    </form>';
    }
}

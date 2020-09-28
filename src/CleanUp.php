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

    protected $anonymise = false;

    protected $debug = false;

    protected $emptyFields = false;

    protected $removeRows = false;

    protected $data = [];

    private static $fields_to_be_cleaned = [];

    private static $tables_to_be_cleaned = [
        'LoginAttempt',
    ];

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
        $this->forReal = $request->getVar('forreal') ? true : false;
        $this->anonymise = $request->getVar('anonymise') ? true : false;
        $this->debug = $request->getVar('debug') ? true : false;
        $this->emptyFields = $request->getVar('emptyfields') ? true : false;

        $this->anonymiser->setDatabaseActions($this->database);
        $this->database->setForReal($this->forReal);
        $this->database->setDebug($this->debug);

        if ($this->forReal) {
            FlushNow::do_flush('<h3>Running in FOR REAL mode</h3>', 'bad');
        } else {
            FlushNow::do_flush('<h3>Not runing FOR REAL</h3>', 'good');
        }
        echo $this->getForm();

        $this->runInner();
        $this->createTable();

    }

    protected function runInner()
    {
        $maxTableSize = $this->Config()->get('max_table_size_in_mb');
        $maxColumnSize = $this->Config()->get('max_column_size_in_mb');
        $tablesToKeep = $this->Config()->get('tables_to_keep');
        $fieldsToKeep = $this->Config()->get('fields_to_keep');
        $fieldTableCombosToKeep = $this->Config()->get('field_table_combos_to_keep');

        $tablesToBeCleaned = $this->Config()->get('tables_to_be_cleaned');
        $fieldsToBeCleaned = $this->Config()->get('fields_to_be_cleaned');

        $this->database->deleteAllObsoleteTables();

        $tables = $this->database->getAllTables();
        foreach ($tables as $tableName) {
            $this->data[$tableName] = [
                'TableName' => $tableName,
                'SizeBefore' => $this->database->getTableSizeInMegaBytes($tableName),
            ];
            if (in_array($tableName, $tablesToKeep, true)) {
                continue;
            }

            //get fields
            $fields = $this->database->getAllFieldsForOneTable($tableName);

            if($this->anonymise) {
                //anonymise
                $this->anonymiser->AnonymiseTable($tableName, $fields, $this->forReal);
            }

            if($this->emptyFields) {
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
                        $this->database->emptyFieldsColumnsFromTable($tableName, $fieldName, 0.05);
                    }
                }
            }

            // clean table
            $tableSize = $this->database->getTableSizeInMegaBytes($tableName);
            $deleteAll = in_array($tableName, $tablesToDelete, true);
            if($deleteAll) {
                $this->database->emptyFieldsRowsFromTable($tableName, 0.01);
            } elseif ($this->removeRows && $tableSize > $maxTableSize) {
                $percentageToKeep = $maxTableSize / $tableSize;
                $this->database->emptyFieldsRowsFromTable($tableName, $percentageToKeep);
            }
            $this->data[$tableName]['SizeAfter'] = $this->database->getTableSizeInMegaBytes($tableName);
        }
    }

    protected function getForm()
    {
        $forreal = empty($_GET['forreal']) ? '' : 'checked="checked"';
        $anonymise = empty($_GET['anonymise']) ? '' : 'checked="checked"';
        $debug = empty($_GET['debug']) ? '' : 'checked="checked"';
        $emptyFields = empty($_GET['emptyfields']) ? '' : 'checked="checked"';
        $removeRows = empty($_GET['removerows']) ? '' : 'checked="checked"';
        return <<<html
            <h1>All sizes in Megabytes</h1>
            <form method="get" style="
                background-color: pink;
                width: 300px;
                height: 300px;
                border-radius: 50%;
                padding: 1vw;
                position: fixed;
                right: 20px;
                top: 20px;
                border: 1px solid red;
                "
            >   <div style="  position: absolute;top: 50%; left: 50%;transform: translate(-50%, -50%);">

                    <div class="field" style="padding: 10px;">
                        <input type="checkbox" name="anonymise" $anonymise />
                        <label>anonymise?</label>
                    </div>
                    <div class="field" style="padding: 10px;">
                        <input type="checkbox" name="emptyfields" $removeRows />
                        <label>empty large fields?</label>
                    </div>
                    <div class="field" style="padding: 10px;">
                        <input type="checkbox" name="removerows" $emptyFields />
                        <label>empty large fields?</label>
                    </div>
                    <div class="field" style="padding: 10px;">
                        <input type="checkbox" name="debug" $debug />
                        <label>debug?</label>
                    </div>
                    <div class="field" style="padding: 10px;">
                        <input type="submit" value="run it now ..." />
                    </div>
                    <div class="field" style="padding: 10px;">
                        <input type="checkbox" name="forreal" $forreal />
                        <label>do it for real?</label>
                    </div>
                </div>
            </form>

html;
    }

    protected function createTable()
    {
        $tbody = '';
        $totalSizeBefore = 0;
        $totalSizeAfter = 0;
        usort(
            $this->data,
            function($a, $b) {
                return $a['SizeAfter'] <=> $b['SizeAfter'];
            }
        );
        foreach($this->data as $table =>$data) {
            $totalSizeBefore += $data['SizeBefore'];
            $totalSizeAfter += $data['SizeAfter'];
            $tbody .= '
                <tr>
                    <td>
                        '.$data['TableName'].'
                    </td>
                    <td style="text-align: center;">
                        '.$data['SizeBefore'].'
                    </td>
                    <td style="text-align: center;">
                        '.$data['SizeAfter'].'
                    </td>
                </tr>';
        }
        $tfoot = '
                <tr>
                    <th width="50%">
                        TOTAL
                    </th>
                    <th width="25%">
                        '.$totalSizeAfter.'
                    </th>
                    <th width="25%">
                        '.$totalSizeAfter.'
                    </th>
                </tr>
        ';
        echo '<table border="1" style="width: calc(100% - 380px);">
            <thead>
                <tr>
                    <th>
                        Table Name
                    </th>

                    <th style="text-align: center;">
                        Before
                    </th>
                    <th style="text-align: center;">
                        After
                    </th>
                </tr>
            </thead>
            <tfoot>'.$tfoot.'</tfoot>
            <tbody>'.$tbody.'</tbody>
        </table>';
    }
}

<?php

namespace Sunnysideup\DatabaseShareCleanUp\Tasks;

use SilverStripe\Control\Director;
use SilverStripe\Control\HTTPRequest;
use SilverStripe\Dev\BuildTask;

use SilverStripe\Core\Injector\Injector;
use Sunnysideup\DatabaseShareCleanUp\Api\DatabaseActions;
use Sunnysideup\DatabaseShareCleanUp\Api\DatabaseInfo;

use Sunnysideup\DatabaseShareCleanUp\Api\SpecificActions\Anonymiser;
use Sunnysideup\DatabaseShareCleanUp\Api\SpecificActions\DeleteOlderRows;

use Sunnysideup\DatabaseShareCleanUp\Api\DatabaseCleanupRunner;
use Sunnysideup\Flush\FlushNow;

class CleanUp extends BuildTask
{

    protected $data = [];

    protected $runner = null;

    /**
     * Set a custom url segment (to follow dev/tasks/).
     *
     * @config
     *
     * @var string
     */
    private static $segment = 'database-share-clean-up';

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

    /**
     * Implement this method in the task subclass to
     * execute via the TaskRunner.
     *
     * @param HTTPRequest $request
     */
    public function run($request)
    {
        $this->runner = Injector::inst()->get(DatabaseCleanupRunner::class);

        $this->runner->setVar('debug', (bool) $request->getVar('debug'));
        $this->runner->setVar('forReal', (bool) $request->getVar('forreal'));

        $this->runner->setVar('anonymise', (bool) $request->getVar('anonymise'));
        $this->runner->setVar('removeObsolete', (bool) $request->getVar('removeobsolete'));
        $this->runner->setVar('removeOldVersions', (bool) $request->getVar('removeoldversions'));
        $this->runner->setVar('removeRows', (bool) $request->getVar('removerows'));
        $this->runner->setVar('emptyFields', (bool) $request->getVar('emptyfields'));
        $this->runner->setVar('selectedTableList', ($request->getVar('selectedtablelist') ?: []));
        $this->runner->setVar('selectedTablesOnly', (bool) $request->getVar('selectedtablesonly'));
        $this->runner->setVar('beforeDate', (string) $request->getVar('beforedate'));
        $this->runner->setVar('archiveRatherThanDelete', (string) $request->getVar('archiveratherthandelete'));

        if(! empty($request->getVar('selectedtablelist') )) {
            $this->runner->setVar('selectedTablesOnly', true);
        }

        if ($request->getVar('forreal')) {
            $this->runner->setVar('debug', true);
            FlushNow::do_flush('<h3>Running in FOR REAL mode</h3>', 'bad');
        } else {
            FlushNow::do_flush('<h3>Not runing FOR REAL</h3>', 'good');
        }

        $this->createForm();
        $this->data = $this->runner->runForAllTables();
        $this->createTable();
    }

    protected function createForm()
    {
        $anonymise = $this->runner->getVar('anonymise') ? 'checked="checked"' : '';
        $removeObsolete = $this->runner->getVar('removeObsolete') ? 'checked="checked"' : '';
        $removeOldVersions = $this->runner->getVar('removeOldVersions') ? 'checked="checked"' : '';
        $removeRows = $this->runner->getVar('removeRows') ? 'checked="checked"' : '';
        $emptyFields = $this->runner->getVar('emptyFields') ? 'checked="checked"' : '';
        $selectedTablesOnly = $this->runner->getVar('selectedtablesonly') ? 'checked="checked"' : '';
        $forReal = $this->runner->getVar('forReal') ? 'checked="checked"' : '';
        $debug = $this->runner->getVar('debug') ? 'checked="checked"' : '';
        $beforeDate = $this->runner->getVar('beforeDate') ? 'value="'.$this->runner->getVar('beforeDate').'"' : '';
        $archiveRatherThanDelete = $this->runner->getVar('archiveratherthandelete') ? 'checked="checked"' : '';
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
                <div class="field" style="padding: 10px;">
                    <input type="text" name="beforeDate" {$beforeDate} /><br />
                    <label>remove old rows before a date</label>
                </div>

                <hr />
                <h4>How to apply?</h4>
                <div class="field" style="padding: 10px;">
                    <input type="checkbox" name="selectedtablesonly" {$selectedTablesOnly} />
                    <label>apply to selected tables only?</label>
                </div>
                <div class="field" style="padding: 10px;">
                    <input type="checkbox" name="archiveRatherThanDelete" {$archiveRatherThanDelete} />
                    <label>archive rather than delete</label>
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
                return $b['SizeBefore'] <=> $a['SizeBefore'];
            }
        );
        foreach ($this->data as $row) {
            $totalSizeBefore += $row['SizeBefore'];
            $totalSizeAfter += $row['SizeAfter'];
            $actions = '';
            if (count($row['Actions'])) {
                $actions = '
                        <ul>
                            <li>
                            ' . implode('</li><li>', $row['Actions']) . '
                            </li>
                        </ul>';
            }
            $selectedTablesOnly = $this->runner->get('selectedTableList');
            $tableList = empty($selectedTablesOnly['TableName']) ? '' : 'checked="checked"';
            $tbody .= '
                <tr>
                    <td>
                        <input type="checkbox" name="selectedtablelist[]" value="' . $row['TableName'] . '" ' . $tableList . ' />
                    </td>
                    <td>
                        ' . $row['TableName'] . '
                        ' . $actions . '
                    </td>
                    <td style="text-align: center;">
                        ' . $row['SizeBefore'] . '
                    </td>
                    <td style="text-align: center;">
                        ' . $row['SizeAfter'] . '
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

<?php

namespace Sunnysideup\DatabaseShareCleanUp;

use Override;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use SilverStripe\PolyExecution\PolyOutput;
use SilverStripe\Control\Director;
use SilverStripe\Core\Convert;
use SilverStripe\Core\Environment;
use SilverStripe\Dev\BuildTask;
use SilverStripe\ORM\DB;
use Sunnysideup\DatabaseShareCleanUp\Api\Anonymiser;
use Sunnysideup\DatabaseShareCleanUp\Api\DatabaseActions;

class CleanUp extends BuildTask
{
    /**
     * @var bool if set to FALSE, keep it from showing in the list
     *           and from being executable through URL or CLI
     */
    private static bool $is_enabled = true;

    /**
     * @var string Shown in the overview on the {@link TaskRunner}
     *             HTML or CLI interface. Should be short and concise, no HTML allowed.
     */
    protected string $title = 'Cleanup and anonymise database - CAREFUL! Data will be deleted. INCLUDES HANDY LIST OF TABLE SIZES';

    /**
     * @var string Describe the implications the task has,
     *             and the changes it makes. Accepts HTML formatting.
     */
    protected static string $description = 'Goes through database and deletes data that may expose personal information and bloat database.';

    protected $forReal = false;

    protected $anonymise = false;

    protected $keepAdmins = false;

    protected $removeObsolete = false;

    protected $removeOldVersions = false;

    protected $debug = false;

    protected $emptyFields = false;

    protected $removeRows = false;

    protected $selectedTables = false;

    protected $selectedTableList = [];

    protected $data = [];

    private static $tables_to_delete_forever = [];

    private static $tables_to_be_cleaned = [];

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
    protected static string $commandName = 'database-share-clean-up';

    /**
     * Name of the (hidden) form field that holds the token.
     */
    protected const FORM_TOKEN_NAME = 'cleanuptoken';

    /**
     * Dropping tables, truncating versions and removing rows can not be undone.
     *
     *  - never on a live site
     *  - command line: always (non-live)
     *  - browser: only in dev mode AND with a valid token
     *    (the token is part of the form, so following a plain link can not trigger it)
     */
    protected function mayRunForReal(InputInterface $input, PolyOutput $output): bool
    {
        if (Director::isLive()) {
            $output->writeln('"For real" can not be run on a live site.');

            return false;
        }

        if (Director::is_cli()) {
            return true;
        }

        if (! Director::isDev()) {
            $output->writeln('"For real" can only be run in the browser when the site is in dev mode (or on the command line).');

            return false;
        }

        if (! $this->checkFormToken((string) $input->getOption(self::FORM_TOKEN_NAME))) {
            $output->writeln('"For real" needs a valid token: please submit using the form below (not a link). The token is valid for about one to two hours.');

            return false;
        }

        return true;
    }

    /**
     * Token that proves the "for real" request came from our own form.
     *
     * Not based on the Silverstripe session / SecurityID: this task writes output
     * straight away (flushing), so the session (and its cookie) can not be saved and
     * a session based token never validates. This token is stateless: a hash of
     * the current hour and a secret that only this site knows.
     */
    protected function getFormToken(int $hoursAgo = 0): string
    {
        $secret = implode('|', [
            BASE_PATH,
            (string) Environment::getEnv('SS_DATABASE_SERVER'),
            (string) Environment::getEnv('SS_DATABASE_USERNAME'),
            (string) Environment::getEnv('SS_DATABASE_PASSWORD'),
            (string) DB::get_conn()->getSelectedDatabase(),
        ]);

        return hash_hmac('sha256', (string) (floor(time() / 3600) - $hoursAgo), $secret);
    }

    protected function checkFormToken(string $token): bool
    {
        if ('' === $token) {
            return false;
        }

        // current hour or the one before
        return hash_equals($this->getFormToken(0), $token) || hash_equals($this->getFormToken(1), $token);
    }

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

    #[Override]
    public function getOptions(): array
    {
        return [
            new InputOption('anonymise', 'a', InputOption::VALUE_NONE, 'Anonymise data'),
            new InputOption('keepadmins', 'k', InputOption::VALUE_NONE, 'Do not anonymise administrators (used with --anonymise)'),
            new InputOption('removeobsolete', 'o', InputOption::VALUE_NONE, 'Remove obsolete tables'),
            new InputOption('removeoldversions', 'm', InputOption::VALUE_NONE, 'Remove versioned table entries'),
            new InputOption('removerows', 'r', InputOption::VALUE_NONE, 'Remove old rows if there are too many (not recommended)'),
            new InputOption('emptyfields', 'e', InputOption::VALUE_NONE, 'Empty large fields'),
            new InputOption('selectedtables', 's', InputOption::VALUE_NONE, 'Apply to selected tables only'),
            new InputOption('selectedtablelist', 'l', InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY, 'List of selected tables'),
            new InputOption('debug', 'd', InputOption::VALUE_NONE, 'Enable debug mode'),
            new InputOption('forreal', 'F', InputOption::VALUE_NONE, 'Execute for real (not a dry run)'),
            new InputOption(self::FORM_TOKEN_NAME, null, InputOption::VALUE_REQUIRED, 'Token of the form (browser only, needed to run for real in the browser)', ''),
        ];
    }

    /**
     * Implement this method in the task subclass to
     * execute via the TaskRunner.
     */
    protected function execute(InputInterface $input, PolyOutput $output): int
    {
        // this can take a while, especially in the browser
        Environment::increaseTimeLimitTo(0);
        Environment::increaseMemoryLimitTo('1024M');

        $this->anonymise = (bool) $input->getOption('anonymise');
        $this->keepAdmins = (bool) $input->getOption('keepadmins');
        $this->removeObsolete = (bool) $input->getOption('removeobsolete');
        $this->removeOldVersions = (bool) $input->getOption('removeoldversions');
        $this->removeRows = (bool) $input->getOption('removerows');
        $this->emptyFields = (bool) $input->getOption('emptyfields');
        $this->selectedTables = (bool) $input->getOption('selectedtables');
        $this->debug = (bool) $input->getOption('debug');
        $this->forReal = (bool) $input->getOption('forreal');
        if ($this->forReal) {
            $this->debug = true;
            if (! $this->mayRunForReal($input, $output)) {
                return Command::FAILURE;
            }
        }

        $this->selectedTableList = $input->getOption('selectedtablelist') ?? [];
        $this->anonymiser->setDatabaseActions($this->database);
        $this->database->setForReal($this->forReal);
        $this->database->setDebug($this->debug);
        $this->database->setKeepAdmins($this->keepAdmins);
        if ($this->forReal) {
            $output->writeForHtml('<h3>Running in FOR REAL mode</h3>', 'bad');
        } else {
            $output->writeForHtml('<h3>Not runing FOR REAL</h3>', 'good');
        }

        if ($this->anonymise) {
            $this->anonymiser->AnonymisePresets();
        }

        $this->createFormInner($output);
        $this->runInner();
        $this->createReport($output);
        $this->createTableInner($output);
        $output->writeln('MEMORY USED:' . $this->formatBytes(memory_get_peak_usage()));
        return Command::SUCCESS;
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
            if (! $this->database->tableExists($tableName)) {
                continue;
            }

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

            if ($this->removeOldVersions) {
                $outcome = $this->database->emptyVersionedTable($tableName);
                if ($outcome) {
                    $this->data[$tableName]['Actions'][] = 'Remove all versions.';
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
                if (str_ends_with((string) $fieldName, 'ID')) {
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
                    $outcome = $this->anonymiser->AnonymiseTableField($tableName, $fieldName);
                    if ($outcome) {
                        $last = $this->database->getLastAnonymiseLogEntry();
                        $this->data[$tableName]['Actions'][] = ' ... ' . $fieldName . ': anonymised'
                            . ($last ? ' as ' . $last['method'] . ' (' . $last['rows'] . ' rows)' : '') . '.';
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

    protected function createFormInner(PolyOutput $output)
    {
        $anonymise = $this->anonymise ? 'checked="checked"' : '';
        $removeObsolete = $this->removeObsolete ? 'checked="checked"' : '';
        $removeOldVersions = $this->removeOldVersions ? 'checked="checked"' : '';
        $removeRows = $this->removeRows ? 'checked="checked"' : '';
        $emptyFields = $this->emptyFields ? 'checked="checked"' : '';
        $selectedTables = $this->selectedTables ? 'checked="checked"' : '';
        $forReal = $this->forReal ? 'checked="checked"' : '';
        $keepAdmins = $this->keepAdmins ? 'checked="checked"' : '';
        $tokenName = self::FORM_TOKEN_NAME;
        $tokenValue = $this->getFormToken();
        $debug = $this->debug ? 'checked="checked"' : '';
        $html = <<<html
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
                    <input type="checkbox" name="keepadmins" {$keepAdmins} />
                    <label>keep admins (do not anonymise administrators)</label>
                    <input type="hidden" name="{$tokenName}" value="{$tokenValue}" />
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
        $output->writeForHtml($html);
    }

    /**
     * What was (or, in a dry run, would be) done, and what else to consider.
     */
    protected function createReport(PolyOutput $output)
    {
        $options = [];
        foreach ([
            'anonymise' => $this->anonymise,
            'keep admins' => $this->keepAdmins,
            'remove versioned table entries' => $this->removeOldVersions,
            'remove obsolete tables' => $this->removeObsolete,
            'empty large fields' => $this->emptyFields,
            'remove old rows' => $this->removeRows,
            'selected tables only' => $this->selectedTables,
        ] as $label => $on) {
            if ($on) {
                $options[] = $label;
            }
        }

        $output->writeln($this->forReal ? 'FOR REAL: the database has been changed.' : 'DRY RUN: nothing has been changed. Add --forreal (or tick "do it for real?") to apply.');
        $output->writeln('Actions: ' . ([] === $options ? 'none selected' : implode(', ', $options)) . '.');

        $html = '<div style="width: calc(100% - 380px);"><h3>Summary</h3>';
        if ($this->anonymise && $this->keepAdmins) {
            $adminIds = $this->database->getAdminIds();
            $line = 'Keeping ' . count($adminIds) . ' administrator(s) in: '
                . implode(', ', array_keys((array) $this->database->config()->get('member_tables_to_protect'))) . '.';
            $output->writeln($line);
        }

        if ($this->anonymise) {
            $log = $this->database->getAnonymiseLog();
            $output->writeln('Anonymised fields: ' . count($log));
            if ([] === $log) {
                $html .= '<p><em>No fields were anonymised.</em></p>';
            } else {
                $html .= '<h3>Anonymised fields (' . count($log) . ')</h3><table border="1" style="width: 100%;"><thead><tr>'
                    . '<th>Table.Field</th><th>Method</th><th>Rows</th><th>Example of new value</th>'
                    . '</tr></thead><tbody>';
                foreach ($log as $entry) {
                    $output->writeln(' - ' . $entry['table'] . '.' . $entry['field'] . ': ' . $entry['method'] . ' (' . (int) $entry['rows'] . ' rows)');
                    $html .= '<tr><td>' . Convert::raw2xml($entry['table'] . '.' . $entry['field']) . '</td>'
                        . '<td>' . Convert::raw2xml($entry['method']) . '</td>'
                        . '<td style="text-align: right;">' . (int) $entry['rows'] . '</td>'
                        . '<td><code>' . Convert::raw2xml($entry['example']) . '</code></td></tr>';
                }

                $html .= '</tbody></table>';
            }

            $suggestions = $this->anonymiser->getSuggestions();
            if ([] !== $suggestions) {
                $output->writeln('Fields that look sensitive but were NOT anonymised (' . count($suggestions) . '), add them to your yml to anonymise them:');
                $output->writeln($this->suggestionsAsYml($suggestions));
                $html .= '<h3>Fields that look sensitive but were NOT anonymised (' . count($suggestions) . ')</h3>'
                    . '<p>To anonymise any of these, add them to your <code>app/_config</code> yml. '
                    . 'To leave them alone on purpose, add them to <code>keep_table_field_combos</code>. '
                    . 'See <code>_config/database-share-clean-up.yml.example</code> in the module for all options.</p>'
                    . '<pre style="background: #eee; padding: 10px;">'
                    . Convert::raw2xml($this->suggestionsAsYml($suggestions))
                    . '</pre>';
            }
        }

        $html .= '</div>';
        $output->writeForHtml($html);
    }

    protected function suggestionsAsYml(array $suggestions): string
    {
        $yml = Anonymiser::class . ":\n  also_remove_table_field_combos:\n";
        foreach ($suggestions as $combo => $method) {
            $yml .= '    ' . $combo . ': ' . $method . "\n";
        }

        return $yml;
    }

    protected function createTableInner(PolyOutput $output)
    {
        $tbody = '';
        $totalSizeBefore = 0;
        $totalSizeAfter = 0;
        usort(
            $this->data,
            fn ($a, $b) => $b['SizeBefore'] <=> $a['SizeBefore']
        );
        foreach ($this->data as $data) {
            $totalSizeBefore += $data['SizeBefore'];
            $totalSizeAfter += $data['SizeAfter'];
            $actions = '';
            if (count($data['Actions']) > 0) {
                $actions = '
                        <ul>
                            <li>
                            ' . implode('</li><li>', $data['Actions']) . '
                            </li>
                        </ul>';
            }

            $tableList = in_array($data['TableName'], (array) $this->selectedTableList, true) ? 'checked="checked"' : '';
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
        $html = '
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
        $output->writeForHtml($html);
    }

    private function formatBytes($size, $precision = 2)
    {
        $base = log($size, 1024);
        $suffixes = ['', 'K', 'M', 'G', 'T'];

        return round(1024 ** ($base - floor($base)), $precision) . ' ' . $suffixes[floor($base)];
    }
}

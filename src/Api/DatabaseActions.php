<?php

namespace Sunnysideup\DatabaseShareCleanUp\Api;

use RuntimeException;
use SilverStripe\Core\Config\Configurable;
use SilverStripe\Core\Convert;
use SilverStripe\Core\Injector\Injectable;
use SilverStripe\ORM\DB;
use SilverStripe\Security\Member;
use SilverStripe\Security\Permission;
use Sunnysideup\Flush\FlushNowImplementor;

class DatabaseActions
{
    use Injectable;
    use Configurable;

    /**
     * @var string[]
     */
    protected const TEXT_FIELDS = [
        'varchar',
        'text',
        'mediumtext',
    ];

    /**
     * @var string[]
     */
    protected const DATE_FIELDS = [
        'date',
    ];

    /**
     * Members with any of these permission codes are treated as administrators
     * (see `setKeepAdmins`).
     *
     * @var string[]
     */
    private static $admin_permission_codes = ['ADMIN'];

    /**
     * Members in groups with any of these codes are treated as administrators
     * (see `setKeepAdmins`).
     *
     * @var string[]
     */
    private static $admin_group_codes = ['administrators'];

    /**
     * Tables that hold member rows => the column that holds the Member ID.
     * Rows for administrators are skipped when anonymising these tables
     * and keeping admins.
     *
     * @var array
     */
    private static $member_tables_to_protect = [
        'Member' => 'ID',
        'Member_Live' => 'ID',
        'Member_Versions' => 'RecordID',
    ];

    /**
     * Tables from which no rows are removed when keeping administrators
     * (otherwise "remove old rows" could strip admin group membership).
     *
     * @var string[]
     */
    private static $tables_not_to_shrink_when_keeping_admins = [
        'Group',
        'Group_Members',
        'Group_Roles',
        'Permission',
        'PermissionRole',
        'PermissionRoleCode',
        'Member',
    ];

    protected $forReal = false;

    protected $debug = false;

    protected $keepAdmins = false;

    /**
     * What has been anonymised so far, for reporting:
     * [['table' =>, 'field' =>, 'method' =>, 'rows' =>, 'example' =>], ...]
     *
     * @var array
     */
    protected $anonymiseLog = [];

    /**
     * SQL expression used for the field that is currently being anonymised.
     *
     * @var string
     */
    protected $currentExpr = '';

    /**
     * WHERE clause used for the field that is currently being anonymised.
     *
     * @var string
     */
    protected $currentWhere = '';

    /**
     * @var int[]|null
     */
    protected $adminIds;

    protected static $tableList = [];

    protected static $fieldsForTable = [];

    public function setForReal(bool $bool)
    {
        $this->forReal = $bool;
    }

    public function setDebug(bool $bool)
    {
        $this->debug = $bool;
    }

    /**
     * When set to true, administrators are not anonymised.
     * Admins are worked out immediately, so before any rows are removed.
     */
    public function setKeepAdmins(bool $bool): self
    {
        $this->keepAdmins = $bool;
        $this->adminIds = null;
        if ($bool) {
            $this->getAdminIds();
        }

        return $this;
    }

    public function getKeepAdmins(): bool
    {
        return $this->keepAdmins;
    }

    /**
     * @return int[]
     */
    public function getAdminIds(): array
    {
        if (null === $this->adminIds) {
            $ids = [];
            $permissionCodes = (array) static::config()->get('admin_permission_codes');
            if ([] !== $permissionCodes) {
                $ids = array_merge($ids, Permission::get_members_by_permission($permissionCodes)->column('ID'));
            }

            $groupCodes = (array) static::config()->get('admin_group_codes');
            if ([] !== $groupCodes) {
                $ids = array_merge($ids, Member::get()->filter('Groups.Code', $groupCodes)->column('ID'));
            }

            $this->adminIds = array_values(array_unique(array_map('intval', $ids)));
        }

        return $this->adminIds;
    }

    public function emptyVersionedTable(string $tableName, ?bool $leaveLastVersion = false): bool
    {
        $specialCase = in_array($tableName, ['ChangeSet', 'ChangeSetItem', 'ChangeSetItem_ReferencedBy'], true);
        if (str_ends_with($tableName, '_Versions') || $specialCase) {
            $nonVersionedTable = substr($tableName, 0, strlen($tableName) - 9);
            if ($this->hasTable($nonVersionedTable) || $specialCase) {
                $this->truncateTable($tableName);
                if ($leaveLastVersion) {
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
                }

                return true;
            }

            FlushNowImplementor::do_flush('ERROR: could not find: ' . $nonVersionedTable, 'bad');
        }

        return false;
    }

    public function deleteObsoleteTables(string $tableName): bool
    {
        if (str_starts_with($tableName, '_obsolete_')) {
            $this->deleteTable($tableName);

            return true;
        }

        return false;
    }

    public function deleteTable(string $tableName)
    {
        $this->debugFlush('Deleting ' . $tableName . ' as it is not required', 'deleted');
        $sql = 'DROP TABLE "' . $tableName . '";';
        $this->executeSql($sql);
    }

    public function truncateTable(string $tableName)
    {
        $this->debugFlush('Emptying ' . $tableName, 'changed');
        $sql = 'TRUNCATE TABLE "' . $tableName . '"; ';
        $this->executeSql($sql);
    }

    public function truncateField(string $tableName, string $fieldName, ?int $limit = 99999999, ?bool $silent = false): bool
    {
        if ($this->isTextField($tableName, $fieldName) || $this->isDateField($tableName, $fieldName)) {
            if (false === $silent) {
                $this->debugFlush('Emptying ' . $tableName . '.' . $fieldName, 'obsolete');
            }

            $sortStatement = $this->getSortStatement($tableName);
            $sql = '
                UPDATE "' . $tableName . '"
                SET "' . $fieldName . '" = \'\'
                ' . $sortStatement . '
                LIMIT ' . $limit;
            $this->executeSql($sql);

            return true;
        }

        $this->debugFlush('Skipping emptying ' . $tableName . '.' . $fieldName . ' as this is not a text field', 'info');

        return false;
    }

    private function getRandomCharExpr(): string
    {
        return "SUBSTR('0123456789abcdefghijklmnopqrstuvwxyz',(RAND()*35)+1,1)";
    }

    private function r(int $count = 1): string
    {
        $r = $this->getRandomCharExpr();
        return implode(', ', array_fill(0, $count, $r));
    }

    /**
     * Builds the UPDATE statement used by (almost) all anonymise methods.
     * - the new value is cut to the length of the column (if it has a length)
     * - empty values are left alone
     * - admins are left alone (if so requested)
     */
    private function updateSql(string $tableName, string $fieldName, string $expr): string
    {
        $maxLength = $this->getMaxLength($tableName, $fieldName);
        if ($maxLength > 0) {
            $expr = 'LEFT(' . $expr . ', ' . $maxLength . ')';
        }

        $where = '"' . $fieldName . '" IS NOT NULL AND "' . $fieldName . '" <> \'\'' . $this->adminExclusionSql($tableName);
        $this->currentExpr = $expr;
        $this->currentWhere = $where;

        return '
        UPDATE "' . $tableName . '"
        SET "' . $fieldName . '" = ' . $expr . '
        WHERE ' . $where;
    }

    /**
     * Returns the maximum length of a char / varchar column, or 0 if there is none.
     */
    private function getMaxLength(string $tableName, string $fieldName): int
    {
        $details = $this->getAllFieldsForOneTableDetails($tableName);
        if (isset($details[$fieldName]) && preg_match('/^(?:var)?char\((\d+)\)/i', (string) $details[$fieldName], $matches)) {
            return (int) $matches[1];
        }

        return 0;
    }

    private function upper(): string
    {
        return "SUBSTR('ABCDEFGHIJKLMNOPQRSTUVWXYZ',(RAND()*25)+1,1)";
    }

    /**
     * When keeping admins, returns SQL (to append to a WHERE) that
     * excludes the rows of administrators from member tables.
     */
    private function adminExclusionSql(string $tableName): string
    {
        if (! $this->keepAdmins) {
            return '';
        }

        $tables = (array) static::config()->get('member_tables_to_protect');
        $idColumn = $tables[$tableName] ?? null;
        $adminIds = $this->getAdminIds();
        if (! $idColumn || [] === $adminIds) {
            return '';
        }

        $this->debugFlush('Keeping ' . count($adminIds) . ' admin(s) in ' . $tableName, 'info');

        return ' AND "' . $idColumn . '" NOT IN (' . implode(', ', $adminIds) . ')';
    }

    /*
     * The anonymise methods below create SHORT, reasonable values on purpose:
     * we do not want to bloat the database with long made-up values.
     * Each method has an example of what it creates.
     */

    protected function anonymiseEmail(string $tableName, string $fieldName): void
    {
        // e.g. k3x9qa@a.co
        $expr = 'CONCAT(' . $this->r(6) . ", '@a.co')";
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseUsername(string $tableName, string $fieldName): void
    {
        $this->anonymiseEmail($tableName, $fieldName);
    }

    protected function anonymiseFirstName(string $tableName, string $fieldName): void
    {
        // e.g. Kxqa
        $expr = 'CONCAT(' . $this->upper() . ', ' . $this->r(3) . ')';
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseSurname(string $tableName, string $fieldName): void
    {
        $this->anonymiseFirstName($tableName, $fieldName);
    }

    protected function anonymiseLastName(string $tableName, string $fieldName): void
    {
        $this->anonymiseFirstName($tableName, $fieldName);
    }

    protected function anonymiseCity(string $tableName, string $fieldName): void
    {
        // e.g. Kxqa9
        $expr = 'CONCAT(' . $this->upper() . ', ' . $this->r(4) . ')';
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseSuburb(string $tableName, string $fieldName): void
    {
        $this->anonymiseCity($tableName, $fieldName);
    }

    protected function anonymiseAddress(string $tableName, string $fieldName): void
    {
        // e.g. 42 Kxq St
        $expr = 'CONCAT(FLOOR(RAND()*99)+1, \' \', ' . $this->upper() . ', ' . $this->r(2) . ', \' St\')';
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseStreet(string $tableName, string $fieldName): void
    {
        // e.g. Kxq St
        $expr = 'CONCAT(' . $this->upper() . ', ' . $this->r(2) . ', \' St\')';
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseAddress2(string $tableName, string $fieldName): void
    {
        // e.g. Unit 42
        $expr = "CONCAT('Unit ', FLOOR(RAND()*99)+1)";
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseCompany(string $tableName, string $fieldName): void
    {
        // e.g. Kxqa Ltd
        $expr = 'CONCAT(' . $this->upper() . ', ' . $this->r(3) . ', \' Ltd\')';
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseCompanyName(string $tableName, string $fieldName): void
    {
        $this->anonymiseCompany($tableName, $fieldName);
    }

    protected function anonymisePhone(string $tableName, string $fieldName): void
    {
        // e.g. 094567890
        $expr = "CONCAT('09', FLOOR(RAND()*9000000)+1000000)";
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseMobile(string $tableName, string $fieldName): void
    {
        // e.g. 0214567890
        $expr = "CONCAT('021', FLOOR(RAND()*9000000)+1000000)";
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseFax(string $tableName, string $fieldName): void
    {
        $this->anonymisePhone($tableName, $fieldName);
    }

    protected function anonymiseDob(string $tableName, string $fieldName): void
    {
        // note: string literals must use single quotes (double quotes are identifiers in ANSI mode)
        $expr = 'DATE_FORMAT(
            DATE_ADD(\'1970-01-01\', INTERVAL FLOOR(RAND()*12783) DAY),
            \'%d/%m/%Y\'
        )';
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseDateOfBirth(string $tableName, string $fieldName): void
    {
        $expr = 'DATE_ADD(\'1970-01-01\', INTERVAL FLOOR(RAND()*12783) DAY)';
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    /**
     * Anonymises free text (e.g. an email subject or body): replaces it with a random string.
     */
    protected function anonymiseText(string $tableName, string $fieldName): void
    {
        // e.g. anon k3x9
        $expr = "CONCAT('anon ', " . $this->r(4) . ')';
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }

    protected function anonymiseIp(string $tableName, string $fieldName): void
    {
        $expr = "CONCAT(
        FLOOR(RAND()*253)+1, '.',
        FLOOR(RAND()*255), '.',
        FLOOR(RAND()*255), '.',
        FLOOR(RAND()*253)+1
    )";
        $this->executeSql($this->updateSql($tableName, $fieldName, $expr));
    }


    protected function anonymiseIpAddress(string $tableName, string $fieldName): void
    {
        $this->anonymiseIp($tableName, $fieldName);
    }

    protected function anonymiseProxy(string $tableName, string $fieldName): void
    {
        $this->anonymiseIp($tableName, $fieldName);
    }

    public function removeOldRowsFromTable(string $tableName, float $percentageToKeep)
    {
        if ($this->keepAdmins && in_array($tableName, (array) static::config()->get('tables_not_to_shrink_when_keeping_admins'), true)) {
            $this->debugFlush('Not removing rows from ' . $tableName . ' (keeping admins)', 'info');

            return;
        }

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

    public function getAllTables(?bool $fresh = true): array
    {
        if ($fresh || 0 === count(self::$tableList)) {
            self::$tableList = DB::table_list();
        }

        return self::$tableList;
    }

    public function getAllFieldsForOneTable(string $tableName): array
    {
        return array_keys($this->getAllFieldsForOneTableDetails($tableName));
    }

    public function getAllFieldsForOneTableDetails(string $tableName): array
    {
        if (! isset(self::$fieldsForTable[$tableName])) {
            self::$fieldsForTable[$tableName] = [];
            if ($this->hasTable($tableName)) {
                self::$fieldsForTable[$tableName] = DB::field_list($tableName);
            }
        }

        return self::$fieldsForTable[$tableName];
    }

    public function isEmptyTable(string $tableName): bool
    {
        if ($this->tableExists($tableName)) {
            return 0 === $this->countRows($tableName);
        }

        return true;
    }

    public function countRows(string $tableName): int
    {
        return (int) DB::query('SELECT COUNT(*) FROM "' . $tableName . '";')->value();
    }

    public function tableExists(string $tableName): bool
    {
        // escape _ and % as they are wildcards in LIKE
        $like = Convert::raw2sql(addcslashes($tableName, '_%\\'));

        return (bool) DB::query("SHOW TABLES LIKE '" . $like . "';")->value();
    }

    public function getTableSizeInMegaBytes(string $tableName): float
    {
        return floatval(DB::query('
            SELECT  round(((data_length + index_length ) / 1024 / 1024), 2) as C
            FROM information_schema.TABLES
            WHERE
                table_schema = \'' . DB::get_conn()->getSelectedDatabase() . '\'
                AND table_name = \'' . $tableName . '\';
        ')->value());
    }

    public function getColumnSizeInMegabytes(string $tableName, string $fieldName): float
    {
        return floatval(DB::query('
            SELECT round(sum(char_length("' . $fieldName . '")) / 1024 / 1024)
            FROM "' . $tableName . '";
        ')->value());
    }

    protected function isTextField(string $tableName, string $fieldName): bool
    {
        return $this->isSomeTypeOfField($tableName, $fieldName, self::TEXT_FIELDS);
    }

    protected function isDateField(string $tableName, string $fieldName): bool
    {
        return $this->isSomeTypeOfField($tableName, $fieldName, self::DATE_FIELDS);
    }

    protected function isSomeTypeOfField(string $tableName, string $fieldName, array $typeStrings): bool
    {
        $details = $this->getAllFieldsForOneTableDetails($tableName);
        if (isset($details[$fieldName])) {
            foreach ($typeStrings as $test) {
                if (0 === stripos(strtolower($details[$fieldName]), (string) $test)) {
                    return true;
                }
            }
        } else {
            FlushNowImplementor::do_flush('ERROR: could not find: ' . $tableName . '.' . $fieldName, 'bad');
        }

        return false;
    }

    protected function turnPercentageIntoLimit(string $tableName, float $percentageToKeep): int
    {
        $count = DB::query('SELECT COUNT("ID") FROM "' . $tableName . '"')->value();
        $count = intval($count);

        return (int) round($percentageToKeep * $count);
    }

    protected function executeSql(string $sql)
    {
        $this->debugFlush('Running <pre>' . $sql . '</pre>', 'info');
        if ($this->forReal) {
            DB::query($sql);
            $this->debugFlush(' ... done', 'green');
        } else {
            $this->debugFlush(' ... not exectuted!', 'info');
        }
    }

    protected function getSortStatement(string $tableName): string
    {
        if ($this->hasField($tableName, 'ID')) {
            return 'ORDER BY "ID" ASC';
        }

        return '';
    }

    protected function hasField(string $tableName, string $fieldName): bool
    {
        return (bool) DB::get_schema()->hasField($tableName, $fieldName);
    }

    protected function hasTable(string $tableName): bool
    {
        return (bool) DB::get_schema()->hasTable($tableName);
    }

    protected function debugFlush(string $message, string $type)
    {
        if ($this->debug) {
            FlushNowImplementor::do_flush($message, $type);
        }
    }

    /**
     * Calls the appropriate anonymise method and logs what it did.
     */
    public function CallAnonymiseMethod(string $fieldType, string $tableName, string $fieldName): bool
    {
        $method = 'anonymise' . $fieldType;
        if (!method_exists($this, $method)) {
            throw new RuntimeException(sprintf("Anonymisation method '%s' does not exist on ", $method) . static::class);
        }

        $this->currentExpr = '';
        $this->currentWhere = '';
        $this->$method($tableName, $fieldName);

        $rows = 0;
        $example = '';
        if ('' !== $this->currentExpr) {
            $rows = (int) DB::query('SELECT COUNT(*) FROM "' . $tableName . '" WHERE ' . $this->currentWhere)->value();
            if ($rows > 0) {
                $example = (string) DB::query('SELECT ' . $this->currentExpr)->value();
            }
        }

        $this->anonymiseLog[] = [
            'table' => $tableName,
            'field' => $fieldName,
            'method' => $fieldType,
            'rows' => $rows,
            'example' => $example,
        ];

        return true;
    }

    /**
     * @return array [['table' =>, 'field' =>, 'method' =>, 'rows' =>, 'example' =>], ...]
     */
    public function getAnonymiseLog(): array
    {
        return $this->anonymiseLog;
    }

    public function getLastAnonymiseLogEntry(): array
    {
        return $this->anonymiseLog ? end($this->anonymiseLog) : [];
    }
}

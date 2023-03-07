<?php

namespace Sunnysideup\DatabaseShareCleanUp\Api;

use SilverStripe\Core\Config\Configurable;
use SilverStripe\Core\Extensible;
use SilverStripe\Core\Injector\Injectable;

class Anonymiser
{
    use Injectable;
    use Configurable;
    use Extensible;

    public $databaseActions;

    protected $databaseAction;

    /**
     * @var array
     */
    private static $fields_to_anonymise = [
        'Email',
        'Username',
        'City',
        'Suburb',
        'Address',
        'Street',
        'Address2',
        'Phone',
        'Mobile',
        'FirstName',
        'Surname',
        'LastName',
        'Fax',
        'Dob',
        'DOB',
        'DateOfBirth',
        'Ip',
        'IP',
        'IpAddress',
        'Proxy',
    ];

    /**
     * @var array
     */
    private static $tables_to_remove = [
        'MemberPassword',
        'LoginAttempt',
    ];

    /**
     * @var array
     */
    private static $tables_to_keep = [];

    /**
     * specify tables with fields that are not to be deleted
     * e.g.
     *    MyTable.MyField,
     *    MyTable2.MyField2,.
     *
     * @var array
     */
    private static $keep_table_field_combos = [];

    /**
     * @var array
     */
    private static $also_remove_table_field_combos = [];

    public function setDatabaseActions($databaseActions)
    {
        $this->databaseActions = $databaseActions;
    }

    public function AnonymiseTable(string $tableName): bool
    {
        $tables = $this->Config()->get('tables_to_remove');
        if (in_array($tableName, $tables, true)) {
            $this->databaseActions->truncateTable($tableName);

            return true;
        }

        return false;
    }

    public function AnonymiseTableField(string $tableName, string $fieldName): bool
    {
        if (in_array($tableName, $this->Config()->get('tables_to_keep'), true)) {
            return false;
        }
        $keepCombos = $this->Config()->get('keep_table_field_combos');

        // $combo = $tableName . '.' . $fieldName;
        if (in_array($fieldName, $keepCombos, true)) {
            return false;
        }

        $fieldsToDelete = $this->Config()->get('fields_to_anonymise');
        foreach ($fieldsToDelete as $fieldTest) {
            if (false !== strpos($fieldName, $fieldTest)) {
                return $this->databaseActions->anonymiseField($tableName, $fieldName);
            }
        }

        return false;
    }

    public function AnonymisePresets()
    {
        $also = $this->Config()->get('also_remove_table_field_combos');
        foreach ($also as $combo) {
            list($tableName, $fieldName) = explode('.', $combo);
            $this->databaseActions->anonymiseField($tableName, $fieldName);
        }
    }

    // look for any field called Email / Phone / Address / FirstName /
}

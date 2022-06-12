<?php

namespace Sunnysideup\DatabaseShareCleanUp\Api\SpecificActions;

use SilverStripe\Core\Config\Configurable;
use SilverStripe\Core\Extensible;
use SilverStripe\Core\Injector\Injectable;

use Sunnysideup\DatabaseShareCleanUp\Api\DatabaseActions;

class Anonymiser extends DatabaseActions
{

    /**
     * @var array
     */
    private static $fields_to_anonymise = [
        'Email',
        'Username',
        'City',
        'Suburb',
        'Address',
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
    ];

    /**
     * @var array
     */
    private static $tables_to_remove = [
        'MemberPassword',
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

    public function AnonymiseTable(string $tableName): bool
    {
        $tables = $this->Config()->get('tables_to_remove');
        if (in_array($tableName, $tables, true)) {
            $this->truncateTable($tableName);

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
                return $this->anonymiseField($tableName, $fieldName);
            }
        }

        return false;
    }

    public function AnonymisePresets()
    {
        $also = $this->Config()->get('also_remove_table_field_combos');
        foreach ($also as $combo) {
            list($tableName, $fieldName) = explode('.', $combo);
            $this->anonymiseField($tableName, $fieldName);
        }
    }

    public function anonymiseField(string $tableName, string $fieldName): bool
    {
        if ($this->isTextField($tableName, $fieldName)) {
            $this->debugFlush('Anonymising ' . $tableName . '.' . $fieldName, 'repaired');
            // $sortStatement = $this->getSortStatement($tableName);
            $r = "SUBSTR('0123456789abcdefghihjlmnopqrstuvwxyz',(RAND()*35)+1,1)";
            $sql = '
                UPDATE "' . $tableName . '"
                SET "' . $fieldName . '" = CONCAT(' . $r . ', ' . $r . ', ' . $r . ", '@', " . $r . ', ' . $r . ", '.', " . $r . ')
                WHERE "' . $fieldName . '" IS NOT NULL AND "' . $fieldName . '" <> \'\'';
            $this->executeSql($sql);

            return true;
        }
        $this->debugFlush('Skipping anonymising ' . $tableName . '.' . $fieldName . ' as this is not a text field', 'info');

        return false;
    }

    // look for any field called Email / Phone / Address / FirstName /
}

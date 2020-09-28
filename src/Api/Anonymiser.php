<?php

namespace Sunnysideup\DatabaseShareCleanUp\Api;

use SilverStripe\Core\Config\Configurable;
use SilverStripe\Core\Extensible;
use SilverStripe\Core\Injector\Injectable;
use Sunnysideup\Flush\FlushNow;

class Anonymiser
{
    use Injectable;
    use Configurable;
    use Extensible;

    protected $databaseAction = null;

    /**
     * @var array
     */
    private static $fields_to_anonymise = [
        'Email',
        'Address',
        'Address1',
        'Address2',
        'Phone',
        'PhoneNumber',
        'Mobile',
        'FirstName',
        'Surname',
        'Fax',
        'Age',
        'Dob',
        'DataOfBirth',
        'Ip',
        'IpAddress',
    ];

    /**
     * @var array
     */
    private static $tables_to_remove = [
        'MemberPassword',
    ];

    /**
     * specify tables with fields that are not to be deleted
     * e.g.
     *    MyTable.MyField,
     *    MyTable2.MyField2,
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

    public function AnonymiseTable(string $tableName, array $fieldList, ?bool $forReal = false)
    {
        $fieldsToDelete = $this->Config()->get('fields_to_anonymise');
        $tables = $this->Config()->get('tables_to_remove');
        $keep = $this->Config()->get('keep_table_field_combos');
        $also = $this->Config()->get('also_remove_table_field_combos');
        if (in_array($tableName, $tables, true)) {
            $this->databaseActions->truncateTable($tableName);
            return;
        }
        foreach ($fieldList as $fieldName) {
            $combo = $tableName . '.' . $fieldName;
            if (in_array($fieldName, $keep, true)) {
                continue;
            }
            if (in_array($fieldName, $fieldsToDelete, true)) {
                $this->databaseActions->truncateField($tableName, $fieldName);
            }
        }
        foreach ($also as $combo) {
            list($tableName, $fieldName) = explode('.', $combo);
            $this->databaseActions->truncateField($tableName, $fieldName);
        }
    }

    // look for any field called Email / Phone / Address / FirstName /
}

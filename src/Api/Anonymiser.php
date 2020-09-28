<?php

namespace Sunnysideup\DatabaseShareCleanUp\Api;
use SilverStripe\ORM\DB;
use Sunnysideup\Flush\FlushNow;
use SilverStripe\Control\HTTPRequest;
use SilverStripe\Core\Config\Configurable;
use SilverStripe\Core\Extensible;
use SilverStripe\Core\Injector\Injectable;

class Anonymiser
{

    use Injectable;
    use Configurable;
    use Extensible;

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
     *
     * @var [type]
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
    private static $keep_table_field_combos = [

    ];

    private static $also_remove_table_field_combos = [

    ];

    protected $databaseAction = null;

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
        if(in_array($tableName, $tables, true)) {
            FlushNow::do_flush('Truncating '.$tableName, 'bad');
            $this->databaseActions->truncateTable($tableName);
            return;
        }
        foreach ($fieldList as $fieldName) {
            $combo = $tableName.'.'.$fieldName;
            if(in_array($fieldName, $keep, true)) {
                continue;
            }
            if (in_array($fieldName, $fieldsToDelete, true)) {
                FlushNow::do_flush('Truncating '.$tableName.'.'.$fieldName, 'bad');
                $this->databaseActions->truncateField($tableName, $fieldName);
            }
        }
        foreach($also as $combo) {
            list($tableName, $fieldName) = explode('.', $combo);
            FlushNow::do_flush('Truncating '.$tableName.'.'.$fieldName, 'bad');
            $this->databaseActions->truncateField($tableName, $fieldName);
        }
    }


    // look for any field called Email / Phone / Address / FirstName /
}

<?php

namespace Sunnysideup\DatabaseShareCleanUp;

class Anonymiser extends BuildTask
{

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

    private static $tables_to_remove = [

    ];

    private static $keep_table_field_combos = [

    ];

    private static $also_remove_table_field_combos = [

    ];

    public function Anonymise(string $tableName, array $fieldList)
    {
        $fields = $this->Config()->get('fields_to_anonymise');
        $tables = $this->Config()->get('tables_to_remove');
        if(in_array($tableName, $tables, true)) {
            DB::query('TRUNCATE TABLE "'.$tableName.'"; ');
        }
        foreach ($fieldList as $field) {
            
        }
    }

    // look for any field called Email / Phone / Address / FirstName /
}

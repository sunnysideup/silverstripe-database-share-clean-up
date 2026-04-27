<?php

namespace Sunnysideup\DatabaseShareCleanUp\Api;

use SilverStripe\Core\Config\Configurable;
use SilverStripe\Core\Extensible;
use SilverStripe\Core\Injector\Injectable;
use SilverStripe\ORM\DB;

class Anonymiser
{
    use Injectable;
    use Configurable;
    use Extensible;

    /**
     * Map of field name patterns to their anonymisation method suffix.
     * e.g. 'IpAddress' => 'Ip' will call anonymiseIp()
     *
     * @var array
     */
    private static $fields_to_anonymise = [
        'Email'       => 'Email',
        'ContactEmail'       => 'Email',
        'Username'    => 'Email',
        'City'        => 'City',
        'Suburb'      => 'Suburb',
        'Address'     => 'Address',
        'FullAddress' => 'Address',
        'PhysicalAddress' => 'Address',
        'Street'      => 'Street',
        'Company'     => 'Company',
        'CompanyName' => 'CompanyName',
        'Address2'    => 'Address2',
        'Phone'       => 'Phone',
        'Mobile'      => 'Mobile',
        'ContactPhone' => 'Phone',
        'MobilePhone' => 'Phone',
        'FirstName'   => 'FirstName',
        'Surname'     => 'Surname',
        'LastName'    => 'LastName',
        'Fax'         => 'Fax',
        'Dob'         => 'Dob',
        'DOB'         => 'DOB',
        'DateOfBirth' => 'DateOfBirth',
        'Ip'          => 'Ip',
        'IP'          => 'IP',
        'IpAddress'   => 'Ip',
        'Proxy'       => 'Ip',
        'UserAgent'       => 'City',
    ];

    /**
     * Tables to truncate entirely during anonymisation.
     *
     * @var array
     */
    private static $tables_to_remove = [
        'MemberPassword',
        'LoginAttempt',
    ];

    /**
     * Tables to skip entirely during anonymisation.
     *
     * @var array
     */
    private static $tables_to_keep = [];

    /**
     * Table.Field combos to skip during anonymisation.
     * e.g. ['MyTable.MyField', 'MyTable2.MyField2']
     *
     * @var array
     */
    private static $keep_table_field_combos = [];

    /**
     * Additional Table.Field combos to anonymise, with their method suffix.
     * e.g. ['MyTable.MyField' => 'Email']
     *
     * @var array
     */
    private static $also_remove_table_field_combos = [];

    /**
     * @var object
     */
    protected $databaseActions;

    public function setDatabaseActions(object $databaseActions): self
    {
        $this->databaseActions = $databaseActions;
        return $this;
    }

    /**
     * Truncates the table if it is in the removal list.
     */
    public function anonymiseTable(string $tableName): bool
    {
        if (in_array($tableName, $this->config()->get('tables_to_remove'), true)) {
            $this->databaseActions->truncateTable($tableName);
            return true;
        }

        return false;
    }

    /**
     * Anonymises a single field in a table if it matches a known pattern.
     */
    public function anonymiseTableField(string $tableName, string $fieldName): bool
    {
        if (in_array($tableName, $this->config()->get('tables_to_keep'), true)) {
            return false;
        }

        if (in_array($tableName . '.' . $fieldName, $this->config()->get('keep_table_field_combos'), true)) {
            return false;
        }

        $fieldNameLower = strtolower($fieldName);
        foreach ($this->config()->get('fields_to_anonymise') as $fieldPattern => $fieldType) {
            if ($fieldNameLower === strtolower((string) $fieldPattern)) {
                return $this->databaseActions->CallAnonymiseMethod($fieldType, $tableName, $fieldName);
            } elseif (str_contains($fieldNameLower, strtolower((string) $fieldPattern))) {
                DB::alteration_message('Also consider: ' . $fieldName . ' contains:' . $fieldPattern, 'info');
            }
        }

        return false;
    }

    /**
     * Runs anonymisation on any additionally specified Table.Field combos.
     */
    public function anonymisePresets(): void
    {
        foreach ($this->config()->get('also_remove_table_field_combos') as $combo => $fieldType) {
            [$tableName, $fieldName] = explode('.', (string) $combo);
            $this->databaseActions->CallAnonymiseMethod($fieldType, $tableName, $fieldName);
        }
    }
}

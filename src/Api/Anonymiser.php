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
     * e.g. ['MyTable.MyField' => 'Email'] (use 'Text' for free text)
     *
     * @var array
     */
    private static $also_remove_table_field_combos = [];

    /**
     * @var object
     */
    protected $databaseActions;

    /**
     * Fields that look sensitive, but are not anonymised (yet): ['Table.Field' => 'SuggestedMethod'].
     *
     * @var array
     */
    protected $suggestions = [];

    /**
     * @return array ['Table.Field' => 'SuggestedMethod']
     */
    public function getSuggestions(): array
    {
        ksort($this->suggestions);

        return $this->suggestions;
    }

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
            }
        }

        $this->collectSuggestion($tableName, $fieldName);

        return false;
    }

    /**
     * Runs anonymisation on any additionally specified Table.Field combos.
     *
     * Preferably configured as `Table.Field: Method`. A plain list of `Table.Field`
     * is also accepted: the method is then guessed from the field name,
     * falling back to `Text`. Invalid entries and missing tables are skipped.
     */
    public function anonymisePresets(): void
    {
        $combos = (array) $this->config()->get('also_remove_table_field_combos');
        foreach ($combos as $combo => $fieldType) {
            if (is_int($combo)) {
                $combo = $fieldType;
                $fieldType = '';
            }

            $parts = explode('.', (string) $combo);
            if (2 !== count($parts) || '' === $parts[0] || '' === $parts[1]) {
                DB::alteration_message('Invalid Table.Field combo: "' . $combo . '"', 'error');

                continue;
            }

            [$tableName, $fieldName] = $parts;
            if (! $this->databaseActions->tableExists($tableName)) {
                DB::alteration_message('Skipping ' . $combo . ': table does not exist', 'info');

                continue;
            }

            $this->databaseActions->CallAnonymiseMethod(
                $fieldType ?: $this->guessFieldType($fieldName),
                $tableName,
                $fieldName
            );
        }
    }

    /**
     * Remembers fields that look sensitive (e.g. "BillingEmail" or "EmailTemplate")
     * but are not anonymised, so that they can be reported at the end.
     * The match is case sensitive on purpose: "ShippingCity" yes, "Velocity" no.
     */
    protected function collectSuggestion(string $tableName, string $fieldName): void
    {
        $combo = $tableName . '.' . $fieldName;
        if (isset($this->config()->get('also_remove_table_field_combos')[$combo])) {
            return;
        }

        foreach ($this->config()->get('fields_to_anonymise') as $fieldPattern => $fieldType) {
            $fieldPattern = (string) $fieldPattern;
            $length = strlen($fieldPattern);
            $endsWith = $length >= 3 && str_ends_with($fieldName, $fieldPattern);
            $startsWith = $length >= 4
                && str_starts_with($fieldName, $fieldPattern)
                && ctype_upper(substr($fieldName, $length, 1) ?: 'A');
            if ($endsWith || $startsWith) {
                $this->suggestions[$combo] = $fieldType;

                return;
            }
        }
    }

    protected function guessFieldType(string $fieldName): string
    {
        foreach ($this->config()->get('fields_to_anonymise') as $fieldPattern => $fieldType) {
            if (strtolower($fieldName) === strtolower((string) $fieldPattern)) {
                return $fieldType;
            }
        }

        return 'Text';
    }
}

<?php

namespace Sunnysideup\DatabaseShareCleanUp\Api;
use SilverStripe\Core\ClassInfo;
use SilverStripe\ORM\DataObject;
use SilverStripe\ORM\DB;
use Sunnysideup\Flush\FlushNow;

use SilverStripe\Core\Config\Configurable;
use SilverStripe\Core\Extensible;
use SilverStripe\Core\Injector\Injector;
use SilverStripe\Core\Injector\Injectable;

class DatabaseInfo
{

    use Injectable;
    use Configurable;
    use Extensible;

    /**
     * @var string[]
     */
    protected const TEXT_FIELDS = [
        'varchar',
        'mediumtext',
    ];

    protected static $tableList = [];

    protected static $fieldsForTable = [];

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
        return 0 === $this->countRows($tableName);
    }

    public function countRows(string $tableName): int
    {
        return (int) DB::query('SELECT COUNT(*) FROM "' . $tableName . '";')->value();
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

    protected function isTextField(string $tableName, string $fieldName)
    {
        $details = $this->getAllFieldsForOneTableDetails($tableName);
        if (isset($details[$fieldName])) {
            foreach (self::TEXT_FIELDS as $test) {
                if (0 === stripos(strtolower($details[$fieldName]), $test)) {
                    return true;
                }
            }
        } else {
            FlushNow::do_flush('ERROR: could not find: ' . $tableName . '.' . $fieldName, 'bad');
        }
    }

    public function getClassNameFromTableName(string $tableName) : string
    {
        if(class_exists($tableName)) {
            if( Injector::inst()->get($tableName) instanceof DataObject) {
                return $tableName;
            }
        }
        $subClasses = ClassInfo::subclassesFor(DataObject::class, false);
        foreach($subClasses as $className) {
            $test = $this->getTableForClassName($className);
            if($tableName === $test) {
                return $className;
            }
        }
        return '';
    }

    public function getTableForClassName(string $className) : string
    {
        return DataObject::getSchema()->tableName($className);
    }

    public function getSubTables(string $className) : array
    {
        $classTables = [];
        $subClasses = ClassInfo::subclassesFor($className, false);
        foreach ($subClasses as $class) {
            if (DataObject::getSchema()->classHasTable($class)) {
                $classTables[] = DataObject::getSchema()->tableName($class);
            }
        }
        $classTables = array_unique($classTables);

        return $classTables;
    }


    protected function turnPercentageIntoLimit(string $tableName, float $percentageToKeep): int
    {
        $count = DB::query('SELECT COUNT(*) FROM "' . $tableName . '"')->value();

        return (int) round($percentageToKeep * $count);
    }

    protected function hasField(string $tableName, string $fieldName): bool
    {
        return (bool) DB::get_schema()->hasField($tableName, $fieldName);
    }

    protected function hasTable(string $tableName): bool
    {
        return (bool) DB::get_schema()->hasTable($tableName);
    }



}

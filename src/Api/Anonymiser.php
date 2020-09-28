<?php

namespace Sunnysideup\DatabaseShareCleanUp;

class Anonymiser extends BuildTask
{

    private static $fields_to_anonimyse = [
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

    private static $tables_to_remove [

    ];

    // look for any field called Email / Phone / Address / FirstName /
}

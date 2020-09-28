# how it works:

1. browse to /dev/tasks/database-share-clean-up (using command line or browser)

2. this will run through entire database AND ....

   - delete any rows (records) marked in `CleanUp::$tables_to_be_cleaned`
   - remove content from any fields in `CleanUp::$fields_to_be_cleaned`

   where the record is more than `CleanUp::$days_back`

   These three values can be set with yml.


3. it will look at any table / field that may be sensitive and remove data (e.g. any Email field).



   

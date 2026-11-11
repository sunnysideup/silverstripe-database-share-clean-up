# how it works

1. browse to `/dev/tasks/database-share-clean-up` (using command line or browser)

2. choose what to do (anonymise, remove versions, remove obsolete tables, empty large fields, remove old rows)
   and hit the button. By default nothing is changed (dry run); tick "do it for real?" to apply.

   - delete any rows (records) in tables listed in `CleanUp::$tables_to_be_cleaned`
   - remove content from any fields in `CleanUp::$fields_to_be_cleaned`

   These values can be set with yml.

3. anonymise will look at any table / field that may be sensitive (e.g. any Email field) and replace the data.
   See `Anonymiser` for the config options, in particular:

   - `fields_to_anonymise`: field name => anonymise method
   - `also_remove_table_field_combos`: `Table.Field: Method` (use `Text` for free text)
   - `keep_table_field_combos` and `tables_to_keep`: what not to anonymise
   - `tables_to_remove`: tables to truncate

   Tick "keep admins" to leave administrators (members with the `ADMIN` permission
   or in the `administrators` group) untouched. See `DatabaseActions::$admin_permission_codes`
   and `$admin_group_codes`.

# "for real" mode

- never on a live site
- on the command line: always (on non-live sites)
- in the browser: only in dev mode, and only by submitting the form (it includes a security token),
  not by following a link.

# Upgrade Guide: SilverStripe 6

## Dependencies

⚠️ Update `composer.json` to require `silverstripe/recipe-core: ^6.0`

🔍 The `sunnysideup/flush` dependency has been temporarily removed pending a compatible stable release. Check if your project relies on this package.

## Task Architecture

⚠️ **Breaking:** `BuildTask::run()` has been replaced with Symfony Console command structure.

### Method Signature Changes

Replace the `run()` method:
```php
// Before
public function run($request)

// After  
protected function execute(InputInterface $input, PolyOutput $output): int
```

Return `Command::SUCCESS` at the end of `execute()`.

### Request Parameter Access

Replace `HTTPRequest` calls with `InputInterface`:
```php
// Before
$request->getVar('anonymise')

// After
$input->getOption('anonymise')
```

### Command Configuration

- Replace `private static $segment` with `protected static string $commandName` for command naming
- Implement `getOptions()` method returning an array of `InputOption` objects for CLI flags

### Output Methods

Replace direct output calls:
```php
// Before
echo 'message';
FlushNowImplementor::do_flush('<h3>message</h3>', 'bad');

// After
$output->writeln('message');
$output->writeForHtml('<h3>message</h3>', 'bad');
```

## Type Declarations

Add explicit type hints to class properties:
```php
protected string $title = '...';
protected static string $description = '...';
```

## Imports

Add new required imports:
```php
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use SilverStripe\PolyExecution\PolyOutput;
```

Remove obsolete imports:
```php
use SilverStripe\Control\HTTPRequest; // No longer needed
```

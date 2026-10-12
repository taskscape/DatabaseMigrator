# DatabaseMigrator

DatabaseMigrator is a .NET Framework console utility that automatically upgrades a SQL Server database by executing an ordered list of SQL scripts. It tracks the current database version in a plain text file and applies only the scripts that have not been executed yet.

## How It Works

1. The application reads the current version from `dbversion.txt` located in the working directory (version `0` is assumed when the file is missing).
2. It loads the ordered script list from `DatabaseMigrator.script.txt` inside the scripts folder. Each line of that file is a script file name, and the number of lines defines the expected version.
3. It compares the current version with the expected version:
   - If the expected version is higher, every script above the current version is executed in order through the configured SQL tool (by default `sqlcmd`).
   - If both versions match, no scripts are executed.
4. After all scripts succeed, the new version number is written back to `dbversion.txt`.
5. If a script fails, execution stops, and the tool optionally lets you mark only the succeeded scripts as executed (interactive confirmation dialog).

## Requirements

- .NET Framework 4.5
- A configured SQL command line tool (by default `sqlcmd`, available with SQL Server or Microsoft Command Line Utilities)
- Windows authentication (`-E`) or adjust the `Script` template in the configuration file

## Usage

Command line mode (all three arguments are required together):

```
DatabaseMigrator.exe <sqlInstanceName> <databaseName> <scriptsPath>
```

Example:

```
DatabaseMigrator.exe "(local)\sqlexpress" "database" "C:\Scripts"
```

Configuration-only mode (run without arguments). The instance is read from `DatabaseInstance` and the scripts folder from `ScriptsFolderPath` in `DatabaseMigrator.exe.config`; the database name is read from `DefaultDatabaseName`.

## Configuration

Settings are read from the `appSettings` section of `DatabaseMigrator.exe.config`:

| Key | Description |
|-----|-------------|
| `SqlTool` | Executable used to run the scripts (default: `sqlcmd`). |
| `Script` | Command template with placeholders: `{0}` — quoted script path, `{1}` — server name, `{2}` — database name. Default: `-S {1} -d {2} -m1 -E -i {0}`. |
| `DefaultDatabaseName` | Database name used when it is not passed on the command line. |
| `DatabaseInstance` | SQL Server instance used in configuration-only mode (default: `(local)\sqlexpress`). |
| `ScriptsFolderPath` | Scripts folder used in configuration-only mode. |
| `TraceLogFilePath` | Optional trace log path. When empty, `DatabaseMigrator.log` is created in the working directory. |

## Files

| File | Purpose |
|------|---------|
| `DatabaseMigrator.script.txt` | Ordered list of script file names to execute (one per line), stored in the scripts folder. |
| `dbversion.txt` | Stores the number of scripts executed so far; created next to the executable after a successful run. |
| `DatabaseMigrator.log` | Trace log with every executed command and its output. |

## Exit Codes

- `0` — migration succeeded (or the database was already up to date).
- `2` — one of the required command line arguments was empty.
- `-1` — configuration or script list error (missing scripts folder, missing `DatabaseMigrator.script.txt`, unreadable `dbversion.txt`).
- `-2` — a script execution failed.

## License

This project is licensed under the GNU General Public License v2.0 - see the [LICENSE](LICENSE) file for details.

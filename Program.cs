using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Sql;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Windows.Forms;

namespace DatabaseMigrator
{
    /// <summary>
    /// This is a tool providing automatic database migrations based on a text scripts
    /// script.txt - file with an ordered list of scripts to execute against the database server during migrations
    /// dbversion.txt - file with a single providing the line matching the last executed script from scripts.txt 
    /// 
    /// usage: DatabaseMigrator.exe (sqlInstanceName) (databaseName) (scriptsPath)
    /// </summary>
    class Program
    {
        /// <summary>
        /// Scripts to execute
        /// </summary>
        private List<string> _filesToExecute;

        /// <summary>
        /// Path to folder with scripts
        /// </summary>
        private string _scriptFolder;
        
        /// <summary>
        /// Server name
        /// </summary>
        private string _serverName;

        /// <summary>
        /// Database name
        /// </summary>
        private string _databaseName;

        static int Main(string[] args)
        {
            var traceLogKeyDefined = true;
            var traceLogPath = ConfigurationManager.AppSettings["TraceLogFilePath"];

            if (string.IsNullOrWhiteSpace(traceLogPath))
            {
                traceLogKeyDefined = false;
                traceLogPath = string.Format("{0}\\{1}", Environment.CurrentDirectory.TrimEnd('\\'), "DatabaseMigrator.log");
            }
            
            Trace.Listeners.Add(new TextWriterTraceListener(traceLogPath));
            Trace.TraceInformation("Started at {0}", DateTime.Now);

            if (!traceLogKeyDefined)
                Trace.TraceWarning("TraceLogFilePath was not configured, using default " + traceLogPath);

            var programResult = new Program().Execute(args);

            Trace.Flush();

            if (programResult == 0) 
                return programResult;

            Console.WriteLine("Execution failed. Please see the tracelog for more details (" + traceLogPath + "). Press any key to continue...");
            Console.ReadKey();

            return programResult;
        }

        private int Execute(IReadOnlyList<string> args)
        {
            if (args.Count >= 2) // InstanceName and SQLScriptsPath
            {
                _serverName = args[0];
                _databaseName = args[1];
                _scriptFolder = args[2];
                
                if (string.IsNullOrWhiteSpace(_serverName))
                {
                    Trace.TraceError("Provided ServerName argument is empty");
                    return 2;
                }

                if (string.IsNullOrWhiteSpace(_databaseName))
                {
                    Trace.TraceError("Provided DatabaseName argument is empty");
                    return 2;
                }

                if (string.IsNullOrWhiteSpace(_scriptFolder))
                {
                    Trace.TraceError("Provided ScriptsFolder argument is empty");
                    return 2;
                }
            }
            else
            {
                var dbInstance = ConfigurationManager.AppSettings["DatabaseInstance"];

                if (string.IsNullOrWhiteSpace(dbInstance))
                {
                    Trace.TraceWarning("DatabaseInstance configuration key is empty, using (local)");
                    _serverName = "(local)";
                }
                else
                    _serverName = dbInstance;
            }

            Trace.TraceInformation("Using '{0}' as a sql instance name", _serverName);
            
            var dbVersion = GetCurrentDbVersion();
            
            if (string.IsNullOrEmpty(dbVersion))
            {
                Trace.TraceWarning("Could not read current version! Missing dbversion.txt file in tool's dir. DatabaseMigrator will start from zero version");
                dbVersion = "0";
            }

            var currentVersion = 0;

            if (!int.TryParse(dbVersion, out currentVersion))
            {
                Trace.TraceError("Cannot read current scripts version (parsing failed)");
                return -1;
            }

            var expectedVersion = LoadScripts();

            if (expectedVersion == -1)
                return expectedVersion;

            Trace.TraceInformation("Upgrading from version {0} to {1}.", currentVersion, expectedVersion);

            if (expectedVersion > currentVersion)
            {
                if (UpdateDbToNewestVersion(currentVersion) == false)
                {
                    Trace.TraceInformation("UpdateDBToNewestVersion failed.");
                    return -2;
                }
            }
            else
            {
                Trace.TraceInformation("Current version: {0} matches expected version: {1}. Process finished.", currentVersion, expectedVersion);
            }

            Trace.TraceInformation("Finished database migration at: {0}.", DateTime.Now);
            return 0;
        }

        private int LoadScripts()
        {
            if (string.IsNullOrWhiteSpace(_scriptFolder))
            {
                _scriptFolder = ConfigurationManager.AppSettings["ScriptsFolderPath"];

                if (string.IsNullOrWhiteSpace(_scriptFolder))
                {
                    Trace.TraceError("Cannot read scripts path from ScriptsPath configuration key");
                    return -1;
                }

                _scriptFolder = _scriptFolder.TrimEnd('\\');
            }

            if (!Directory.Exists(_scriptFolder))
            {
                Trace.TraceError("Path to SQL scripts doesn't exist! " + _scriptFolder);
                return -1;
            }

            var databaseMigratorScriptPath = string.Format("{0}\\{1}", _scriptFolder, "DatabaseMigrator.script.txt");

            if (!File.Exists(databaseMigratorScriptPath))
            {
                Trace.TraceError("DatabaseMigrator.script.txt doesn't exist in path: " + databaseMigratorScriptPath);
                return -1;
            }

            var currentVersion = 0;
            
            using (var stream = new StreamReader(databaseMigratorScriptPath))
            {
                var scriptsList = stream.ReadToEnd();

                _filesToExecute = scriptsList.Replace("\r", string.Empty).Split(new[] { "\n" }, StringSplitOptions.RemoveEmptyEntries).ToList();
                currentVersion = _filesToExecute.Count;
            }

            Trace.TraceInformation("{0} scripts found.", currentVersion);
            return currentVersion;
        }

        private bool UpdateDbToNewestVersion(int dbVersion)
        {
            var tool = ConfigurationManager.AppSettings["SqlTool"];
            if (string.IsNullOrEmpty(tool))
            {
                Trace.TraceInformation("SqlTool not set in .config file. Exiting...");
                return false;
            }

            if (string.IsNullOrEmpty(_databaseName))
            {
                _databaseName = ConfigurationManager.AppSettings["DefaultDatabaseName"];

                if (string.IsNullOrEmpty(_databaseName))
                {
                    Trace.TraceInformation("Cannot read DefaultDatabaseName configuration key. Exiting...");
                    return false;
                }
            }

            var scriptsToExecute = _filesToExecute.Skip(dbVersion).ToList();
            var scriptToExecute = ConfigurationManager.AppSettings["Script"];

            var executeSuccessed = true;
            var currentVersion = dbVersion;

            try
            {
                foreach (var script in scriptsToExecute.Select(s => string.Format(scriptToExecute, string.Format("\"{0}\\{1}\"", _scriptFolder, s), _serverName, _databaseName)))
                {
                    if (RunCommand(tool, script))
                        currentVersion++;
                    else
                    {
                        executeSuccessed = false;
                        break;
                    }
                }
            }
            catch (Exception exception)
            {
                Trace.TraceError("UpdateDBToNewestVersion failed: {0}", exception);
                executeSuccessed = false;
            }

            if (executeSuccessed)
            {
                dbVersion += scriptsToExecute.Count;
                File.WriteAllText("dbversion.txt", dbVersion.ToString());
                Trace.TraceInformation("New version number ({0}) stored to file.", dbVersion);
                
                return true;
            }

            if (MessageBox.Show(
                "Database migration failed." + Environment.NewLine +
                "Do you want to store expected database migration pointer?" + Environment.NewLine +
                "Choosing 'Yes' will mark only succeeded scripts as executed." + Environment.NewLine +
                "Choosing 'No' will not update database migration pointer.",
                "Database migration",
                MessageBoxButtons.YesNo) == DialogResult.Yes)
            {
                File.WriteAllText("dbversion.txt", currentVersion.ToString());
            }

            return false;
        }

        private bool RunCommand(string tool, string script)
        {
            var processInfo = new ProcessStartInfo
            {
                WorkingDirectory = Environment.CurrentDirectory,
                FileName = tool,
                Arguments = script,
                RedirectStandardError = true,
                RedirectStandardOutput = true,
                UseShellExecute = false
            };

            Trace.TraceInformation(
                "Executing command: '{0}' with arguments: '{1}' against server: '{2}'.",
                processInfo.FileName,
                processInfo.Arguments,
                _serverName);

            var process = Process.Start(processInfo);
            if (process != null)
            {
                process.WaitForExit();

                Trace.TraceInformation(process.StandardOutput.ReadToEnd());
                Trace.TraceInformation(process.StandardError.ReadToEnd());

                if (process.ExitCode != 0)
                {
                    Trace.TraceError("Generic error encoundered.");
                    Trace.WriteLine("Generic error encountered");
                    return false;
                }

                Trace.TraceInformation(
                    "Executing command: '{0}' with arguments: '{1}' succedded.",
                    processInfo.FileName,
                    processInfo.Arguments);

                return true;
            }

            Trace.TraceError("Could not start the process using the tool: '{0}'", tool);
            return false;
        }

        private string GetCurrentDbVersion()
        {
            var version = string.Empty;
            
            if (File.Exists("dbversion.txt"))
                version = File.ReadAllText("dbversion.txt");
            
            Trace.TraceInformation("Current database version is: '{0}'.", string.IsNullOrWhiteSpace(version) ? "0 (missing dbversion.txt file" : version);
            return version;
        }
    }
}

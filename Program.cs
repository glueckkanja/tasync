using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.WindowsAzure;
using Mono.Options;

namespace Tasync
{
    internal static class Program
    {
        private static int Main(string[] args)
        {
            var cmd = new Dictionary<string, object>();

            var p = new OptionSet
                        {
                            {
                                "c=|cred=", "credentials (account|key)",
                                s => cmd["credentials"] = ParseCred(s)
                                },
                            {"h|help", "show this message and exit", s => cmd["help"] = s}
                        };

            SyncMode mode;
            string path;

            try
            {
                IList<string> extra = p.Parse(args);

                mode = (extra[0].IndexOf("im", StringComparison.InvariantCultureIgnoreCase) >= 0)
                           ? SyncMode.Import
                           : SyncMode.Export;

                path = Path.GetFullPath(extra[1]);

                Directory.CreateDirectory(path);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Invalid arguments");
                Console.Error.WriteLine(e);
                Console.Error.WriteLine("Try 'tasync --help'.");
                return 1;
            }

            if (cmd.ContainsKey("help"))
            {
                ShowHelp(p);
                return 0;
            }

            if (cmd.ContainsKey("credentials"))
            {
                return MainExec(mode, path, cmd);
            }

            ShowHelp(p);
            return 0;
        }

        private static int MainExec(SyncMode mode, string path, Dictionary<string, object> cmd)
        {
            var credentials = (StorageCredentialsAccountAndKey) cmd["credentials"];

            Console.WriteLine();
            Console.WriteLine("## ACTION     {0} {1}", mode, credentials.AccountName);
            Console.WriteLine();
            Console.WriteLine("   Scanning account ...");
            Console.WriteLine();

            StorageInfoBase info = (mode == SyncMode.Import)
                                       ? (StorageInfoBase) new StorageImportInfo(credentials, path)
                                       : new StorageExportInfo(credentials, path);

            try
            {
                info.LoadMetaData();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error loading table meta data");
                Console.Error.WriteLine(e);
                return 2;
            }

            foreach (var kv in info.DestinationTableLevelActions.OrderBy(x => x.Key))
            {
                Console.WriteLine("## {0,-10} {1}", ActionToString(kv.Value), kv.Key);
            }

            Console.WriteLine();

            StorageSyncBase sync = null;

            if (mode == SyncMode.Export)
            {
                sync = new StorageExport(credentials, path, info);
            }
            else if (mode == SyncMode.Import)
            {
                sync = new StorageImport(credentials, path, info);
            }

            if (sync != null)
            {
                sync.RunTableLevelActions();
                sync.RunDataLevelActions();
            }

            return 0;
        }

        private static string ActionToString(TableAction action)
        {
            switch (action)
            {
                case TableAction.CreateTable:
                    return "CREATE";
                case TableAction.DeleteTable:
                    return "DELETE";
                case TableAction.SyncData:
                    return "SYNC";
            }

            return "";
        }

        private static StorageCredentialsAccountAndKey ParseCred(string str)
        {
            string[] arr = str.Split('|').Take(2).ToArray();
            return new StorageCredentialsAccountAndKey(arr[0], arr[1]);
        }

        private static void ShowHelp(OptionSet p)
        {
            Console.WriteLine("tasync");
            Console.WriteLine("Export, sync and import whole Azure Table Storage accounts.");
            Console.WriteLine();

            p.WriteOptionDescriptions(Console.Out);
        }

        #region Nested type: SyncMode

        private enum SyncMode
        {
            Export,
            Import
        }

        #endregion
    }
}
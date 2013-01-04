using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Mono.Options;

namespace Tasync
{
    internal static class Program
    {
        private static readonly CancellationTokenSource Ctc = new CancellationTokenSource();

        private static int Main(string[] args)
        {
            Console.CancelKeyPress += (_, e) =>
                                          {
                                              e.Cancel = true;
                                              Ctc.Cancel();
                                          };

            var cmd = new Dictionary<string, object>();

            var p = new OptionSet
                        {
                            {
                                "src=|source=", "source account credentials (account,key) or 'dev'",
                                s => cmd["source"] = ParseAccount(s)
                            },
                            {
                                "dst=|destination=", "destination account credentials (account,key) or 'dev'",
                                s => cmd["destination"] = ParseAccount(s)
                            },
                            {
                                "d|delete", "delete in destination (defaults to off)",
                                s => cmd["delete"] = s
                            },
                            {
                                "h|help", "show this message and exit",
                                s => cmd["help"] = s
                            }
                        };

            p.Parse(args);

            if (cmd.ContainsKey("help") && cmd["help"] != null)
            {
                ShowHelp(p);
                return 0;
            }

            if (cmd.ContainsKey("source") && cmd.ContainsKey("destination"))
            {
                bool del = cmd.ContainsKey("delete") && cmd["delete"] != null;

                var src = (CloudStorageAccount) cmd["source"];
                var dst = (CloudStorageAccount) cmd["destination"];

                return MainExec(src, dst, del).Result;
            }

            ShowHelp(p);
            return 0;
        }

        private static async Task<int> MainExec(CloudStorageAccount source, CloudStorageAccount destination, bool delete)
        {
            Exception ex = null;

            var syncer = new Syncer(source, destination, Ctc.Token);

            try
            {
                await syncer.SyncOneWay();
                
                if (delete)
                {
                    await syncer.DeleteInDestination();
                }
            }
            catch (Exception e)
            {
                ex = e;
            }

            if (Ctc.IsCancellationRequested)
            {
                Console.WriteLine("User cancelled");
                return 1;
            }

            if (ex != null)
            {
                Console.Error.WriteLine("General error");
                Console.Error.WriteLine(ex);
                return 100;
            }

            return 0;
        }

        private static CloudStorageAccount ParseAccount(string str)
        {
            if ("dev".Equals(str, StringComparison.OrdinalIgnoreCase))
                return CloudStorageAccount.DevelopmentStorageAccount;

            string[] arr = str.Split(',').Take(2).ToArray();
            return new CloudStorageAccount(new StorageCredentials(arr[0], arr[1]), true);
        }

        private static void ShowHelp(OptionSet p)
        {
            Console.WriteLine("tasync");
            Console.WriteLine("Incrementally sync complete Azure Table Storage accounts.");
            Console.WriteLine();

            p.WriteOptionDescriptions(Console.Out);
        }
    }
}
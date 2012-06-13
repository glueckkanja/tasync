using System;
using System.Collections.Generic;
using System.Linq;
using Mono.Options;

namespace Tasync
{
    internal static class Program
    {
        private static int Main(string[] args)
        {
            var cmd = new Dictionary<string, string>();

            var p = new OptionSet
                        {
                            {"h|help", "show this message and exit", s => cmd["help"] = s}
                        };

            try
            {
                p.Parse(args);
            }
            catch (OptionException e)
            {
                Console.WriteLine("Invalid arguments");
                Console.WriteLine(e.Message);
                Console.WriteLine("Try 'tysync --help'.");
                return 1;
            }

            if (cmd.ContainsKey("help"))
            {
                ShowHelp(p);
                return 0;
            }

            ShowHelp(p);
            return 0;
        }

        private static void ShowHelp(OptionSet p)
        {
            Console.WriteLine("tasync");
            Console.WriteLine("Export, sync and import whole Azure Table Storage accounts.");
            Console.WriteLine();

            p.WriteOptionDescriptions(Console.Out);
        }
    }
}
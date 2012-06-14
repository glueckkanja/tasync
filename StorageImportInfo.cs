using System.Collections.Generic;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;

namespace Tasync
{
    internal class StorageImportInfo : StorageInfoBase
    {
        public StorageImportInfo(StorageCredentialsAccountAndKey credentials, string path) : base(credentials, path)
        {
        }

        protected override void LoadTables()
        {
            Task<string[]> destinationTables = Task.Factory.StartNew(() => Client.ListTables().ToArray());

            SourceTables = Directory
                .EnumerateFiles(Path, FilePrefix + @"*.json")
                .Select(System.IO.Path.GetFileNameWithoutExtension)
                .Select(x => x.Substring(FilePrefix.Length))
                .ToArray();

            DestinationTables = destinationTables.Result;
        }
    }
}
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;

namespace Tasync
{
    internal class StorageExportInfo : StorageInfoBase
    {
        public StorageExportInfo(StorageCredentials credentials, string path) : base(credentials, path)
        {
        }

        protected override void LoadTables()
        {
            Task<CloudTable[]> sourceTables = Task.Factory.StartNew(() => Client.ListTables().ToArray());

            DestinationTables = Directory
                .EnumerateFiles(Path, FilePrefix + @"*.json")
                .Select(System.IO.Path.GetFileNameWithoutExtension)
                .Select(x => x.Substring(FilePrefix.Length))
                .ToArray();

            SourceTables = sourceTables.Result.Select(x => x.Name).ToArray();
        }
    }
}
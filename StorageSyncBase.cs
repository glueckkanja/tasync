using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.StorageClient;

namespace Tasync
{
    internal abstract class StorageSyncBase
    {
        private const string FilePrefix = @"Table_";

        public StorageSyncBase(StorageCredentialsAccountAndKey credentials, string root, StorageInfoBase info)
        {
            Root = root;
            Info = info;

            Client = new CloudStorageAccount(credentials, true).CreateCloudTableClient();
        }

        public CloudTableClient Client { get; set; }
        public StorageInfoBase Info { get; set; }
        public string Root { get; set; }

        public void RunTableLevelActions()
        {
            var actions = Info.DestinationTableLevelActions
                .Where(x => x.Value == TableAction.CreateTable || x.Value == TableAction.DeleteTable)
                .OrderBy(x => x.Key)
                .ThenBy(x => x.Value);

            foreach (var action in actions)
            {
                if (action.Value == TableAction.CreateTable)
                {
                    CreateTable(action.Key);
                }
                else if (action.Value == TableAction.DeleteTable)
                {
                    DeleteTable(action.Key);
                }
            }
        }

        public void RunDataLevelActions()
        {
            var actions = Info.DestinationTableLevelActions
                .Where(x => x.Value == TableAction.CreateTable || x.Value == TableAction.SyncData)
                .OrderBy(x => x.Key)
                .ThenBy(x => x.Value);

            foreach (var action in actions)
            {
                SyncData(action.Key);
            }
        }

        protected abstract void CreateTable(string table);
        protected abstract void DeleteTable(string table);

        protected abstract void SyncData(string table);

        protected string BuildFilePath(string table)
        {
            return Path.Combine(Root, FilePrefix + table + @".json");
        }
    }
}
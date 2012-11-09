using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;

namespace Tasync
{
    internal abstract class StorageInfoBase
    {
        protected const string FilePrefix = @"Table_";

        public StorageInfoBase(StorageCredentials credentials, string path)
        {
            Path = path;
            Client = new CloudStorageAccount(credentials, true).CreateCloudTableClient();

            DestinationTableLevelActions = new Dictionary<string, TableAction>();
        }

        protected CloudTableClient Client { get; private set; }
        protected string Path { get; private set; }

        public ICollection<string> SourceTables { get; set; }
        public ICollection<string> DestinationTables { get; set; }
        public IDictionary<string, TableAction> DestinationTableLevelActions { get; set; }

        public void LoadMetaData()
        {
            LoadTables();
            ComputeTableActions();
        }

        protected abstract void LoadTables();

        private void ComputeTableActions()
        {
            foreach (string t in SourceTables)
            {
                DestinationTableLevelActions[t] = TableAction.SyncData;
            }

            foreach (string t in SourceTables.Except(DestinationTables))
            {
                DestinationTableLevelActions[t] = TableAction.CreateTable;
            }

            foreach (string t in DestinationTables.Except(SourceTables))
            {
                DestinationTableLevelActions[t] = TableAction.DeleteTable;
            }
        }
    }
}
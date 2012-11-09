using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.DataServices;
using Newtonsoft.Json;

namespace Tasync
{
    internal class StorageExport : StorageSyncBase
    {
        public StorageExport(StorageCredentials credentials, string root, StorageInfoBase info)
            : base(credentials, root, info)
        {
        }

        protected override void CreateTable(string table)
        {
            Console.WriteLine("## TABLE CREATE    {0}", table);
            File.CreateText(BuildFilePath(table));
        }

        protected override void DeleteTable(string table)
        {
            Console.WriteLine("## TABLE DELETE    {0}", table);
            File.Delete(BuildFilePath(table));
        }

        protected override void SyncData(string table)
        {
            Console.WriteLine("## DATA SYNC       {0}", table);

            TableServiceContext ctx = Client.GetTableServiceContext();

            ctx.IgnoreMissingProperties = true;
            ctx.ReadingEntity += GenericEntity.OnReadingEntity;

            GenericEntity[] data = ctx.CreateQuery<GenericEntity>(table)
                .AsTableServiceQuery(ctx)
                .ToArray()
                .OrderBy(x => x.PartitionKey)
                .ThenBy(x => x.RowKey)
                .ToArray();

            using (FileStream file = File.OpenWrite(BuildFilePath(table)))
            {
                // truncate
                file.SetLength(0);

                using (var writer = new StreamWriter(file))
                {
                    writer.WriteLine(@"{");
                    writer.Write(@"""Meta"": ");
                    writer.Write(JsonLine(new {version = "1.0"}));
                    writer.WriteLine(@",");
                    writer.WriteLine(@"""Data"": [");

                    long pos = -1;

                    foreach (GenericEntity entity in data)
                    {
                        writer.Write(JsonLine(entity));
                        writer.Flush();
                        pos = file.Position;
                        writer.WriteLine(@",");
                    }

                    writer.Flush();

                    if (pos > 0)
                    {
                        file.Position = pos;
                    }

                    writer.WriteLine();
                    writer.WriteLine(@"]");
                    writer.WriteLine(@"}");
                }
            }
        }

        private string JsonLine(object obj)
        {
            return JsonConvert.SerializeObject(obj);
        }
    }
}
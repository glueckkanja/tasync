using System.Collections.Generic;
using System;
using System.Data.Services.Client;
using System.IO;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table.DataServices;
using Newtonsoft.Json;

namespace Tasync
{
    internal class StorageImport : StorageSyncBase
    {
        public StorageImport(StorageCredentials credentials, string root, StorageInfoBase info)
            : base(credentials, root, info)
        {
        }

        protected override void CreateTable(string table)
        {
            Console.WriteLine("## TABLE CREATE    {0}", table);
            Client.GetTableReference(table).CreateIfNotExists();
        }

        protected override void DeleteTable(string table)
        {
            Console.WriteLine("## TABLE DELETE    {0}", table);
            Client.GetTableReference(table).DeleteIfExists();
        }

        protected override void SyncData(string table)
        {
            Console.WriteLine("## DATA SYNC       {0}", table);

            var proto = new {Meta = new {version = ""}, Data = new List<GenericEntity>()};

            var data = JsonConvert.DeserializeAnonymousType(File.ReadAllText(BuildFilePath(table)), proto);
            var batches = Batch(data.Data).ToArray();
            int count = batches.Length;

            Console.WriteLine("   {0} batches", count);

            int i = 0, l = 0;

            // insert or replace (AttachTo, UpdateObject, ReplaceOnUpdate)
            foreach (var batch in batches)
            {
                TableServiceContext ctx = Client.GetTableServiceContext();
                ctx.ReadingEntity += GenericEntity.OnReadingEntity;
                ctx.WritingEntity += GenericEntity.OnWritingEntity;

                foreach (GenericEntity entity in batch)
                {
                    ctx.AttachTo(table, entity);
                    ctx.UpdateObject(entity);
                }

                ctx.SaveChangesWithRetries(SaveChangesOptions.ReplaceOnUpdate);

                i++;

                double p = (1.0*i/count);

                if ((int) (p*10) > l || i == count - 1)
                {
                    l = (int) (p*10);
                    Console.Write("   {0:p0}", p);
                }
            }

            Console.WriteLine();
        }

        private static IEnumerable<IEnumerable<T>> Batch<T>(IEnumerable<T> items, int chunkSize = 25)
        {
            int i = 0;

            return from name in items
                   group name by i++/chunkSize
                   into part
                   select part.AsEnumerable();
        }
    }
}
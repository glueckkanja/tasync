using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Tasync
{
    internal class Syncer
    {
        private const string MetatableName = "aaametatasync";

        private static readonly DateTimeOffset LowestTimestamp = new DateTimeOffset(1601, 1, 1, 0, 0, 0, TimeSpan.Zero);

        private readonly CloudTableClient _dstClient;
        private readonly CloudTableClient _srcClient;
        private readonly CancellationToken _token;

        private IDictionary<CloudTable, DateTimeOffset> _timestamps;

        public Syncer(CloudStorageAccount source, CloudStorageAccount destination, CancellationToken token)
        {
            _token = token;
            _srcClient = source.CreateCloudTableClient();
            _dstClient = destination.CreateCloudTableClient();
        }

        public async Task Execute()
        {
            await Prechecks();
            await GatherInfo();
            await DoSync();
            await SaveInfo();
        }

        private async Task Prechecks()
        {
            await _dstClient.GetTableReference(MetatableName).CreateIfNotExistsAsync(_token);
        }

        private async Task GatherInfo()
        {
            IList<CloudTable> tables = await _srcClient.ListTablesAsync(_token);
            _timestamps = await LoadTimestampsFromMetatable(tables);

            if (_token.IsCancellationRequested)
                return;

            Console.WriteLine("destination status");

            foreach (var kv in _timestamps.OrderBy(x => x.Key.Name))
            {
                Console.WriteLine("{0,-40}{1}", kv.Key.Name, kv.Value);
            }
        }

        private async Task DoSync()
        {
            foreach (var kv in _timestamps.OrderBy(x => x.Key.Name))
            {
                if (_token.IsCancellationRequested)
                    return;

                Console.WriteLine("syncing table {0}", kv.Key.Name);

                await SyncTableSince(kv.Key, kv.Value);
            }
        }

        private async Task SaveInfo()
        {
            CloudTable metatable = _dstClient.GetTableReference(MetatableName);

            Console.WriteLine("new destination status");

            var op = new TableBatchOperation();

            foreach (var batch in _timestamps.OrderBy(x => x.Key.Name).Batch(100))
            {
                foreach (var item in batch)
                {
                    var entity = new DynamicTableEntity("SyncTimestamps", item.Key.Name);
                    entity.Properties["SourceTimestamp"] = new EntityProperty(item.Value);
                    op.InsertOrReplace(entity);

                    Console.WriteLine("{0,-40}{1}", item.Key.Name, item.Value);
                }

                await metatable.ExecuteBatchAsync(op, _token);
            }
        }

        private async Task SyncTableSince(CloudTable table, DateTimeOffset timestamp)
        {
            var query = new TableQuery<DynamicTableEntity>
                            {
                                FilterString = TableQuery.GenerateFilterConditionForDate("Timestamp", "gt", timestamp),
                            };

            IList<DynamicTableEntity> allFuckingEntities = await table.ExecuteQueryAsync(
                query, _token, list => Console.WriteLine("loaded {0} rows", list.Count));

            CloudTable dstTable = _dstClient.GetTableReference(table.Name);
            await dstTable.CreateIfNotExistsAsync();

            int n = 0;
            DateTimeOffset maxSourceTs = timestamp;

            foreach (var batch1 in allFuckingEntities.GroupBy(x => x.PartitionKey))
            {
                if (_token.IsCancellationRequested)
                    return;

                foreach (var batch2 in batch1.Batch(100))
                {
                    if (_token.IsCancellationRequested)
                        return;

                    var op = new TableBatchOperation();

                    foreach (DynamicTableEntity entity in batch2)
                    {
                        op.InsertOrReplace(entity);

                        if (entity.Timestamp > maxSourceTs)
                        {
                            maxSourceTs = entity.Timestamp;
                        }
                    }

                    await dstTable.ExecuteBatchAsync(op, _token);

                    n += Math.Min(op.Count, 100);
                    Console.WriteLine("sent {0} rows", n);
                }
            }

            _timestamps[table] = maxSourceTs;
        }

        private async Task<IDictionary<CloudTable, DateTimeOffset>> LoadTimestampsFromMetatable(
            IEnumerable<CloudTable> tables)
        {
            var result = new Dictionary<CloudTable, DateTimeOffset>();

            foreach (CloudTable table in tables)
            {
                result[table] = LowestTimestamp;
            }

            CloudTable metatable = _dstClient.GetTableReference(MetatableName);

            string tableList = string.Join(
                " or ", result.Select(x => TableQuery.GenerateFilterCondition("RowKey", "eq", x.Key.Name)));

            var query = new TableQuery<DynamicTableEntity>
                            {
                                FilterString = TableQuery.CombineFilters(
                                    TableQuery.GenerateFilterCondition("PartitionKey", "eq", "SyncTimestamps"), "and",
                                    tableList),
                                SelectColumns = new List<string> {"SourceTimestamp"}
                            };

            foreach (DynamicTableEntity entity in await metatable.ExecuteQueryAsync(query, _token))
            {
                KeyValuePair<CloudTable, DateTimeOffset> kv =
                    result.First(x => x.Key.Name == entity.RowKey);

                result[kv.Key] = entity["SourceTimestamp"].DateTimeOffsetValue ?? LowestTimestamp;
            }

            return result;
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Tasync
{
    public static class StorageExtensions
    {
        public static Task<TableResult> ExecuteAsync(this CloudTable table,
                                                     TableOperation operation,
                                                     CancellationToken ct = default(CancellationToken))
        {
            return Task.Factory.FromAsync<TableOperation, TableResult>(
                (a, b, c) =>
                    {
                        ICancellableAsyncResult ar = table.BeginExecute(a, b, c);
                        ct.Register(ar.Cancel);
                        return ar;
                    },
                table.EndExecute,
                operation, null);
        }

        public static Task<IList<TableResult>> ExecuteBatchAsync(this CloudTable table,
                                                                 TableBatchOperation operation,
                                                                 CancellationToken ct = default(CancellationToken))
        {
            return Task.Factory.FromAsync<TableBatchOperation, IList<TableResult>>(
                (a, b, c) =>
                    {
                        ICancellableAsyncResult ar = table.BeginExecuteBatch(a, b, c);
                        ct.Register(ar.Cancel);
                        return ar;
                    },
                table.EndExecuteBatch,
                operation, null);
        }

        public static Task<IEnumerable<T>> ExecuteQueryAsync<T>(this CloudTable table,
                                                                TableQuery<T> query,
                                                                CancellationToken ct = default(CancellationToken))
            where T : ITableEntity, new()
        {
            var token = new TableContinuationToken();

            return Task.Factory.FromAsync<TableQuery<T>, TableContinuationToken, IEnumerable<T>>(
                (a, b, c, d) =>
                    {
                        ICancellableAsyncResult ar = table.BeginExecuteQuerySegmented(a, b, c, d);
                        ct.Register(ar.Cancel);
                        return ar;
                    },
                table.EndExecuteQuerySegmented<T>,
                query, token, null);
        }
    }
}
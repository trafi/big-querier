using Google.Cloud.BigQuery.V2;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Trafi.BigQuerier
{
    public class BigQueryTableClient : IBigQueryTableClient
    {
        private readonly BigQueryTable _table;

        public BigQueryTableClient(BigQueryTable table)
        {
            _table = table;
        }

        public async Task InsertRows(BigQueryInsertRow[] rows, CancellationToken ct)
        {
            try
            {
                await _table.InsertRowsAsync(
                    rows, 
                    new InsertOptions
                    {
                        AllowUnknownFields = true,
                    }, 
                    cancellationToken: ct
                );
            }
            catch (Exception ex)
            {
                throw new BigQuerierException($"Failed to insert row to {_table.FullyQualifiedId}", ex);
            }
        }
    }
}

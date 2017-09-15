using Google.Cloud.BigQuery.V2;
using System.Threading;
using System.Threading.Tasks;

namespace Trafi.BigQuerier
{
    public interface IBigQueryTableClient
    {
        /// <summary>
        /// Inserts rows.
        /// </summary>
        /// <param name="rows">Rows to insert</param>
        /// <param name="ct">Cancellation token</param>
        Task InsertRows(BigQueryInsertRow[] rows, CancellationToken ct);
    }
}

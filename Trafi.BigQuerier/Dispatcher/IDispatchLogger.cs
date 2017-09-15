using System;
using Google.Cloud.BigQuery.V2;

namespace Trafi.BigQuerier.Dispatcher
{
    public interface IDispatchLogger
    {
        void WaitForEnd();
        void InsertRows(int insertRowsCount, string traceId);
        void InsertError(Exception ex, BigQueryInsertRow[] insertRows, string traceId);
        void UnsentRows(BigQueryInsertRow[] insertRows);
        void CannotAdd(BigQueryInsertRow row);
    }
}
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using System;

namespace Trafi.BigQuerier
{
    public class ContractCache
    {
        public TableSchema Schema;
        public Func<object, object> ValueToRow;
        public Func<BigQueryRow, object> ValueFromRow;

        public ContractCache(TableSchema schema, Func<object, object> valueToRow, Func<BigQueryRow, object> valueFromRow)
        {
            Schema = schema;
            ValueToRow = valueToRow;
            ValueFromRow = valueFromRow;
        }
    }
}

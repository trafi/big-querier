// Copyright 2021 TRAFI
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using System;

#nullable disable

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

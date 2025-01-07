// Copyright 2021 TRAFI
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Trafi.BigQuerier.Mapper;

namespace Trafi.BigQuerier;

public class Contract<T>
{
    private Contract(ContractCache cache)
    {
        Cache = cache;
    }

    public static Contract<T> Create()
    {
        var type = typeof(T);

        return new Contract<T>(
            new ContractCache(
                Record.GetSchema(type),
                Record.GetValueToBigQueryFunction(type),
                Record.GetValueFromBigQueryFunction(type)
            )
        );
    }

    public ContractCache Cache { get; }

    public TableSchema Schema => Cache.Schema;

#nullable disable

    public BigQueryInsertRow ToRow(T value)
    {
        return (BigQueryInsertRow) Cache.ValueToRow(value);
    }

#nullable restore

    public T FromRow(BigQueryRow resultRow)
    {
        return (T) Cache.ValueFromRow(resultRow);
    }
}

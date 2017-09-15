// Copyright 2017 TRAFI
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using System.Collections.Generic;
using Trafi.BigQuerier.Mapper;
using System.Threading.Tasks;
using System.Threading;

namespace Trafi.BigQuerier
{
    public class Contract<T>
    {
        private readonly ContractCache _cache;

        private Contract(ContractCache cache)
        {
            _cache = cache;
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

        public ContractCache Cache => _cache;

        public TableSchema Schema => _cache.Schema;
        public BigQueryInsertRow ToRow(T value)
        {
            return (BigQueryInsertRow)_cache.ValueToRow(value);
        }

        public T FromRow(BigQueryRow resultRow)
        {
            return (T)_cache.ValueFromRow(resultRow);
        }

        public async Task<IEnumerable<T>> FromRowsAsync(IAsyncEnumerable<BigQueryRow> rows, CancellationToken ct)
        {
            var results = new List<T>();

            using (var enumerator = rows.GetEnumerator())
            {
                while (await enumerator.MoveNext(ct))
                {
                    results.Add(FromRow(enumerator.Current));
                }
            }

            return results;
        }
    }
}

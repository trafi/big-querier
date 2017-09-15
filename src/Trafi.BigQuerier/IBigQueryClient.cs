// Copyright 2017 TRAFI
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using System.Threading;
using System.Threading.Tasks;

namespace Trafi.BigQuerier
{
    public interface IBigQueryClient
    {
        /// <summary>
        /// Gets or creates table and returns client to work with it.
        /// </summary>
        /// <param name="datasetId">Dataset id</param>
        /// <param name="tableId">Table id</param>
        /// <param name="schema">Schema</param>
        /// <param name="ct">Cancellation token</param>
        /// <param name="createOptions">Create options</param>
        /// <returns>Table client</returns>
        Task<IBigQueryTableClient> GetTableClient(
            string datasetId, 
            string tableId, 
            TableSchema schema, 
            CancellationToken ct,
            CreateTableOptions createOptions = null
        );
    }
}

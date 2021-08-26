// Copyright 2021 TRAFI
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

using System.Collections.Generic;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using System.Threading;
using System.Threading.Tasks;

namespace Trafi.BigQuerier
{
    public interface IBigQueryClient
    {
        Google.Cloud.BigQuery.V2.BigQueryClient InnerClient { get; }

        /// <summary>
        /// Gets or creates table and returns client to work with it.
        /// </summary>
        /// <param name="datasetId">Dataset id</param>
        /// <param name="tableId">Table id</param>
        /// <param name="schema">Schema</param>
        /// <param name="createDatasetOptions">BigQuery dataset creation options</param>
        /// <param name="ct">Cancellation token</param>
        /// <returns>Table client</returns>
        Task<IBigQueryTableClient> GetTableClient(
            string datasetId,
            string tableId,
            TableSchema schema,
            Dataset? createDatasetOptions = null,
            CancellationToken ct = default
        );

        /// <summary>
        /// Execute a query.
        /// </summary>
        /// <param name="sql">SQL</param>
        /// <param name="options">Query options</param>
        /// <param name="ct">Cancellation token</param>
        /// <returns>Rows</returns>
        Task<IAsyncEnumerable<BigQueryRow>> Query(string sql, QueryOptions? options = null,
            CancellationToken ct = default);
        
        /// <summary>
        /// Execute a query with parameters.
        /// </summary>
        /// <param name="sql">SQL</param>
        /// <param name="namedParameters">Query parameters</param>
        /// <param name="options">Query options</param>
        /// <param name="ct">Cancellation token</param>
        /// <returns>Rows</returns>
        Task<IAsyncEnumerable<BigQueryRow>> ParametricQuery(
            string sql,
            IList<BigQueryParameter> namedParameters,
            QueryOptions options,
            CancellationToken ct = default);
        
        /// <summary>
        /// Delete a table.
        /// </summary>
        /// <param name="datasetId">Dataset id</param>
        /// <param name="tableId">Table id</param>
        /// <param name="ct">Cancellation token</param>
        Task DeleteTable(string datasetId, string tableId, CancellationToken ct = default);
    }
}
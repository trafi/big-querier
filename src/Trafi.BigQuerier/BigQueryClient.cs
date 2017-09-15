// Copyright 2017 TRAFI
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2;
using Google.Apis.Bigquery.v2.Data;
using Google.Apis.Services;
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

namespace Trafi.BigQuerier
{
    /// <summary>
    /// Big queries for simple people.
    /// </summary>
    public class BigQueryClient : IBigQueryClient
    {
        private readonly Google.Cloud.BigQuery.V2.BigQueryClient _client;

        public BigQueryClient(
            string projectId,
            string certFileName,
            string certSecret,
            string email
        ) {
            if (!File.Exists(certFileName))
            {
                throw new BigQuerierException(
                    $"Initializing BigQueryClient failed: certificate not found:certFile={certFileName}"
                );
            }

            var certificate = new X509Certificate2(
                certFileName,
                certSecret,
                X509KeyStorageFlags.MachineKeySet | X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.Exportable
            );

            var serviceAccountCredential = new ServiceAccountCredential(
                new ServiceAccountCredential.Initializer(email)
                {
                    Scopes = new[] { BigqueryService.Scope.CloudPlatform }
                }.FromCertificate(certificate));

            _client = new BigQueryClientImpl(
                projectId, 
                new BigqueryService(new BaseClientService.Initializer()
                {
                    HttpClientInitializer = serviceAccountCredential,
                    ApplicationName = "BigQuery API Service",
                })
            );
        }

        public async Task DeleteTable(string datasetId, string tableId, CancellationToken ct)
        {
            try
            {
                await _client.DeleteTableAsync(datasetId, tableId, cancellationToken: ct);
            }
            catch (Exception ex)
            {
                throw new BigQuerierException($"Failed to create dataset {datasetId} or table {tableId}", ex);
            }
        }

        public async Task<IBigQueryTableClient> GetTableClient(
            string datasetId, 
            string tableId, 
            TableSchema schema, 
            CancellationToken ct, 
            CreateTableOptions createOptions = null
        ) {
            try
            {
                var dataset = await _client.GetOrCreateDatasetAsync(datasetId, cancellationToken: ct);
                var table = await dataset.GetOrCreateTableAsync(
                    tableId,
                    schema,
                    cancellationToken: ct,
                    createOptions: createOptions
                );

                return new BigQueryTableClient(table);
            }
            catch (Exception ex)
            {
                throw new BigQuerierException($"Failed to create dataset {datasetId} or table {tableId}", ex);
            }
        }

        public async Task<IAsyncEnumerable<BigQueryRow>> Query(string sql, CancellationToken ct)
        {
            try
            {
                var job = await _client.CreateQueryJobAsync(sql, cancellationToken: ct);
                await job.PollUntilCompletedAsync(cancellationToken: ct);
                var results = await job.GetQueryResultsAsync(cancellationToken: ct);
                return results.GetRowsAsync();
            }
            catch (Exception ex)
            {
                throw new BigQuerierException($"Failed to execute query job for sql {sql}", ex);
            }
        }
    }
}

// Copyright 2021 TRAFI
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

namespace Trafi.BigQuerier;

/// <summary>
/// Big queries for simple people.
/// </summary>
public class BigQueryClient : IBigQueryClient
{
    public Google.Cloud.BigQuery.V2.BigQueryClient InnerClient { get; }

    public BigQueryClient(
        string projectId,
        string certFileName,
        string certSecret,
        string email
    )
    {
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
                Scopes = new[] {BigqueryService.Scope.CloudPlatform}
            }.FromCertificate(certificate));

        InnerClient = new BigQueryClientImpl(
            projectId,
            new BigqueryService(new BaseClientService.Initializer()
            {
                HttpClientInitializer = serviceAccountCredential,
                ApplicationName = "BigQuery API Service",
            })
        );
    }

    public BigQueryClient(Google.Cloud.BigQuery.V2.BigQueryClient bigQueryClient)
    {
        InnerClient = bigQueryClient;
    }

    public async Task DeleteTable(
        string datasetId,
        string tableId,
        CancellationToken ct = default
    )
    {
        try
        {
            await InnerClient.DeleteTableAsync(datasetId, tableId, cancellationToken: ct);
        }
        catch (Exception ex)
        {
            throw new BigQuerierException($"Failed to delete table {datasetId}.{tableId}", ex);
        }
    }

    public async Task<IBigQueryTableClient> GetTableClient(
        string datasetId,
        string tableId,
        TableSchema schema,
        Dataset? createDatasetOptions = null,
        CancellationToken ct = default
    )
    {
        try
        {
            var datasetOptions = createDatasetOptions ?? new Dataset();
            var dataset = await InnerClient.GetOrCreateDatasetAsync(
                datasetId,
                datasetOptions,
                cancellationToken: ct);
            var table = await dataset.GetOrCreateTableAsync(
                tableId,
                schema,
                cancellationToken: ct);

            return new BigQueryTableClient(table);
        }
        catch (Exception ex)
        {
            throw new BigQuerierException($"Failed to create dataset {datasetId} or table {tableId}", ex);
        }
    }

    public async Task<IAsyncEnumerable<BigQueryRow>> Query(
        string sql,
        QueryOptions? options = null,
        CancellationToken ct = default
    )
    {
        BigQueryJob job;
        try
        {
            job = await InnerClient.CreateQueryJobAsync(sql, options: options, cancellationToken: ct, parameters: null);
        }
        catch (Exception ex)
        {
            throw new BigQuerierException($"Failed to create big query job for sql {sql}", ex);
        }

        try
        {
            await job.PollUntilCompletedAsync(cancellationToken: ct);
        }
        catch (Exception ex)
        {
            throw new BigQuerierException($"Failed to poll big query job to completion {sql}", ex, job.Status);
        }

        BigQueryResults results;

        try
        {
            results = await job.GetQueryResultsAsync(cancellationToken: ct);
        }
        catch (Exception ex)
        {
            throw new BigQuerierException($"Failed to get job results {sql}", ex, job.Status);
        }

        try
        {
            return results.GetRowsAsync();
        }
        catch (Exception ex)
        {
            throw new BigQuerierException($"Failed to get rows {sql}", ex);
        }
    }

    public async Task<IAsyncEnumerable<BigQueryRow>> ParametricQuery(
        string sql,
        IList<BigQueryParameter> namedParameters,
        QueryOptions options,
        CancellationToken ct = default)
    {
        BigQueryJob job;

        try
        {
            job = await InnerClient.CreateQueryJobAsync(sql, options: options, cancellationToken: ct, parameters: namedParameters);
        }
        catch (Exception ex)
        {
            throw new BigQuerierException($"Failed to create big query job for sql {sql}", ex);
        }

        try
        {
            await job.PollUntilCompletedAsync(cancellationToken: ct);
        }
        catch (Exception ex)
        {
            throw new BigQuerierException($"Failed to poll big query job to completion {sql}", ex,
                job.Status);
        }

        BigQueryResults results;

        try
        {
            results = await job.GetQueryResultsAsync(cancellationToken: ct);
        }
        catch (Exception ex)
        {
            throw new BigQuerierException($"Failed to get job results {sql}", ex, job.Status);
        }

        try
        {
            return results.GetRowsAsync();
        }
        catch (Exception ex)
        {
            throw new BigQuerierException($"Failed to get rows {sql}", ex);
        }
    }
}

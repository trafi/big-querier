// Copyright 2021 TRAFI
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;

namespace Trafi.BigQuerier.Dispatcher;

public class DispatcherService : IDisposable
{
    private struct QueueItem
    {
        public DateTime Time;
        public BigQueryInsertRow Row;
    }

    private readonly Func<DateTime, string> _tableNameFun;
    private readonly string _datasetId;
    private readonly IBigQueryClient _client;
    private readonly TableSchema _schema;
    private readonly int _batchSize;
    private readonly int _maxQueueLength;
    private readonly AsyncSemaphore _activeDispatchesSemaphore;
    private readonly ManualResetEventSlim _pauseManualResetEvent = new(false);
    private readonly ConcurrentDictionary<AsyncSemaphore.Entry, Task> _dispatchToBigQueryTasks = [];

    private readonly Dataset? _createDatasetOptions;

    private readonly TimeSpan _sendBatchInterval = TimeSpan.FromSeconds(2);
    private DateTimeOffset _lastDispatchCheck = DateTimeOffset.MinValue;

    private readonly ConcurrentQueue<QueueItem> _outboundQueue = new();
    private readonly Task _internalDispatchTask;
    private bool _disposing = false;

    private readonly IDispatchLogger? _logger;

    private TimeProvider _timeProvider = TimeProvider.System;

    public int QueueSize => _outboundQueue.Count;

    public DispatcherService(IBigQueryClient client,
        TableSchema schema,
        string datasetId,
        Func<DateTime, string> tableNameFun,
        int batchSize,
        int concurrentDispatches = 1,
        int maxQueueLength = 1_000_000,
        Dataset? createDatasetOptions = null,
        IDispatchLogger? logger = null)
    {
        _client = client;
        _schema = schema;
        _datasetId = datasetId;
        _tableNameFun = tableNameFun;
        _batchSize = batchSize;
        _maxQueueLength = maxQueueLength;
        _activeDispatchesSemaphore = new AsyncSemaphore(concurrentDispatches);
        _createDatasetOptions = createDatasetOptions;
        _logger = logger;

        _internalDispatchTask = Task.Run(RunInternalDispatch);
    }

    public DispatcherService(
        IBigQueryClient client,
        TableSchema schema,
        string datasetId,
        Func<DateTime, string> tableNameFun,
        Dataset? createDatasetOptions = null,
        IDispatchLogger? logger = null) :
        this(client, schema, datasetId, tableNameFun,
            batchSize: 100,
            concurrentDispatches: 1,
            maxQueueLength: 1_000_000,
            createDatasetOptions,
            logger)
    {
    }

    /// <summary>
    /// Enqueues a row to be dispatched to BigQuery.
    /// </summary>
    /// <param name="time">Time for determining table name, passed to tableNameFun</param>
    /// <param name="row">Row to insert to BigQuery</param>
    public void Dispatch(DateTime time, BigQueryInsertRow row)
    {
        var queueSize = _outboundQueue.Count;
        if (queueSize >= _maxQueueLength)
        {
            _logger?.CannotAdd(row);
            return;
        }

        var queueItem = new QueueItem
        {
            Row = row,
            Time = time
        };

        _outboundQueue.Enqueue(queueItem);
        if (queueSize + 1 == _batchSize)
            _pauseManualResetEvent.Set();
    }

    public async Task<BigQueryDataset> GetOrCreateDatasetAsync(CancellationToken ct = default)
    {
        return await _client.InnerClient.GetOrCreateDatasetAsync(
            _datasetId,
            _createDatasetOptions,
            cancellationToken: ct);
    }

    private async Task RunInternalDispatch()
    {
        while (true)
        {
            var pause = _sendBatchInterval - (_timeProvider.GetUtcNow() - _lastDispatchCheck);
            var queueSize = _outboundQueue.Count;
            var shouldPauseForUnfilledBatch = pause > TimeSpan.Zero && queueSize < _batchSize;
            if (shouldPauseForUnfilledBatch && !_disposing)
            {
                // Since ManualResetEventSlim doesn't have TimeProvider enabled API, we use CancellationTokenSource
                // to enable cancelling the wait from tests
                var cts = new CancellationTokenSource(pause, _timeProvider);
                try
                {
                    var signaled = _pauseManualResetEvent.Wait(pause, cts.Token);
                    if (signaled)
                        _pauseManualResetEvent.Reset();
                }
                catch (OperationCanceledException)
                {
                }

                // Update queue size after pause
                queueSize = _outboundQueue.Count;
            }

            _lastDispatchCheck = _timeProvider.GetUtcNow();

            if (queueSize == 0)
            {
                if (_disposing)
                    break;

                continue;
            }

            // Can enter up to the `concurrentDispatches` limit
            var entry = await _activeDispatchesSemaphore.Enter(CancellationToken.None);

            var batch = DequeueBatch(Math.Min(_batchSize, queueSize));
            // We track active dispatches to wait for them to finish when dispatcher is disposed/stopped/cancelled
            _dispatchToBigQueryTasks[entry] = Task.Run(async () =>
            {
                using (entry) // Release semaphore when done
                {
                    await DispatchToBigQuerySafe(batch, entry.Entrance);
                    _dispatchToBigQueryTasks.Remove(entry, out _);
                }
            });
        }

        // Wait for all remaining tasks to finish when dispatcher is disposed/stopped/cancelled
        await Task.WhenAll(_dispatchToBigQueryTasks.Values);
    }

    private List<QueueItem> DequeueBatch(int count)
    {
        var items = new List<QueueItem>(count);
        for (var i = 0; i < count; i++)
        {
            if (!_outboundQueue.TryDequeue(out var item))
                break;

            items.Add(item);
        }

        return items;
    }

    private async Task DispatchToBigQuerySafe(IReadOnlyCollection<QueueItem> items, int worker)
    {
        var traceId = Guid.NewGuid().ToString();

        try
        {
            await DispatchToBigQuery(items, worker, traceId);
        }
        catch (Exception e)
        {
            _logger?.InsertError(e, items.Select(b => b.Row).ToArray(), traceId);
        }
    }

    private async Task DispatchToBigQuery(IReadOnlyCollection<QueueItem> items, int worker, string traceId)
    {
        var sw = Stopwatch.StartNew();
        var stored = 0;

        foreach (var batch in items.GroupBy(item => _tableNameFun(item.Time)))
        {
            var tableName = batch.Key;
            var insertRows = batch.Select(b => b.Row).ToArray();
            try
            {
                var client = await _client.GetTableClient(
                    _datasetId, tableName, _schema,
                    createDatasetOptions: _createDatasetOptions);

                stored += insertRows.Length;
                _logger?.InsertRows(insertRows.Length, traceId);

                await client.InsertRows(insertRows, CancellationToken.None);
            }
            catch (Exception ex)
            {
                _logger?.InsertError(ex, insertRows, traceId);
            }
        }

        sw.Stop();
        _logger?.Stored(
            stored: stored,
            timeTakenMs: (int)sw.Elapsed.TotalMilliseconds,
            remainingInQueue: _outboundQueue.Count,
            traceId: traceId,
            worker: worker);
    }

    public void Dispose()
    {
        _disposing = true;
        _pauseManualResetEvent.Set();

        _logger?.WaitForEnd();
        _internalDispatchTask.Wait();

        if (!_outboundQueue.IsEmpty)
            _logger?.UnsentRows(_outboundQueue.Select(q => q.Row).ToArray());
    }

    internal static class TestAccessor
    {
        internal static void SetTimeProvider(DispatcherService dispatcherService, TimeProvider timeProvider)
            => dispatcherService._timeProvider = timeProvider;

        internal static ICollection<Task> GetDispatchToBigQueryTasks(DispatcherService dispatcherService)
            => dispatcherService._dispatchToBigQueryTasks.Values;
    }
}

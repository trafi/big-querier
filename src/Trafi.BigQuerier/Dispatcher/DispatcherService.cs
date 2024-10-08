﻿// Copyright 2021 TRAFI
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

namespace Trafi.BigQuerier.Dispatcher
{
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

        private readonly Dataset? _createDatasetOptions;

        private readonly TimeSpan _timeToFinish = TimeSpan.FromSeconds(5);
        private readonly TimeSpan _storageRestDuration = TimeSpan.FromMilliseconds(500);
        private readonly TimeSpan _sendBatchInterval = TimeSpan.FromSeconds(2);
        private const int MaxQueueLength = 1_000_000;

        private DateTime _lastBatchSent = DateTime.MinValue;

        private readonly BlockingCollection<QueueItem> _queue = new BlockingCollection<QueueItem>(MaxQueueLength);
        private readonly ConcurrentQueue<QueueItem> _storageQueue = new ConcurrentQueue<QueueItem>();
        private readonly CancellationTokenSource _tokenSource;
        private readonly Task _consumeTask;
        private readonly Task _storageTask;

        private readonly IDispatchLogger? _logger;

        public int? LastEntriesBatchSize { get; set; }
        public TimeSpan? LastBatchSendTime { get; set; }
        public int QueueSize => _queue.Count;

        public DispatcherService(IBigQueryClient client,
            TableSchema schema,
            string datasetId,
            Func<DateTime, string> tableNameFun,
            int batchSize,
            Dataset? createDatasetOptions = null,
            IDispatchLogger? logger = null)
        {
            _client = client;
            _schema = schema;
            _datasetId = datasetId;
            _tableNameFun = tableNameFun;
            _batchSize = batchSize;
            _createDatasetOptions = createDatasetOptions;
            _logger = logger;

            _tokenSource = new CancellationTokenSource();
            _consumeTask = Task.Factory.StartNew(() => RunConsume(_tokenSource.Token), TaskCreationOptions.LongRunning);
            _storageTask = Task.Factory.StartNew(() => RunStorage(_tokenSource.Token),
                TaskCreationOptions.LongRunning);
        }
        
        public DispatcherService(
            IBigQueryClient client,
            TableSchema schema,
            string datasetId,
            Func<DateTime, string> tableNameFun,
            Dataset? createDatasetOptions = null,
            IDispatchLogger? logger = null) : this(client, schema, datasetId, tableNameFun, 100, createDatasetOptions, logger)
        {
        }

        public void Dispatch(DateTime time, BigQueryInsertRow row)
        {
            Dispatch(time, row, CancellationToken.None);
        }

        public void Dispatch(DateTime time, BigQueryInsertRow row, CancellationToken ct)
        {
            var queueItem = new QueueItem
            {
                Row = row,
                Time = time
            };
            if (!_queue.TryAdd(queueItem, 0, ct))
                _logger?.CannotAdd(queueItem.Row);
        }

        public async Task<BigQueryDataset> GetOrCreateDatasetAsync(CancellationToken ct = default)
        {
            return await _client.InnerClient.GetOrCreateDatasetAsync(
                _datasetId,
                _createDatasetOptions,
                cancellationToken: ct);
        }

        private void RunConsume(CancellationToken ct)
        {
            try
            {
                foreach (var item in _queue.GetConsumingEnumerable(ct))
                {
                    _storageQueue.Enqueue(item);
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        private void RunStorage(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                if (_storageQueue.Count >= _batchSize)
                {
                    Save(_batchSize, ct: ct);
                }
                else if (DateTime.UtcNow.Subtract(_lastBatchSent) > _sendBatchInterval && !_storageQueue.IsEmpty)
                {
                    Save(_storageQueue.Count, ct: ct);
                }
                ct.WaitHandle.WaitOne(_storageRestDuration);
            }

            while (!_storageQueue.IsEmpty)
            {
                var count = Math.Min(_storageQueue.Count, _batchSize);
                Save(count, ct: ct);
            }
        }

        private void Save(int count, CancellationToken ct = default)
        {
            var items = new List<QueueItem>();
            for (var i = 0; i < count; i++)
            {
                if (_storageQueue.TryDequeue(out var item))
                    items.Add(item);
            }
            Store(items, ct: ct);
            _lastBatchSent = DateTime.UtcNow;
        }

        private void Store(IReadOnlyCollection<QueueItem> items, CancellationToken ct = default)
        {
            var sw = Stopwatch.StartNew();
            var traceId = Guid.NewGuid().ToString();
            var stored = 0;
            foreach (var batch in items.GroupBy(item => _tableNameFun(item.Time)))
            {
                var tableName = batch.Key;
                var insertRows = batch.Select(b => b.Row).ToArray();
                try
                {
                    var client = _client.GetTableClient(_datasetId, tableName, _schema,
                        createDatasetOptions: _createDatasetOptions,
                        ct: ct).Result;

                    stored += insertRows.Length;
                    _logger?.InsertRows(insertRows.Length, traceId);

                    client.InsertRows(insertRows, CancellationToken.None).Wait(ct);
                }
                catch (Exception ex)
                {
                    _logger?.InsertError(ex, insertRows, traceId);
                }
            }
            sw.Stop();
            LastBatchSendTime = sw.Elapsed;
            LastEntriesBatchSize = items.Count;
            _logger?.Stored(
                stored: stored,
                timeTakenMs: (int)sw.Elapsed.TotalMilliseconds,
                remainingInQueue: _storageQueue.Count,
                traceId: traceId);
        }

        public void Dispose()
        {
            _queue.CompleteAdding();
            _consumeTask.Wait(_timeToFinish);
            _tokenSource.Cancel();

            _logger?.WaitForEnd();
            _storageTask.Wait();

            if (!_storageQueue.IsEmpty)
                _logger?.UnsentRows(_storageQueue.Select(q => q.Row).ToArray());
        }
    }
}

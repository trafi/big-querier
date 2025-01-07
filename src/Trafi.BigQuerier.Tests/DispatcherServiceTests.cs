using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Microsoft.Extensions.Time.Testing;
using Moq;
using NUnit.Framework;
using Trafi.BigQuerier.Dispatcher;

namespace Trafi.BigQuerier.Tests;

public class DispatcherServiceTests
{
    private FakeTimeProvider _timeProvider = null!;

    [SetUp]
    public void SetUp()
    {
        _timeProvider = new FakeTimeProvider();
        _timeProvider.SetUtcNow(new DateTimeOffset(2024, 01, 01, 00, 00, 00, TimeSpan.Zero));
    }

    [Test]
    public async Task GivenMoreRowsThanCanFitIntoBatchesOfAvailableWorkers_RemainingRowsAreHandledOnceAWorkerIsFreed()
    {
        var (bigQueryClientMock, bigQueryTableClientMock, _) =
            SetupClients(TimeSpan.FromSeconds(60), _timeProvider);

        var dispatcher = new DispatcherService(bigQueryClientMock.Object, new TableSchema(), "datasetId", dt => $"r{dt.Date:yyyyMMdd}",
            batchSize: 10,
            concurrentDispatches: 3);
        DispatcherService.TestAccessor.SetTimeProvider(dispatcher, _timeProvider);
        await Task.Delay(100);

        // At this point RunInternalDispatch is pausing for 2 seconds

        // Four batches worth of rows added
        // Adding more than a batche's worth of rows cancels the waiting period
        DispatchRows(dispatcher, 33);
        await Task.Delay(100);

        // 3 workers have picked up 10 rows each
        VerifyInsertedRows(bigQueryTableClientMock, numberOfRows: 10, numberOfTimes: 3);
        bigQueryTableClientMock.VerifyNoOtherCalls();

        // Inserting is configured to last 60 seconds so nothing happens after the pause
        _timeProvider.Advance(TimeSpan.FromSeconds(3));
        await Task.Delay(100);
        bigQueryTableClientMock.VerifyNoOtherCalls();

        // Insert succeeded, 3 workers are ready to pick up work
        _timeProvider.Advance(TimeSpan.FromSeconds(60));
        await Task.Delay(100);

        // Remaining 3 rows are taken into a single batch
        VerifyInsertedRows(bigQueryTableClientMock, numberOfRows: 3, numberOfTimes: 1);
        bigQueryTableClientMock.VerifyNoOtherCalls();

        // Make sure nothing else happens after all 33 rows have been dispatched
        _timeProvider.Advance(TimeSpan.FromSeconds(100));
        await Task.Delay(100);
        bigQueryTableClientMock.VerifyNoOtherCalls();
    }

    [Test]
    public async Task Dispose_WhenThereAreManyRowsToDispatch_ExitsPauseBetweenRuns()
    {
        var (bigQueryClientMock, bigQueryTableClientMock, _) =
            SetupClients(TimeSpan.Zero, _timeProvider);

        var loggerMock = new Mock<IDispatchLogger>();

        var dispatcher = new DispatcherService(bigQueryClientMock.Object, new TableSchema(), "datasetId", dt => $"r{dt.Date:yyyyMMdd}",
            logger: loggerMock.Object,
            batchSize: 10,
            concurrentDispatches: 3);
        DispatcherService.TestAccessor.SetTimeProvider(dispatcher, _timeProvider);
        await Task.Delay(100);

        // At this point RunInternalDispatch is pausing for 2 seconds

        // Seven batches worth of rows added
        DispatchRows(dispatcher, 66);

        // Act - stopping during pause between runs
        var disposeTask = Task.Run(() => dispatcher.Dispose());

        await Task.Delay(100);

        // Notice we didn't move the clock forward for 2 seconds to exit the pause.
        // The dispatcher should exit the pause when disposed to complate as much work as possible.

        // 3 workers have picked up 3*10*2+6 rows
        VerifyInsertedRows(bigQueryTableClientMock, numberOfRows: 10, numberOfTimes: 6);
        VerifyInsertedRows(bigQueryTableClientMock, numberOfRows: 6, numberOfTimes: 1);
        bigQueryTableClientMock.VerifyNoOtherCalls();

        await disposeTask;

        loggerMock.Verify(l => l.WaitForEnd(), Times.Once);
        loggerMock.Verify(l => l.UnsentRows(It.IsAny<BigQueryInsertRow[]>()), Times.Never);

        var remainingDispatchTasks = DispatcherService.TestAccessor.GetDispatchToBigQueryTasks(dispatcher);
        Assert.That(remainingDispatchTasks.Count, Is.Zero);
    }

    [Test]
    public async Task GivenRowsFromDifferentDays_TheyAreInsertedIntoCorrespondingTables()
    {
        var (bigQueryClientMock, bigQueryTable0101ClientMock, bigQueryTable0102ClientMock) =
            SetupClients(TimeSpan.Zero, _timeProvider);

        var dispatcher = new DispatcherService(bigQueryClientMock.Object, new TableSchema(), "datasetId", dt => $"r{dt.Date:yyyyMMdd}",
            batchSize: 10,
            concurrentDispatches: 1);
        DispatcherService.TestAccessor.SetTimeProvider(dispatcher, _timeProvider);
        await Task.Delay(100);

        // At this point RunInternalDispatch is pausing for 2 seconds

        // Batch 1: 7 rows for 2024-01-01 3 rows for 2024-01-02
        // Batch 2: 8 rows for 2024-01-01 2 rows for 2024-01-02
        DispatchRows(dispatcher, 7);
        DispatchRows(dispatcher, 5, _timeProvider.GetUtcNow().DateTime.AddDays(1));
        DispatchRows(dispatcher, 8);

        // Finishing the pause
        _timeProvider.Advance(TimeSpan.FromSeconds(3));
        await Task.Delay(100);

        // 3 workers have picked up 10 rows each
        VerifyInsertedRows(bigQueryTable0101ClientMock, numberOfRows: 7, numberOfTimes: 1);
        VerifyInsertedRows(bigQueryTable0102ClientMock, numberOfRows: 3, numberOfTimes: 1);
        VerifyInsertedRows(bigQueryTable0102ClientMock, numberOfRows: 2, numberOfTimes: 1);
        VerifyInsertedRows(bigQueryTable0101ClientMock, numberOfRows: 8, numberOfTimes: 1);
        bigQueryTable0101ClientMock.VerifyNoOtherCalls();
        bigQueryTable0102ClientMock.VerifyNoOtherCalls();

        // Make sure nothing else happens after all 33 rows have been dispatched
        _timeProvider.Advance(TimeSpan.FromSeconds(100));
        await Task.Delay(100);
        bigQueryTable0101ClientMock.VerifyNoOtherCalls();
        bigQueryTable0102ClientMock.VerifyNoOtherCalls();
    }

    [Test]
    public async Task GivenMoreRowsThanCanFitInAQueue_OverflowRowsAreDropped()
    {
        var (bigQueryClientMock, bigQueryTableClientMock, _) =
            SetupClients(TimeSpan.FromSeconds(60), _timeProvider);

        var loggerMock = new Mock<IDispatchLogger>();

        var dispatcher = new DispatcherService(bigQueryClientMock.Object, new TableSchema(),
            "datasetId", dt => $"r{dt.Date:yyyyMMdd}",
            logger: loggerMock.Object,
            batchSize: 10,
            maxQueueLength: 100,
            concurrentDispatches: 3);
        DispatcherService.TestAccessor.SetTimeProvider(dispatcher, _timeProvider);
        await Task.Delay(100);

        // At this point RunInternalDispatch is pausing for 2 seconds

        // Four batches worth of rows added
        DispatchRows(dispatcher, 33);
        await Task.Delay(100);

        // 3 workers have picked up 10 rows each
        VerifyInsertedRows(bigQueryTableClientMock, numberOfRows: 10, numberOfTimes: 3);
        bigQueryTableClientMock.VerifyNoOtherCalls();

        // There are 3 rows left in the queue at this moment, 3 should be dropped
        DispatchRows(dispatcher, 100);
        await Task.Delay(200);

        loggerMock.Verify(l => l.CannotAdd(It.IsAny<BigQueryInsertRow>()), Times.Exactly(3));
        Assert.That(dispatcher.QueueSize, Is.EqualTo(100));
    }

    [Test]
    public async Task GivenTableNameResolverFuncThatThrows_ErrorsAreReportedToLogger()
    {
        var (bigQueryClientMock, bigQueryTable0101ClientMock, bigQueryTable0102ClientMock) =
            SetupClients(TimeSpan.Zero, _timeProvider);

        static string ThrowingTableNameResolver(DateTime d) =>
            d.Day == 1 ? "r20240101" : throw new Exception("Test exception");

        var loggerMock = new Mock<IDispatchLogger>();

        var dispatcher = new DispatcherService(bigQueryClientMock.Object, new TableSchema(), "datasetId",
            ThrowingTableNameResolver,
            logger: loggerMock.Object,
            batchSize: 10,
            concurrentDispatches: 2);
        DispatcherService.TestAccessor.SetTimeProvider(dispatcher, _timeProvider);
        await Task.Delay(100);

        // At this point RunInternalDispatch is pausing for 2 seconds

        // Batch 1: 10 rows for 2024-01-01
        // Batch 2: 2 rows for 2024-01-01 3 rows for 2024-01-02
        DispatchRows(dispatcher, 12);
        DispatchRows(dispatcher, 3, _timeProvider.GetUtcNow().DateTime.AddDays(1));

        // Finishing the pause to prcess the first batch
        _timeProvider.Advance(TimeSpan.FromSeconds(3));
        await Task.Delay(100);
        // Since second batch is not full a pause is active
        _timeProvider.Advance(TimeSpan.FromSeconds(3));
        await Task.Delay(100);

        VerifyInsertedRows(bigQueryTable0101ClientMock, numberOfRows: 10, numberOfTimes: 1);
        VerifyInsertedRows(bigQueryTable0102ClientMock, numberOfRows: null, numberOfTimes: 0);
        bigQueryTable0101ClientMock.VerifyNoOtherCalls();
        bigQueryTable0102ClientMock.VerifyNoOtherCalls();

        loggerMock.Verify(l => l.InsertError(
            It.Is<Exception>(e => e.Message == "Test exception"),
            It.Is<BigQueryInsertRow[]>(r => r.Length == 5),
            It.Is<string>(a => Guid.Parse(a) != Guid.Empty)), Times.Exactly(1));

        // Make sure nothing else happens
        _timeProvider.Advance(TimeSpan.FromSeconds(100));
        await Task.Delay(100);
        bigQueryTable0101ClientMock.VerifyNoOtherCalls();
        bigQueryTable0102ClientMock.VerifyNoOtherCalls();
    }

    private void DispatchRows(DispatcherService dispatcher, int count, DateTime? date = null)
    {
        date ??= _timeProvider.GetUtcNow().DateTime;

        for (int i = 0; i < count; i++)
            dispatcher.Dispatch(date.Value, new BigQueryInsertRow());
    }

    private (Mock<IBigQueryClient>, Mock<IBigQueryTableClient>, Mock<IBigQueryTableClient>) SetupClients(TimeSpan delay, TimeProvider timeProvider)
    {
        var bigQueryTable0101ClientMock = new Mock<IBigQueryTableClient>();
        bigQueryTable0101ClientMock.Setup(c => c.InsertRows(It.IsAny<BigQueryInsertRow[]>(), It.IsAny<CancellationToken>()))
            .Returns(() => Task.Delay(delay, timeProvider));
        var bigQueryTable0102ClientMock = new Mock<IBigQueryTableClient>();
        bigQueryTable0102ClientMock.Setup(c => c.InsertRows(It.IsAny<BigQueryInsertRow[]>(), It.IsAny<CancellationToken>()))
            .Returns(() => Task.Delay(delay, timeProvider));

        var bigQueryClientMock = new Mock<IBigQueryClient>();
        bigQueryClientMock.Setup(c => c.GetTableClient("datasetId", "r20240101", It.IsAny<TableSchema>(), null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(bigQueryTable0101ClientMock.Object);
        bigQueryClientMock.Setup(c => c.GetTableClient("datasetId", "r20240102", It.IsAny<TableSchema>(), null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(bigQueryTable0102ClientMock.Object);

        return (bigQueryClientMock, bigQueryTable0101ClientMock, bigQueryTable0102ClientMock);
    }

    private static void VerifyInsertedRows(Mock<IBigQueryTableClient> mock, int? numberOfRows, int numberOfTimes)
    {
        mock.Verify(c => c.InsertRows(
                It.Is<BigQueryInsertRow[]>(r => numberOfRows == null || r.Length == numberOfRows),
                It.IsAny<CancellationToken>()),
            Times.Exactly(numberOfTimes));
    }
}

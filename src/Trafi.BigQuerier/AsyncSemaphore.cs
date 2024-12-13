using System;
using System.Threading;
using System.Threading.Tasks;

namespace Trafi.BigQuerier;

public class AsyncSemaphore
{
    private readonly SemaphoreSlim _inner;

    /// <summary>
    /// This initializes a <see cref="System.Threading.SemaphoreSlim"/> with initialCount and maxCount set to a value of <paramref name="allowedEntrances"/>
    /// </summary>
    /// <param name="allowedEntrances">A value used to initialize a <see cref="System.Threading.SemaphoreSlim"/> setting initialCount and maxCount</param>
    public AsyncSemaphore(int allowedEntrances)
    {
        _inner = new SemaphoreSlim(allowedEntrances, allowedEntrances);
    }

    public async Task<Entry> Enter()
    {
        await _inner.WaitAsync();
        return new Entry(_inner);
    }

    public async Task<Entry> Enter(CancellationToken ct)
    {
        await _inner.WaitAsync(ct);
        return new Entry(_inner);
    }

    public async Task<Entry> Enter(TimeSpan timeout)
    {
        await _inner.WaitAsync(timeout);
        return new Entry(_inner);
    }

    public class Entry : IDisposable
    {
        private readonly SemaphoreSlim _semaphore;

        public Entry(SemaphoreSlim semaphore)
        {
            _semaphore = semaphore;
        }

        public void Dispose()
        {
            _semaphore.Release();
        }
    }
}

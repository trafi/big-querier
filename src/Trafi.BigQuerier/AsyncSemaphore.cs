using System;
using System.Threading;
using System.Threading.Tasks;

namespace Trafi.BigQuerier;

public class AsyncSemaphore
{
    private readonly SemaphoreSlim _inner;
    private readonly int _allowedEntrances;

    /// <summary>
    /// This initializes a <see cref="System.Threading.SemaphoreSlim"/> with initialCount and maxCount set to a value of <paramref name="allowedEntrances"/>
    /// </summary>
    /// <param name="allowedEntrances">A value used to initialize a <see cref="System.Threading.SemaphoreSlim"/> setting initialCount and maxCount</param>
    public AsyncSemaphore(int allowedEntrances)
    {
        _allowedEntrances = allowedEntrances;
        _inner = new SemaphoreSlim(allowedEntrances, allowedEntrances);
    }

    public async Task<Entry> Enter()
    {
        await _inner.WaitAsync();
        return new Entry(_inner, _allowedEntrances - _inner.CurrentCount);
    }

    public async Task<Entry> Enter(CancellationToken ct)
    {
        await _inner.WaitAsync(ct);
        return new Entry(_inner, _allowedEntrances - _inner.CurrentCount);
    }

    public async Task<Entry> Enter(TimeSpan timeout)
    {
        await _inner.WaitAsync(timeout);
        return new Entry(_inner, _allowedEntrances - _inner.CurrentCount);
    }

    public class Entry : IDisposable
    {
        private readonly SemaphoreSlim _semaphore;
        public readonly int Entrance;

        public Entry(SemaphoreSlim semaphore, int entrance)
        {
            _semaphore = semaphore;
            Entrance = entrance;
        }

        public void Dispose()
        {
            _semaphore.Release();
        }
    }
}

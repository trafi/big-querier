// Copyright 2021 TRAFI
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

using System;
using Google.Cloud.BigQuery.V2;

namespace Trafi.BigQuerier.Dispatcher;

public interface IDispatchLogger
{
    void WaitForEnd();
    void InsertRows(int insertRowsCount, string traceId);
    void InsertError(Exception ex, BigQueryInsertRow[] insertRows, string traceId);
    void UnsentRows(BigQueryInsertRow[] insertRows);
    void CannotAdd(BigQueryInsertRow row);
    /// <summary>
    /// Called after a batch is stored
    /// </summary>
    /// <param name="stored">Stored items count</param>
    /// <param name="timeTakenMs">Time taken to Store the batch in milliseconds</param>
    /// <param name="remainingInQueue">Items remaining in queue</param>
    /// <param name="traceId">TraceId, unique per Storage attempt</param>
    void Stored(int stored, int timeTakenMs, int remainingInQueue, string traceId, int worker);
}

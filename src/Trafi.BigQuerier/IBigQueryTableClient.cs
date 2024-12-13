// Copyright 2021 TRAFI
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

using Google.Cloud.BigQuery.V2;
using System.Threading;
using System.Threading.Tasks;

namespace Trafi.BigQuerier;

public interface IBigQueryTableClient
{
    /// <summary>
    /// Inserts rows.
    /// </summary>
    /// <param name="rows">Rows to insert</param>
    /// <param name="ct">Cancellation token</param>
    Task InsertRows(BigQueryInsertRow[] rows, CancellationToken ct);
}

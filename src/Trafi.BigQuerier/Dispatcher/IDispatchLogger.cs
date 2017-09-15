﻿// Copyright 2017 TRAFI
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

using System;
using Google.Cloud.BigQuery.V2;

namespace Trafi.BigQuerier.Dispatcher
{
    public interface IDispatchLogger
    {
        void WaitForEnd();
        void InsertRows(int insertRowsCount, string traceId);
        void InsertError(Exception ex, BigQueryInsertRow[] insertRows, string traceId);
        void UnsentRows(BigQueryInsertRow[] insertRows);
        void CannotAdd(BigQueryInsertRow row);
    }
}
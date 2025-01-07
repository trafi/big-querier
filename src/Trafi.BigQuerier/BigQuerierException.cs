// Copyright 2021 TRAFI
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

using System;
using Google.Apis.Bigquery.v2.Data;

namespace Trafi.BigQuerier;

public class BigQuerierException : Exception
{
    public JobStatus? JobStatus { get; }

    public BigQuerierException(string message, JobStatus? jobStatus = null) : base(
        CombineMessageWithJobErrorMessage(message, jobStatus))
    {
        JobStatus = jobStatus;
    }

    public BigQuerierException(string message, Exception innerException, JobStatus? jobStatus = null) : base(
        CombineMessageWithJobErrorMessage(message, jobStatus), innerException)
    {
        JobStatus = jobStatus;
    }

    private static string CombineMessageWithJobErrorMessage(string message, JobStatus? jobStatus)
    {
        if (jobStatus?.ErrorResult == null)
            return message;

        var reasonText = jobStatus.ErrorResult.Reason != null ? $", {jobStatus.ErrorResult.Reason}" : "";
        var jobError = (jobStatus.ErrorResult.Location != null
                           ? $"Error in {jobStatus.ErrorResult.Location}{reasonText}: "
                           : "")
                       + jobStatus.ErrorResult.Message;

        return $"{message}. {jobError}";
    }
}

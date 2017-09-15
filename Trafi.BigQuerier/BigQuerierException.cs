using System;

namespace Trafi.BigQuerier
{
    public class BigQuerierException : Exception
    {
        public BigQuerierException(string message) : base(message)
        {
        }

        public BigQuerierException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
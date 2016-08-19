using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Intratec.Common.Helpers.Extensions
{
    public static class ExceptionExtensions
    {
        /// <summary>
        /// Returns the exception message and messages of all inner exceptions
        /// </summary>
        /// <param name="exception"></param>
        /// <param name="withInnerExceptions">if true, the messages of the inner exceptions are added too</param>
        /// <returns></returns>
        public static string ToDebugString(this Exception exception, bool withInnerExceptions=true)
        {
            if (exception == null) return "<null>";

            List<Exception> exceptionList = new List<Exception>();

            exceptionList.Add(exception);

            if (exception is AggregateException)
            {
                exceptionList.AddRange((exception as AggregateException).InnerExceptions);
            }

            string exMessages = String.Empty;
            Func<Exception,Exception> innerSelector = null;

            if (withInnerExceptions)
            {
                innerSelector = ex => ex.InnerException;
            }
            else
            {
                innerSelector = ex => null;
            }

            try
            {
                IEnumerable<string> messages = exceptionList.SelectMany(ex => ex.FromHierarchy(innerSelector))
                    .Select(ex => String.Format("[{0}] {1}", ex.GetType().Name, ex.Message));

                exMessages = String.Join(",", messages);
            }
            catch (Exception ex)
            {
                exMessages = "Exception while processing inner messages: " + ex.Message;
            }

            return exMessages;
        }
    }
}

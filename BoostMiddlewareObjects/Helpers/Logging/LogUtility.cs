using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Intratec.Common.Services.Logging
{
    public class LogUtility
    {

        /// <summary>
        /// This methods formats an error message so that it is 
        /// in a nice format for the event log or other places
        /// </summary>
        /// <param name="x">The exception</param>
        /// <returns>A formatted error message</returns>
        public static string BuildExceptionMessage(Exception x)
        {

            Exception logException = x;
            if (x.InnerException != null)
                logException = x.InnerException;

            string strErrorMsg = String.Empty;

            // Get the error message
            strErrorMsg += Environment.NewLine + "Message :" + logException.Message;

            // Source of the message
            strErrorMsg += Environment.NewLine + "Source :" + logException.Source;

            // Stack Trace of the error

            strErrorMsg += Environment.NewLine + "Stack Trace :" + logException.StackTrace;

            // Method where the error occurred
            strErrorMsg += Environment.NewLine + "TargetSite :" + logException.TargetSite;
            return strErrorMsg;
        }
    }
}
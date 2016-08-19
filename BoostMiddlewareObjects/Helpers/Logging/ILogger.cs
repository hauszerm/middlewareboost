using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Intratec.Common.Services.Logging
{
    /// <summary>
    /// An interface that any logger can use
    /// </summary>
    public interface ILogger
    {
        void Info(string message);
        void InfoFormat(string format, object arg0);
        void InfoFormat(string format, object arg0, object arg1);
        void InfoFormat(string format, params object[] args);
        void InfoFormat(IFormatProvider provider, string format, params object[] args);

        void Warn(string message);
        void Warn(object message, Exception exception);
        void WarnFormat(string format, object arg0);
        void WarnFormat(string format, object arg0, object arg1);
        void WarnFormat(string format, object arg0, object arg1, object arg2);
        void WarnFormat(IFormatProvider provider, string format, params object[] args);

        void Debug(string message);
        void Debug(object message);
        void Debug(object message, Exception exception);
        void DebugFormat(string format, object arg0);
        void DebugFormat(string format, params object[] args);
        void DebugFormat(IFormatProvider provider, string format, params object[] args);

        void Error(string message);
        void Error(string message, Exception x);
        void Error(Exception x);        

        void Fatal(string message);
        void Fatal(Exception x);

    }
}
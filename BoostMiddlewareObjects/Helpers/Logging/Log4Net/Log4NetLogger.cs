using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

using log4net;

namespace Intratec.Common.Services.Logging.Log4Net
{
    public class Log4NetLogger : ILogger
    {

        private ILog _logger;

        public Log4NetLogger()
        {
            _logger = LogManager.GetLogger(this.GetType());
        }

        public Log4NetLogger(Type type)
        {
          _logger = LogManager.GetLogger(type);
        }

        public void Info(string message)
        {
            _logger.Info(message);
        }

        public void InfoFormat(string format, object arg0)
        {
          _logger.InfoFormat(format, arg0);
        }

        public void InfoFormat(string format, object arg0, object arg1)
        {
          _logger.InfoFormat(format, arg0, arg1);
        }

        public void InfoFormat(IFormatProvider provider, string format, params object[] args)
        {
          _logger.InfoFormat(provider, format, args);
        }

        public void InfoFormat(string format, params object[] args)
        {
          _logger.InfoFormat(format, args);
        }

        public void Warn(string message)
        {
            _logger.Warn(message);
        }

        public void Warn(object message, Exception exception)
        {
          _logger.Warn(message, exception);
        }
        
      public void WarnFormat(string format, object arg0)
        {
          _logger.WarnFormat(format, arg0);
        }

      public void WarnFormat(string format, object arg0, object arg1)
        {
          _logger.WarnFormat(format, arg0, arg1);
        }

        public void WarnFormat(IFormatProvider provider, string format, params object[] args)
        {
          _logger.WarnFormat(provider, format, args);
        }

        public void WarnFormat(string format, object arg0, object arg1, object arg2)
        {
          _logger.WarnFormat(format, arg0, arg1, arg2);
        }

      public void Debug(string message)
        {
            _logger.Debug(message);
        }

      public void Debug(object message)
      {
        _logger.Debug(message);
      }

      public void Debug(object message, Exception exception)
      {
        _logger.Debug(message, exception);
      }

      public void DebugFormat(string format, object arg0)
      {
        _logger.DebugFormat(format, arg0);
      }

      public void DebugFormat(string format, params object[] args)
      {
        _logger.DebugFormat(format, args);
      }

      public void DebugFormat(IFormatProvider provider, string format, params object[] args)
      {
        _logger.DebugFormat(provider, format, args);
      }

      public void Error(string message)
        {
            _logger.Error(message);            
        }

        public void Error(Exception x)
        {
            Error(LogUtility.BuildExceptionMessage(x));
        }

        public void Error(string message, Exception x)
        {
            _logger.Error(message, x);
        }

        public void Fatal(string message)
        {
            _logger.Fatal(message);
        }

        public void Fatal(Exception x)
        {
            Fatal(LogUtility.BuildExceptionMessage(x));
        }
    }
}
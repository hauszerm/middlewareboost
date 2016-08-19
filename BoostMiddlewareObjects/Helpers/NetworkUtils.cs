using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BoostMiddlewareObjects.Helpers
{
    public class NetworkUtils
    {
        public static string ConvertBytesToHumanReadable(long bytes)
        {
            string bytesReceived = (bytes > 1024 * 1024) ? string.Format("{0:0.00}Mb", ((float)bytes / (1024 * 1024)))
                : (bytes > 1024) ? string.Format("{0:0.00}kb", ((float)bytes / 1024))
                : string.Format("{0}bytes", bytes);
            return bytesReceived;
        }
    }
}

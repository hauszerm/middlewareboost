using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;

namespace BoostMiddlewareImporter.Server
{
    public class TCPMessage<TData>
    {
        public TCPMessage()
        {
            TimestampReceivedUTC = DateTime.UtcNow;
        }

        public TcpClient Client { get; set; }
        public byte[] RawData { get; set; }
        public TData Data { get; set; }

        public DateTime TimestampReceivedUTC { get; set; }
    }
}

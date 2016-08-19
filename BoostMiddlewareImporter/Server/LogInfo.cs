using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BoostMiddlewareImporter.Server
{
    public class LogInfo
    {
        //public TcpClient client { get; set; }
        public long totalBytesReceived { get; set; }
        public long totalBytesSent { get; set; }
        public long totalMessagesReceived { get; set; }
        public double messagesPerformance { get; set; }
        public long totalMessagesSent { get; set; }
        public Queue<float> ReceivePerformanceList { get; set; }
        public Queue<float> ReceiveDurationList { get; set; }
        public float averageReceivePerformance { get; set; }
        public float averageReceiveDuration { get; set; }

        public bool ActiveClient { get; set; }

        public Task Task { get; set; }

        public long PostMessageMilliseconds { get; set; }

        public LogInfo()
        {
            ReceivePerformanceList = new Queue<float>();
            ReceiveDurationList = new Queue<float>();

            ReInit();
        }

        public void ReInit()
        {
            totalBytesReceived = 0;
            totalBytesSent = 0;
            totalMessagesReceived = 0;
            messagesPerformance = 0;
            totalMessagesSent = 0;

            ReceivePerformanceList.Clear();
            ReceiveDurationList.Clear();

            averageReceiveDuration = 0;
            averageReceivePerformance = 0;

            ActiveClient = true;
            this.Task = null;
            PostMessageMilliseconds = 0;
        }

      
    }
}

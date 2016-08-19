using BoostMiddlewareObjects.Helpers;
using Intratec.Common.Services.Logging;
using Intratec.Common.Services.Logging.Log4Net;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BoostMiddlewareImporter.Server
{
    //Performance considerations see file://///at01/Ablage/Intratec_Doku/IntratecKB.html#C%23%20HighPerformance

    public abstract class AsyncServerBase<TData> : BoostMiddlewareImporter.Server.IMessageGenerator
    {
        protected static ILogger logger = new Log4NetLogger(typeof(AsyncServerBase<TData>));

        protected int _listeningPort;
        protected int connections = 0;

        // We know how many items we want to insert into the ConcurrentDictionary.
        // So set the initial capacity to some prime number above that, to ensure that
        // the ConcurrentDictionary does not need to be resized while initializing it.
        private static int initialCapacity = 2001;

        // The higher the concurrencyLevel, the higher the theoretical number of operations
        // that could be performed concurrently on the ConcurrentDictionary.  However, global
        // operations like resizing the dictionary take longer as the concurrencyLevel rises. 
        // For the purposes of this example, we'll compromise at numCores * 2.
        private static int numProcs = Environment.ProcessorCount;
        private static int concurrencyLevel = numProcs * 2;

#if TCPOnly
        protected ConcurrentDictionary<string, AsyncTcpServer.LogInfo> LogInfo = new ConcurrentDictionary<string, AsyncTcpServer.LogInfo>(concurrencyLevel, initialCapacity);
#else
        protected ConcurrentDictionary<string, LogInfo> LogInfo = new ConcurrentDictionary<string, LogInfo>(concurrencyLevel, initialCapacity);
#endif
        protected int _postMessageToProcessorTimeout = 0;
        protected IMessageProcessor<TData> _messageProcessor;
        
        protected TaskScheduler _ts;
        protected int _statisticPerMessagesCount;

        protected TroughputStopwatch totalMessagesThroughput = null;
        protected long msgReceivedCountTotal = 0;

        public int Connections
        {
            get
            {
                return this.connections;
            }
        }

        public abstract System.Threading.Tasks.Task Run(CancellationToken cancelToken);
        public abstract void Stop();

        public AsyncServerBase(int port, IMessageProcessor<TData> processor, TaskScheduler ts, int statisticPerMessagesCount)
        {
            _listeningPort = port;
            _messageProcessor = processor;
            _ts = ts;

            _statisticPerMessagesCount = statisticPerMessagesCount;

            if (_ts == null)
            {
                _ts = TaskScheduler.Default;
            }
        }

        
        
        
        protected void LogMessage(string message,
                                [CallerMemberName]string callername = "")
        {
            System.Console.WriteLine("[{0}] - Thread-{1}- {2}",
                    callername, Thread.CurrentThread.ManagedThreadId, message);
        }

        protected void GenerateStatistic(long byteCountTotal, ref long byteCountTotalBackup, long msgReceivedCountTotal, string clientInfo, Stopwatch stopwatch, TroughputStopwatch msgTroughput, Stopwatch swPostMessage)
        {
            stopwatch.Stop();
            long period = stopwatch.ElapsedMilliseconds; //milliseconds
            stopwatch.Restart();

            int bytecountInPeriod = (int)(byteCountTotal - byteCountTotalBackup);
            byteCountTotalBackup = byteCountTotal;
            float throughput = ((float)bytecountInPeriod / (float)period) * 1000; // bytes per second // ( * 1000) converts milliseconds to seconds
            float performance = (throughput / 1024.0f); // kbytes per second 
            //LogMessage(clientInfo + " sent " + bytecountInPeriod.ToString() + "(" + byteCountTotal.ToString() + ") bytes, D=" + ((float)((float)period / 1000)).ToString("0.000") + "s P=" +  performance.ToString("0.00") + "kb/s");

            LogStatistics(clientInfo, msgReceivedCountTotal, byteCountTotal, period, performance, msgTroughput.GetCurrentThroughput(), swPostMessage.ElapsedMilliseconds);
        }



        protected void LogStatistics(string clientInfo, long msgCountTotal, long byteCountTotal, long period, float performance, double messagesPerformance, long postMessageMilliseconds)
        {
#if TCPOnly
            AsyncTcpServer.LogInfo logInfo = LogInfo[clientInfo];
#else
            LogInfo logInfo = LogInfo[clientInfo];
#endif

            logInfo.totalMessagesReceived = msgCountTotal;
            logInfo.messagesPerformance = messagesPerformance;
            logInfo.totalBytesReceived = byteCountTotal;
            logInfo.ReceiveDurationList.Enqueue(((float)period / 1000));
            if (logInfo.ReceiveDurationList.Count > 20) logInfo.ReceiveDurationList.Dequeue();
            float sumDuration = 0;
            foreach (var item in logInfo.ReceiveDurationList)
            {
                sumDuration += item;
            }
            logInfo.averageReceiveDuration = sumDuration / logInfo.ReceiveDurationList.Count;

            logInfo.ReceivePerformanceList.Enqueue(performance);
            if (logInfo.ReceivePerformanceList.Count > 20) logInfo.ReceivePerformanceList.Dequeue();
            float sumPerformance = 0;
            foreach (var item in logInfo.ReceivePerformanceList)
            {
                sumPerformance += item;
            }
            logInfo.averageReceivePerformance = sumPerformance / logInfo.ReceivePerformanceList.Count;

            logInfo.PostMessageMilliseconds = postMessageMilliseconds;
            //LogInfo[clientInfo] = logInfo; //not required

        }

        public void PrintStatistics()
        {
            foreach (var item in LogInfo)
            {
                string bytesReceived = NetworkUtils.ConvertBytesToHumanReadable(item.Value.totalBytesReceived);

                Console.WriteLine("client {0} data={1}, M={2} M={5}/s T={6} D={3:0.00}s, P={4:0.00}kb/s PM={7}ms", item.Key, bytesReceived, msgReceivedCountTotal,
                    item.Value.averageReceiveDuration, item.Value.averageReceivePerformance, item.Value.messagesPerformance.ToString("0.0"), _postMessageToProcessorTimeout, item.Value.PostMessageMilliseconds);
            }

            PrintStatisticsCommon();
        }

        protected void PrintStatisticsCommon()
        {
            if (totalMessagesThroughput != null)
            {
                Console.WriteLine("TotalMessages: {0:0000.00}/s", totalMessagesThroughput.GetCurrentThroughput());
            }
        }

        public void PrintStatisticsAverage()
        {
            //Console.Clear();
            long totalBytesReceived = 0;
            long totalMessagesReceived = 0;
            double averageMessagePerformance = 0;
            float averageReceiveDuration = 0;
            float averageReceivePerformance = 0;

            var activeLogInfos = LogInfo.Where(i => i.Value.ActiveClient).ToList();

            long maxPostMessageMilliseconds = 0;

            double messagePerformanceWorst = 9999999;

            foreach (var item in activeLogInfos)
            {
                totalBytesReceived += item.Value.totalBytesReceived;
                averageMessagePerformance += item.Value.messagesPerformance;
                messagePerformanceWorst = Math.Min(messagePerformanceWorst, item.Value.messagesPerformance);
                totalMessagesReceived += item.Value.totalMessagesReceived;
                averageReceiveDuration += item.Value.averageReceiveDuration;
                averageReceivePerformance += item.Value.averageReceivePerformance;
                maxPostMessageMilliseconds = Math.Max(maxPostMessageMilliseconds, item.Value.PostMessageMilliseconds);
            }
            averageReceiveDuration = averageReceiveDuration / activeLogInfos.Count;
            averageReceivePerformance = averageReceivePerformance / activeLogInfos.Count;
            averageMessagePerformance = averageMessagePerformance / activeLogInfos.Count;

            string bytesReceived = NetworkUtils.ConvertBytesToHumanReadable(totalBytesReceived);

            Console.WriteLine("{0} clients data={1}, M={2} M={5}/s Mworst={8:0.0}/s T={6} D={3:0.00}s, P={4:0.00}kb/s PM(max)={7}ms", activeLogInfos.Count, bytesReceived, msgReceivedCountTotal,
                averageReceiveDuration, averageReceivePerformance, averageMessagePerformance.ToString("0.0"), _postMessageToProcessorTimeout, maxPostMessageMilliseconds, messagePerformanceWorst);

            PrintStatisticsCommon();
        }
        
    }
}

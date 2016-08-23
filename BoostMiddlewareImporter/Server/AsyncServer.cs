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

    public class AsyncServer<TData> : AsyncServerBase<TData>
    {
        private readonly int BUFFER_SIZE = 60000;
        private readonly int DATA_LENGTH_SIZE = 4;

        private TcpListener _listener;
        private bool _listenerStopped = false;

        private TaskFactory _taskFactory;
        

        public AsyncServer(int port, IMessageProcessor<TData> processor, TaskScheduler ts, int statisticPerMessagesCount):base(port, processor, ts, statisticPerMessagesCount)
        {
            if (_ts == null)
            {
                _ts = TaskScheduler.Default;
            }

            _taskFactory = new TaskFactory(
                    CancellationToken.None, TaskCreationOptions.DenyChildAttach,
                    TaskContinuationOptions.None, _ts);
        }
        ///<summary>
        /// Start listening for connection
        /// </summary>
        /// <param name="cancelToken">not used</param>
        public override async Task Run(CancellationToken cancelToken)
        {

            //IPAddress ipAddress = IPAddress.Loopback;
            IPAddress ipAddress = IPAddress.Any;
            _listener = new TcpListener(ipAddress, _listeningPort);
            _listener.Start();
            LogMessage("Server is running");
            LogMessage("Listening on port " + _listeningPort);

            TcpClient tcpClient = null;

            do
            {
                LogMessage("Waiting for connections...");
                try
                {
                    tcpClient = await _listener.AcceptTcpClientAsync().ConfigureAwait(false); //configureAwait(false), continue execution with different thread

                    if (tcpClient != null)
                    {
                        if (totalMessagesThroughput == null)
                        {
                            totalMessagesThroughput = new TroughputStopwatch();
                            totalMessagesThroughput.Start();
                        }

                        TcpClient taskTcpClient = tcpClient;

                        string clientInfo = tcpClient.Client.RemoteEndPoint.ToString();
                        LogInfo info = new LogInfo();
                        LogInfo.TryAdd(clientInfo, info);
                        //info.Task = Task.Run(async delegate { await HandleConnectionAsync(taskTcpClient); });
                        
                        /*use defined Task Scheduler*/
                        
                        info.Task = _taskFactory.Create((async () => await HandleConnectionAsync(taskTcpClient)));             //Create(TaskCreationOptions.LongRunning) causes a new thread for each task to start, that is not what we want here
                        info.Task.Start();

                        //info.Task = _taskFactory.Create((() => HandleConnectionAsync(taskTcpClient).Wait())); //causes a new thread for each task to start
                        

                        //use current task scheduler
                        //info.Task = HandleConnectionAsync(taskTcpClient);
                    }
                }
                catch (Exception exp)
                {
                    if (_listenerStopped)
                    {
                        tcpClient = null;
                    }
                    LogMessage(exp.ToString());
                }
            }
            while (tcpClient != null);

            await Task.WhenAll(LogInfo.Values.Select(x => x.Task).ToArray()); //CHG thk 23.08.2016 use await here now

            totalMessagesThroughput = null;
        }
        ///<summary>
        /// Process Individual client
        /// </summary>
        ///
        ///
        private async Task HandleConnectionAsync(TcpClient tcpClient)
        {
            //optimize TCP server http://stackoverflow.com/questions/22013072/tcplistener-based-application-that-does-not-scale-up-well/22222578#22222578
            //author of the optimize answer: https://github.com/vtortola/AynchronousTCPListener/blob/master/TCPServer/Program.cs

            //not required when we start this method using Task.Run() or taskFactory.Create()
            //await Task.Yield(); //ensure this method is executed asynchronously
                                //has no negative performance impact
                                //executed once per Client Connection
                                //http://stackoverflow.com/questions/22013072/tcplistener-based-application-that-does-not-scale-up-well/22222578#22222578
                                //https://github.com/vtortola/AynchronousTCPListener/blob/master/TCPServer/Program.cs
              

            byte[] dataLenBuffer = new byte[DATA_LENGTH_SIZE];
            byte[] dataBuffer = new byte[BUFFER_SIZE];

            long byteCountTotal = 0;
            long byteCountTotalBackup = 0;
            

            string clientInfo = tcpClient.Client.RemoteEndPoint.ToString();
            LogMessage(string.Format("Got connection request from {0}", clientInfo));

            LogInfo info = null;
            LogInfo.TryGetValue(clientInfo, out info);
            Interlocked.Increment(ref connections);

            Stopwatch stopwatch = new Stopwatch();
            TroughputStopwatch msgTroughput = new TroughputStopwatch();

            Stopwatch sw_postMessage = new Stopwatch();

            try
            {
                using (var networkStream = tcpClient.GetStream())
                //using (var reader = new StreamReader(networkStream))
                using (var bufferedStream = new BufferedStream(networkStream))
                //using (var reader = new StreamReader(bufferedStream))
                //using (var writer = new StreamWriter(networkStream))
                {
                    stopwatch.Start();
                    msgTroughput.Start();
                    //writer.AutoFlush = true;
                    int loop = 0;
                    //networkStream.ReadTimeout = 10;
                    while (!_listenerStopped)
                    {
                        loop++;
                        
                        //commented out for performance tests
                        //Array.Clear(dataBuffer, 0, dataBuffer.Length);
                        //Array.Clear(dataLenBuffer, 0, dataLenBuffer.Length);

                        //var data = await reader.ReadLineAsync();

                        // get length of packet    
                        int len = await bufferedStream.ReadAsync(dataLenBuffer, 0, DATA_LENGTH_SIZE).ConfigureAwait(false);
                        byteCountTotal += len;
                        while (len < DATA_LENGTH_SIZE)
                        {
                            logger.Warn("DataLenBuffer was not filled with first read!!!");
                            len = await bufferedStream.ReadAsync(dataLenBuffer, len - 1, DATA_LENGTH_SIZE).ConfigureAwait(false);
                            byteCountTotal += len;
                        }
                        // retrieve length
                        int dataLength = BitConverter.ToInt32(dataLenBuffer, 0);

                        // read data
                        int receivedBytes = 0;
                        int bytesToRead = dataLength;
                        int bufferSize = BUFFER_SIZE;
                        if ((bytesToRead > 0) && (bytesToRead < BUFFER_SIZE))
                        {
                            bufferSize = bytesToRead;
                        }
                        int bytesRead = 0;

                        //bytesRead = await bufferedStream.ReadAsync(dataBuffer, 0, bufferSize).ConfigureAwait(false);
                        bytesRead = bufferedStream.Read(dataBuffer, 0, bufferSize);
                        receivedBytes += bytesRead;
                        //LogMessage("received " + bytesRead.ToString() + " bytes over network");

                        //now we read as much bytes as defined in dataLength/bytesToRead
                        if ((bytesToRead - receivedBytes) > 0)
                        {
                            while (bytesRead > 0)
                            {
                                if ((bytesToRead > 0) && (bytesToRead - receivedBytes < BUFFER_SIZE))
                                {
                                    bufferSize = bytesToRead - receivedBytes;
                                    if (bufferSize <= 0)
                                    {
                                        bytesRead = 0;
                                        break;
                                    }
                                }

                                bytesRead = await bufferedStream.ReadAsync(dataBuffer, receivedBytes, bufferSize).ConfigureAwait(false);
                                //LogMessage("received " + bytesRead.ToString() + " bytes over network");
                                receivedBytes += bytesRead;

                                // check bytes received
                                if ((bytesToRead > 0) && (receivedBytes >= bytesToRead))
                                {
                                    bytesRead = 0;
                                    //LogMessage(" buffer=" + receivedBytes.ToString() + " bytes");
                                }
                            } //while bytesRead > 0
                        }

                        //TCPMessage<TData> msg = null;
                        TCPMessage<TData> msg = new TCPMessage<TData>() {
                            Client = tcpClient,
                            RawData = new byte[bytesToRead],
                        };

                        ////TODO we must handle the case where the bytesToRead-value is bigger than the bufferSize
                        Buffer.BlockCopy(dataBuffer, 0, msg.RawData, 0, bytesToRead);
                        
                        bool timeoutOccured = false;
                        //Task postMessage;
                        const int msgTimeout = 1000; //milliseconds

                        /*using Sync ProcessMessage() or Task.WhenAny() should be more performant */
                        do
                        {
                            CancellationTokenSource tokenSource = new CancellationTokenSource();

                            if (timeoutOccured)
                            {
                                Interlocked.Increment(ref this._postMessageToProcessorTimeout);
                                //LogMessage(String.Format("MessageProcessor did not accept the msg within expected time of {0}ms", msgTimeout));
                                LogMessage(String.Format("MessageProcessor did not accept the msg, waiting {0}ms and try again.", msgTimeout));
                                await Task.Delay(msgTimeout); //wait a moment and then try to add the message again
                            }

                            sw_postMessage.Restart();

                            /*this sync-method seems to enable more Messages/second to be processed (Tested with 300 messages per second), thk*/
                            if (_messageProcessor != null)
                            {
                                timeoutOccured = !_messageProcessor.ProcessMessage(msg);
                            }

                            /*
                             * this code is more correct than the .Wait() code below
                             * but detecting if the message was queued with a timeout might be an overkill
                             * so be add the message synchronly
                             */
                            //postMessage = _messageProcessor.ProcessMessageAsync(msg, tokenSource.Token);
                            //if (await Task.WhenAny(postMessage, Task.Delay(msgTimeout)).ConfigureAwait(false) == postMessage)
                            //{
                            //    // task completed within timeout
                            //    timeoutOccured = false;
                            //}
                            //else
                            //{
                            //    // timeout logic
                            //    timeoutOccured = true;
                            //    tokenSource.Cancel(); //cancel the current Add Task, we will try to run add the token again with a new task
                            //}

                            sw_postMessage.Stop();
                        }
                        while (timeoutOccured);


                        /*using .Wait() will freeze the current thread, that can case bad performance, thk*/
                        
                        //do
                        //{
                        //    if (timeoutOccured) {
                        //        Interlocked.Increment(ref this._postMessageToProcessorTimeout);
                        //        LogMessage(String.Format("MessageProcessor did not accept the msg within expected time of {0}ms", msgTimeout));
                        //    }
                        //    timeoutOccured = false;
                        //    postMessage = _messageProcessor.ProcessMessageAsync(msg);
                        //    timeoutOccured = true;
                        //}
                        //while (!postMessage.Wait(msgTimeout));

                        byteCountTotal += receivedBytes;
                        msgReceivedCountTotal++;
                        msgTroughput.AddDataCount(1);
                        if (totalMessagesThroughput != null)
                        {
                            //should always be non null, because should only be set to null after all this methods finished
                            totalMessagesThroughput.AddDataCount(1);
                        }

                        // logging
                        if ((loop > 0) && (loop % _statisticPerMessagesCount /*500*/) == 0)
                        {
                            GenerateStatistic(byteCountTotal, ref byteCountTotalBackup, msgReceivedCountTotal, clientInfo, stopwatch, msgTroughput, sw_postMessage);
                        }
                        //await writer.WriteLineAsync("from Server: " + dataFromServer);
                    }
                }
            }
            catch (Exception exp)
            {
                LogMessage(exp.Message);
            }
            finally
            {
                GenerateStatistic(byteCountTotal, ref byteCountTotalBackup, msgReceivedCountTotal, clientInfo, stopwatch, msgTroughput, sw_postMessage);

                LogMessage(clientInfo + " sent " + byteCountTotal.ToString() + " bytes");
                LogMessage(string.Format("Closing the client connection - {0}", clientInfo));
                
                info.ActiveClient = false;

                //keep all clients in the list, so that the statistics are available after connection end
                //LogInfo deleteInfo;
                //LogInfo.TryRemove(clientInfo, out deleteInfo);

                tcpClient.Close();
                if (connections > 0) Interlocked.Decrement(ref connections);
            }

        }

        
        public override void Stop()
        {
            _listenerStopped = true;
            _listener.Stop();
        }
    }

    static class TaskExtensions
    {
        /// <summary>
        /// Add this method to async() methods that are called without storing the return Task object anywhere.
        /// Suppresses the compiler warning.
        /// </summary>
        /// <param name="t"></param>
        /// <example>
        ///   MyMethodAsync().NoWarning();
        /// </example>
        /// <source>http://blog.cincura.net/233470-tcplistener-and-tcpclient-an-easy-to-use-example/</source>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void NoWarning(this Task t) { }
    }
}

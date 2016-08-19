using BoostMiddlewareObjects.Helpers;
using Intratec.Common.Services.Logging;
using Intratec.Common.Services.Logging.Log4Net;
using System;
using System.Collections;
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
    //IDEA I1
    // MSDN https://msdn.microsoft.com/de-de/library/system.net.sockets.socketasynceventargs(v=vs.110).aspx
    // Codeproject http://www.codeproject.com/Articles/22918/How-To-Use-the-SocketAsyncEventArgs-Class

    //IDEA I2
    //Codeproject http://www.codeproject.com/Articles/83102/C-SocketAsyncEventArgs-High-Performance-Socket-Cod

    //Performance considerations see file://///at01/Ablage/Intratec_Doku/IntratecKB.html#C%23%20HighPerformance

    public class AsyncSocketServer<TData> : AsyncServerBase<TData>
    {
        #region classes

        // Represents a collection of reusable SocketAsyncEventArgs objects.  
        class SocketAsyncEventArgsPool
        {
            Stack<SocketAsyncEventArgs> m_pool;

            // Initializes the object pool to the specified size
            //
            // The "capacity" parameter is the maximum number of 
            // SocketAsyncEventArgs objects the pool can hold
            public SocketAsyncEventArgsPool(int capacity)
            {
                m_pool = new Stack<SocketAsyncEventArgs>(capacity);
            }

            // Add a SocketAsyncEventArg instance to the pool
            //
            //The "item" parameter is the SocketAsyncEventArgs instance 
            // to add to the pool
            public void Push(SocketAsyncEventArgs item)
            {
                if (item == null) { throw new ArgumentNullException("Items added to a SocketAsyncEventArgsPool cannot be null"); }
                lock (m_pool)
                {
                    m_pool.Push(item);
                }
            }

            // Removes a SocketAsyncEventArgs instance from the pool
            // and returns the object removed from the pool
            public SocketAsyncEventArgs Pop()
            {
                lock (m_pool)
                {
                    return m_pool.Pop();
                }
            }

            // The number of SocketAsyncEventArgs instances in the pool
            public int Count
            {
                get { return m_pool.Count; }
            }

        }

        // This class creates a single large buffer which can be divided up 
        // and assigned to SocketAsyncEventArgs objects for use with each 
        // socket I/O operation.  t
        // This enables bufffers to be easily reused and guards against 
        // fragmenting heap memory.
        // 
        // The operations exposed on the BufferManager class msare not thread safe.
        class BufferManager
        {
            int m_numBytes;                 // the total number of bytes controlled by the buffer pool
            byte[] m_buffer;                // the underlying byte array maintained by the Buffer Manager
            Stack<int> m_freeIndexPool;     // 
            int m_currentIndex;
            int m_bufferSize;

            public BufferManager(int totalBytes, int bufferSize)
            {
                m_numBytes = totalBytes;
                m_currentIndex = 0;
                m_bufferSize = bufferSize;
                m_freeIndexPool = new Stack<int>();
            }

            // Allocates buffer space used by the buffer pool
            public void InitBuffer()
            {
                // create one big large buffer and divide that 
                // out to each SocketAsyncEventArg object
                m_buffer = new byte[m_numBytes];
            }

            // Assigns a buffer from the buffer pool to the 
            // specified SocketAsyncEventArgs object
            //
            // <returns>true if the buffer was successfully set, else false</returns>
            public bool SetBuffer(SocketAsyncEventArgs args)
            {

                if (m_freeIndexPool.Count > 0)
                {
                    args.SetBuffer(m_buffer, m_freeIndexPool.Pop(), m_bufferSize);
                }
                else
                {
                    if ((m_numBytes - m_bufferSize) < m_currentIndex)
                    {
                        return false;
                    }
                    args.SetBuffer(m_buffer, m_currentIndex, m_bufferSize);
                    m_currentIndex += m_bufferSize;
                }
                return true;
            }

            /// <summary>
            /// Ensure that the offset of the buffer is the original value
            /// </summary>
            /// <param name="receiveSendEventArgs"></param>
            /// <param name="offset"></param>
            internal void ResetBuffer(SocketAsyncEventArgs receiveSendEventArgs, int offset)
            {
                receiveSendEventArgs.SetBuffer(offset, m_bufferSize);
            }

            // Removes the buffer from a SocketAsyncEventArg object.  
            // This frees the buffer back to the buffer pool
            public void FreeBuffer(SocketAsyncEventArgs args)
            {
                m_freeIndexPool.Push(args.Offset);
                args.SetBuffer(null, 0, 0);
            }


        }

        #endregion

#region variablest
        
        private const Int32 DEFAULT_PORT = 51540, DEFAULT_NUM_CONNECTIONS = 10, DEFAULT_BUFFER_SIZE = Int16.MaxValue * 16;

        private int m_numConnections;   // the maximum number of connections the sample is designed to handle simultaneously 
        private int m_receiveBufferSize;// buffer size to use for each socket I/O operation 
        BufferManager m_bufferManager;  // represents a large reusable set of buffers for all socket operations
        const int opsToPreAlloc = 2;    // read, write (don't alloc buffer space for accepts)
        Socket listenSocket;            // the socket used to listen for incoming connection requests
        // pool of reusable SocketAsyncEventArgs objects for write, read and accept socket operations
        SocketAsyncEventArgsPool m_readWritePool;
        int m_totalBytesRead;           // counter of the total # bytes received by the server
        Semaphore m_maxNumberAcceptedClients;
        Hashtable m_clientBytesRead;
        private Object thisLock = new Object();

        private TaskCompletionSource<object> listenSocketClosed = null;

        private MessageHandler<TData> _messageHandler = new MessageHandler<TData>();
        private PrefixHandler<TData> _prefixHandler = new PrefixHandler<TData>();


#endregion


        /// <summary>
        /// 
        /// </summary>
        /// <param name="cancelToken">not used at the moment</param>
        /// <returns></returns>
        public override Task Run(CancellationToken cancelToken)
        {
            listenSocketClosed = new TaskCompletionSource<object>();
            Start();
            return listenSocketClosed.Task;
        }

        public override void Stop()
        {
            listenSocket.Close();
            listenSocket = null;

            if (listenSocketClosed != null)
            {
                listenSocketClosed.SetResult(null);
            }
        }


        // Create an uninitialized server instance.  
        // To start the server listening for connection requests
        // call the Init method followed by Start method 
        //
        // <param name="numConnections">the maximum number of connections the sample is designed to handle simultaneously</param>
        // <param name="receiveBufferSize">buffer size to use for each socket I/O operation</param>
        public AsyncSocketServer(int numConnections, int receiveBufferSize, int port, IMessageProcessor<TData> processor, TaskScheduler ts, int statisticPerMessagesCount):base(port, processor, ts, statisticPerMessagesCount)
        {
            m_totalBytesRead = 0;
            connections = 0;
            m_numConnections = numConnections;
            m_receiveBufferSize = receiveBufferSize;
            // allocate buffers such that the maximum number of sockets can have one outstanding read and 
            //write posted to the socket simultaneously  
            m_bufferManager = new BufferManager(receiveBufferSize * numConnections * opsToPreAlloc, receiveBufferSize);

            m_readWritePool = new SocketAsyncEventArgsPool(numConnections);
            m_maxNumberAcceptedClients = new Semaphore(numConnections, numConnections);
            m_clientBytesRead = new Hashtable();
        }


        // Initializes the server by preallocating reusable buffers and 
        // context objects.  These objects do not need to be preallocated 
        // or reused, but it is done this way to illustrate how the API can 
        // easily be used to create reusable objects to increase server performance.
        //
        public void Init()
        {
            // Allocates one large byte buffer which all I/O operations use a piece of.  This gaurds 
            // against memory fragmentation
            m_bufferManager.InitBuffer();

            // preallocate pool of SocketAsyncEventArgs objects
            SocketAsyncEventArgs readWriteEventArg;

            for (int i = 0; i < m_numConnections; i++)
            {
                //Pre-allocate a set of reusable SocketAsyncEventArgs
                readWriteEventArg = new SocketAsyncEventArgs();
                readWriteEventArg.Completed += new EventHandler<SocketAsyncEventArgs>(IO_Completed);

                // assign a byte buffer from the buffer pool to the SocketAsyncEventArg object
                m_bufferManager.SetBuffer(readWriteEventArg);

                //the AsyncUserToken must know the offset in the buffer
                //we use this position to reset to it for reading the message, thk
                readWriteEventArg.UserToken = new AsyncUserToken<TData>(readWriteEventArg.Offset)
                {
#if TCPOnly
                    Stats = new AsyncTcpServer.LogInfo(),
#else
                    Stats = new LogInfo(),
#endif

                };


                // add SocketAsyncEventArg to the pool
                m_readWritePool.Push(readWriteEventArg);
            }

        }

        public void Start()
        {
            // Get host related information.
            IPAddress[] addressList = Dns.GetHostEntry(Environment.MachineName).AddressList;

            // Get endpoint for the listener.
            IPEndPoint localEndPoint = new IPEndPoint(IPAddress.Any, _listeningPort);
            //IPEndPoint localEndPoint = new IPEndPoint(addressList[addressList.Length - 1], port);

            Start(localEndPoint);
        }

        // Starts the server such that it is listening for 
        // incoming connection requests.    
        //
        // <param name="localEndPoint">The endpoint which the server will listening 
        // for connection requests on</param>
        internal void Start(IPEndPoint localEndPoint)
        {
            // create the socket which listens for incoming connections
            listenSocket = new Socket(localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            listenSocket.Bind(localEndPoint);
            // start the server with a listen backlog of 100 connections
            listenSocket.Listen(100);

            LogMessage("Server is running");
            LogMessage("Listening on port " + localEndPoint.Port);
            LogMessage("Waiting for connections...");

            // post accepts on the listening socket
            StartAccept(null);


            //TODO!!!thk move console output here
            //Console.WriteLine("{0} connected sockets with one outstanding receive posted to each....press any key", m_outstandingReadCount);
            //Console.WriteLine("Press any key to terminate the server process....");
            //Console.ReadKey();  
        }


        // Begins an operation to accept a connection request from the client 
        //
        // <param name="acceptEventArg">The context object to use when issuing 
        // the accept operation on the server's listening socket</param>
        public void StartAccept(SocketAsyncEventArgs acceptEventArg)
        {
            if (acceptEventArg == null)
            {
                acceptEventArg = new SocketAsyncEventArgs();
                acceptEventArg.Completed += new EventHandler<SocketAsyncEventArgs>(AcceptEventArg_Completed);
            }
            else
            {
                // socket must be cleared since the context object is being reused
                acceptEventArg.AcceptSocket = null;
            }

            m_maxNumberAcceptedClients.WaitOne();

            if (listenSocket == null)
                return;
            
            bool willRaiseEvent = listenSocket.AcceptAsync(acceptEventArg);
            if (!willRaiseEvent)
            {
                ProcessAccept(acceptEventArg);
            }
        }

        // This method is the callback method associated with Socket.AcceptAsync 
        // operations and is invoked when an accept operation is complete
        //msgTroughput
        void AcceptEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            ProcessAccept(e);
        }

        private void ProcessAccept(SocketAsyncEventArgs e)
        {
            if (!e.AcceptSocket.Connected)
                return;

            //eval statistics
            if (totalMessagesThroughput == null)
            {
                totalMessagesThroughput = new TroughputStopwatch();
                totalMessagesThroughput.Start();
            }

            
            Interlocked.Increment(ref connections);

            LogMessage(string.Format("Client connection accepted from {0}. {1} clients connected to the server", e.RemoteEndPoint, connections));
            
            lock (thisLock)
            {
                m_clientBytesRead.Add(e.AcceptSocket, (long)0);
            }

            // Get the socket for the accepted client connection and put it into the 
            //ReadEventArg object user token
            SocketAsyncEventArgs readEventArgs = m_readWritePool.Pop();
            AsyncUserToken<TData> token = ((AsyncUserToken<TData>)readEventArgs.UserToken);
            token.Socket = e.AcceptSocket;
            
            /*eval statistics*/
            token.Stats.ReInit();
            token.Stopwatch.Start();
            token.MsgTroughput.Start();

            LogInfo.TryAdd(token.Socket.RemoteEndPoint.ToString(), token.Stats);
            
            //DOCU: e.RemoteEndPoint is NULL here only filled when used by Socket.ReceiveFromAsync or Socket.ReceiveMessageFromAsync

            readEventArgs.AcceptSocket = token.Socket;
            StartReceive(readEventArgs);
            // As soon as the client is connected, post a receive to the connection
            //bool willRaiseEvent = e.AcceptSocket.ReceiveAsync(readEventArgs);
            //if (!willRaiseEvent)
            //{
            //    ProcessReceive(readEventArgs);
            //}

            // Accept the next connection request
            StartAccept(e);
        }

        // This method is called whenever a receive or send operation is completed on a socket 
        //
        // <param name="e">SocketAsyncEventArg associated with the completed receive operation</param>
        void IO_Completed(object sender, SocketAsyncEventArgs e)
        {
            // determine which type of operation just completed and call the associated handler
            switch (e.LastOperation)
            {
                case SocketAsyncOperation.Receive:
                    ProcessReceive(e);
                    break;
                case SocketAsyncOperation.Send:
                    ProcessSend(e);
                    break;
                default:
                    throw new ArgumentException("The last operation completed on the socket was not a receive or send");
            }

        }



            // Set the receive buffer and post a receive op.
    private void StartReceive(SocketAsyncEventArgs receiveSendEventArgs)
    {
        AsyncUserToken<TData> token = receiveSendEventArgs.UserToken as AsyncUserToken<TData>;

        if (!receiveSendEventArgs.AcceptSocket.Connected)
        {
            return;
        }

        /*
         * KB: We do not need to change the receiveSendEventArgs.Buffer here
         *     the SAEA object already has a buffer since the init-method, reinitialize not required
         */

        //Set the buffer for the receive operation.
        //m_bufferManager.FreeBuffer(receiveSendEventArgs); //FROM I1
        //m_bufferManager.ResetBuffer(receiveSendEventArgs, token.bufferOffsetReceive); //is this required?
        //receiveSendEventArgs.SetBuffer(token.bufferOffsetReceive); //FROM I2

        // Post async receive operation on the socket.
        bool willRaiseEvent =
             receiveSendEventArgs.AcceptSocket.ReceiveAsync(receiveSendEventArgs);

        //Socket.ReceiveAsync returns true if the I/O operation is pending. The
        //SocketAsyncEventArgs.Completed event on the e parameter will be raised
        //upon completion of the operation. So, true will cause the IO_Completed
        //method to be called when the receive operation completes.
        //That's because of the event handler we created when building
        //the pool of SocketAsyncEventArgs objects that perform receive/send.
        //It was the line that said
        //eventArgObjectForPool.Completed +=
        //     new EventHandler<SocketAsyncEventArgs>(IO_Completed);

        //Socket.ReceiveAsync returns false if I/O operation completed synchronously.
        //In that case, the SocketAsyncEventArgs.Completed event on the e parameter
        //will not be raised and the e object passed as a parameter may be
        //examined immediately after the method call
        //returns to retrieve the result of the operation.
        // It may be false in the case of a socket error.
        if (!willRaiseEvent)
        {
            //If the op completed synchronously, we need to call ProcessReceive
            //method directly. This will probably be used rarely, as you will
            //see in testing.
            ProcessReceive(receiveSendEventArgs);
        }
    }

        // This method is invoked when an asynchronous receive operation completes. 
        // If the remote host closed the connection, then the socket is closed.  
        // If data was received then the data is echoed back to the client.
        //
        private void ProcessReceive(SocketAsyncEventArgs e)
        {
            SocketAsyncEventArgs receiveSendEventArgs = e;

            // check if the remote host closed the connection
            AsyncUserToken<TData> token = (AsyncUserToken<TData>)e.UserToken;

            if (e.BytesTransferred > 0 && e.SocketError == SocketError.Success)
            {
                //increment the count of the total bytes receive by the server
                Interlocked.Add(ref m_totalBytesRead, e.BytesTransferred);
                lock (thisLock)
                {
                    m_clientBytesRead[token.Socket] = (long)m_clientBytesRead[token.Socket] + e.BytesTransferred;
                }
                //The BytesTransferred property tells us how many bytes
                //we need to process.
                Int32 remainingBytesToProcess = receiveSendEventArgs.BytesTransferred;

                bool bStartReceive = false;

                while (remainingBytesToProcess > 0)
                {
                    /*loop required, because we might have more than one message in the buffer*/

                    //If we have not got all of the prefix already,
                    //then we need to work on it here.
                    if (token.receivedPrefixBytesDoneCount < token.receivePrefixLength)
                    {
                        remainingBytesToProcess = _prefixHandler.HandlePrefix(receiveSendEventArgs,
                                    token, remainingBytesToProcess);

                        if (remainingBytesToProcess == 0)
                        {
                            // We need to do another receive op, since we do not have
                            // the message yet, but remainingBytesToProcess == 0.
                            bStartReceive = true;
                            //Jump out of the loop.
                            break;
                        }
                    }

                    //just that number of bytes belongs to the current message
                    //* all received bytes
                    //* all bytes as long as the message
                    //* just some bytes, because we received the first part(s) of the message with previous SAEA objects
                    int maxBytesToProcess = Math.Min(remainingBytesToProcess, token.lengthOfCurrentIncomingMessage - token.receivedMessageBytesDoneCount);

                    // If we have processed the prefix, we can work on the message now.
                    // We'll arrive here when we have received enough bytes to read
                    // the first byte after the prefix.
                    bool incomingTcpMessageIsReady = _messageHandler
                                .HandleMessage(receiveSendEventArgs,
                                token, maxBytesToProcess);

                    //we processed some bytes, mark them as processed
                    remainingBytesToProcess = remainingBytesToProcess - maxBytesToProcess;

                    if (incomingTcpMessageIsReady == true)
                    {
                        // Pass the DataHolder object to the Mediator here. The data in
                        // this DataHolder can be used for all kinds of things that an
                        // intelligent and creative person like you might think of.
                        //token.theMediator.HandleData(token.theDataHolder);

                        ProcessTCPMessage(token);


                        // Create a new DataHolder for next message.
                        _messageHandler.ResetDataHolder(token);

                        //Reset the variables in the UserToken, to be ready for the
                        //next message that will be received on the socket in this
                        //SAEA object.

                        if (remainingBytesToProcess <= 0)
                        {
                            //reset the full token
                            token.Reset();
                        }
                        else //if (curRemainingBytesToProcess > 0)
                        {
                            //reset the token, but move the MessageOffset so that we keep reading the SAEA-buffer
                            token.Reset(maxBytesToProcess);
                            //token.receiveMessageOffset = token.bufferOffsetReceive + ;
                        }

                        //token.theMediator.PrepareOutgoingData();
                        //StartSend(token.theMediator.GiveBack());
                        bStartReceive = true;
                    }
                    else
                    {
                        // Since we have NOT gotten enough bytes for the whole message,
                        // we need to do another receive op. Reset some variables first.

                        if (remainingBytesToProcess <= 0)
                        {
                            // All of the data that we receive in the next receive op will be
                            // message. None of it will be prefix. So, we need to move the
                            // token.receiveMessageOffset to the beginning of the
                            // receive buffer space for this SAEA.
                            token.receiveMessageOffset = token.bufferOffsetReceive;
                        }
                        else //if (curRemainingBytesToProcess > 0)
                        {
                            token.receiveMessageOffset = token.receiveMessageOffset + maxBytesToProcess;
                        }

                        // Do NOT reset token.receivedPrefixBytesDoneCount here.
                        // Just reset recPrefixBytesDoneThisOp.
                        token.recPrefixBytesDoneThisOp = 0;

                        // Since we have not gotten enough bytes for the whole message,
                        // we need to do another receive op.
                        bStartReceive = true;
                    }
                }//while


                if (bStartReceive)
                {
                    StartReceive(receiveSendEventArgs);
                }

                //Console.WriteLine("The server has read a total of {0} bytes", m_totalBytesRead, );
                //Console.WriteLine("The server has read from {0} {1} bytes", token.Socket.RemoteEndPoint.ToString(), m_clientBytesRead[token.Socket]);

                ////echo the data received back to the client
                //e.SetBuffer(e.Offset, e.BytesTransferred);
                //bool willRaiseEvent = token.Socket.SendAsync(e);
                //if (!willRaiseEvent)
                //{
                //    ProcessSend(e);
                //}

                //StartReceive(e);

                /*moved to StartReceive*/
                //m_bufferManager.FreeBuffer(e);
                //m_bufferManager.SetBuffer(e);
                //bool willRaiseEvent = token.Socket.ReceiveAsync(e);
                //if (!willRaiseEvent)
                //{
                //    ProcessReceive(e);
                //}
            }
            else
            {
                token.Reset();
                CloseClientSocket(e);
            }
        }



        private void ProcessTCPMessage(AsyncUserToken<TData> token)
        {
            TCPMessage<TData> msg = token.theDataHolder;

            bool timeoutOccured = false;
            //Task postMessage;
            const int msgTimeout = 1000; //milliseconds

            Stopwatch sw_postMessage = new Stopwatch();

            /*using Sync ProcessMessage() or Task.WhenAny() should be more performant */
            do
            {
                CancellationTokenSource tokenSource = new CancellationTokenSource();

                if (timeoutOccured)
                {
                    if (listenSocket == null)
                    {
                        token.Socket.Close();
                        return; //we are not connected anymore, do not process the message
                    }

                    Interlocked.Increment(ref this._postMessageToProcessorTimeout);
                    //LogMessage(String.Format("MessageProcessor did not accept the msg within expected time of {0}ms", msgTimeout));
                    LogMessage(String.Format("MessageProcessor did not accept the msg, waiting {0}ms and try again.", msgTimeout));
                    //await Task.Delay(msgTimeout); //wait a moment and then try to add the message again
                    Thread.Sleep(msgTimeout);
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

            token.byteCountTotal += (msg.RawData.Length + AsyncUserToken<TData>.RECEIVE_PREFIX_LENGTH);
            Interlocked.Increment(ref msgReceivedCountTotal);

            token.MsgTroughput.AddDataCount(1);

            
            if (totalMessagesThroughput != null)
            {
                //should always be non null, because should only be set to null after all this methods finished
                totalMessagesThroughput.AddDataCount(1);
            }

            // logging
            if ((token.MsgTroughput.DataCount > 0) && (token.MsgTroughput.DataCount % _statisticPerMessagesCount /*500*/) == 0)
            {
                long byteCountTotalBackup = token.byteCountTotalBackup;
                GenerateStatistic(token.byteCountTotal, ref byteCountTotalBackup, msgReceivedCountTotal, token.GetClientInfo(), token.Stopwatch, token.MsgTroughput, sw_postMessage);
                token.byteCountTotalBackup = byteCountTotalBackup;
            }
            //await writer.WriteLineAsync("from Server: " + dataFromServer);
        }

        // This method is invoked when an asynchronous send operation completes.  
        // The method issues another receive on the socket to read any additional 
        // data sent from the client
        //
        // <param name="e"></param>
        private void ProcessSend(SocketAsyncEventArgs e)
        {
            if (e.SocketError == SocketError.Success)
            {
                //m_bufferManager.FreeBuffer(e);
                m_bufferManager.SetBuffer(e);
                // done echoing data back to the client
                AsyncUserToken<TData> token = (AsyncUserToken<TData>)e.UserToken;
                // read the next block of data send from the client
                bool willRaiseEvent = token.Socket.ReceiveAsync(e);
                if (!willRaiseEvent)
                {
                    ProcessReceive(e);
                }
            }
            else
            {
                CloseClientSocket(e);
            }
        }

        private void CloseClientSocket(SocketAsyncEventArgs e)
        {
            AsyncUserToken<TData> token = e.UserToken as AsyncUserToken<TData>;

            string clientInfo = token.Socket.RemoteEndPoint.ToString();

            // close the socket associated with the client
            try
            {
                token.Socket.Shutdown(SocketShutdown.Send);
            }
            // throws if client process has already closed
            catch (Exception ex) {
                Debug.Assert(false, "CloseClientSocket: " + ex.Message);
            }

            lock (thisLock)
            {
                m_clientBytesRead.Remove(token.Socket);
            }

            token.Socket.Close();

            // decrement the counter keeping track of the total number of clients connected to the server
            Interlocked.Decrement(ref connections);
            m_maxNumberAcceptedClients.Release();
            Console.WriteLine("client {0} disconnected from the server. {1} clients connected to the server", clientInfo, connections);

            token.Stats.ActiveClient = false;

            //GenerateStatistic(byteCountTotal, ref byteCountTotalBackup, msgReceivedCountTotal, clientInfo, stopwatch, msgTroughput, sw_postMessage);

            // Free the SocketAsyncEventArg so they can be reused by another client
            m_readWritePool.Push(e);
        }

    }//AsyncSocketServer
}

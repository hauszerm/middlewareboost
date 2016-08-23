using BoostMiddlewareImporter.Database;
using BoostMiddlewareImporter.Processor;
using BoostMiddlewareImporter.Properties;
using BoostMiddlewareImporter.Server;
using BoostMiddlewareObjects;
using BoostMiddlewareObjects.Helpers;
using Cassandra;
using Cassandra.Mapping;
using Intratec.Common.Helpers.Extensions;
using Intratec.Common.Services.Logging;
using Intratec.Common.Services.Logging.Log4Net;
using MongoDB.Driver;
using Raven.Client.Document;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Threading.Tasks.Schedulers;

namespace BoostMiddlewareImporter
{
    /// <summary>
    /// This Program uses different compile time switches to configure
    ///  * the messages source
    ///  * the datastore target
    ///  
    /// Define the switch "Locally" to let this importer generate the messages by its own. 10.000s of messages
    /// per seconds will be generated.
    /// Do NOT DEFINE "Locally" to start a TCP Server on port <see cref="TCP_SERVER_PORT"/>, that accepts JSON messages and converts that JSON to  an instance of the class <see cref="PlantData"/>.
    /// 
    /// Define one of these switches to use that engine as the datastore for the messages.
    /// MSSQL
    /// RavenDB
    /// Cassandra
    /// MongoDB
    /// 
    /// Startup:
    ///   just start this console application and press <Enter> to start the TCP server.
    ///   Start the TCPClient and enter a number of clients, e.g. 10.
    ///     Then these 10 clients will send each 10ms a message.
    /// 
    /// Program structure:
    ///   the program sets up a graph of TPL dataflow blocks that process the TCPMessages.
    ///     * receive message
    ///     * transform message (to JSON string and .net Object) to db records
    ///     * block db records to batches
    ///     * insert many db records at once
    ///   That is done in the Run() method after the user started the program.
    ///   A <see cref="IMessageGenerator"/> produces the TCPMessage and
    ///   the *MessageImporter for the selected datastore adds the record to the datastore.
    ///   
    /// configuration
    ///   App.config -> appSettings/connectionStrings -> connection to the datastore is configured here
    ///   
    ///   see App.config for a list of configuration parameters that influence the performance of the processing
    ///   PERF -> related to performance
    ///   DUR -> durability/stability of the application
    ///   UI -> only user interface relevant
    ///   
    ///   
    /// logging
    ///   a logfile containing exception details created "BoostImporter.log"
    ///   
    /// TCP client
    ///   the second project "BoostMiddlewareClient" can generate tcp messages that the included tcp server can read.
    /// </summary>
    /// <remarks>
    /// good to know:
    ///   an exception during the excution of an ActionBlock causes the tpl to never ever send messages to that block again.
    /// </remarks>
    class Program
    {
        public const int TCP_SERVER_PORT = 51530;

        /// <summary>
        /// combine the records to this block and pass that block to the db inserter
        /// </summary>
        static int _TableBulkInsertSize = 100; 
        
        static string connectionString = ConfigurationManager.AppSettings["ConnectionString"];
        private static CsvHelper.CsvWriter _csvFile;
        private static StreamWriter _csvTextFileBase;

        private static ILogger _logger = new Log4NetLogger(typeof(Program));

#if RavenDB
        static string ravenDBConnectionStringName = "RavenDB";
#endif

        #region cleanup

        //private static Buffer<RawValueData> _queue = null;


        /*
         * not required anymore, we use async methods
         */
        //public static void ReadLoad(MessageImporter importer, int mode = 1)
        //{
        //    long maxInsertTime = 0;
        //    long minInserTime = long.MaxValue;
        //    long sumInsertTime = 0;
        //    int loop = 0;
        //    RawValueData table = new RawValueData("PropertyData");

        //    Stopwatch sw = Stopwatch.StartNew();
        //    while (true)
        //    {
        //        loop++;
        //        try
        //        {
        //            RawValueData values = new RawValueData("PropertyData");
        //            //values = _queue.Take() as RawValueData;

        //            Stopwatch sw1 = Stopwatch.StartNew();
        //            if (mode == 1)
        //            {
        //                //use connection pooling with wait
        //                Task.Run(async () => await importer.WriteToDatabaseAsync(connectionString, values)).Wait();
        //                //Task.Run(async () => await WriteToServer(connectionString, values));
        //            }
        //            if (mode == 2)
        //            {
        //                // sync writeToServer
        //                importer.WriteToDatabaseSync(connectionString, values);
        //            }
        //            if (mode == 3 || mode == 4)
        //            {
        //                values.AsEnumerable().Take(values.Rows.Count).CopyToDataTable(table, LoadOption.Upsert);
        //                //foreach (DataRow item in values.AsEnumerable())
        //                //{
        //                //    table.Rows.Add(item.ItemArray);                            
        //                //}
        //                if (table.Rows.Count > _TableBulkInsertSize)
        //                {
        //                    if (mode == 3)
        //                    {
        //                        Task.Run(async () => await importer.WriteToDatabaseAsync(connectionString, table)).Wait();
        //                    }
        //                    if (mode == 4)
        //                    {
        //                        importer.WriteToDatabaseSync(connectionString, table);
        //                    }
        //                    table.Clear();
        //                }

        //            }
        //            sw1.Stop();
        //            maxInsertTime = Math.Max(maxInsertTime, sw1.ElapsedMilliseconds);
        //            minInserTime = Math.Min(minInserTime, sw1.ElapsedMilliseconds);
        //            sumInsertTime += sw1.ElapsedMilliseconds;

        //        }
        //        catch (InvalidOperationException)
        //        {
        //            // An InvalidOperationException means that Take() was called on a completed collection
        //            Console.WriteLine("\nqueue is empty");
        //            if (mode == 3)
        //            {
        //                Task.Run(async () => await importer.WriteToDatabaseAsync(connectionString, table)).Wait();
        //            }
        //            if (mode == 4)
        //            {
        //                importer.WriteToDatabaseSync(connectionString, table);
        //            }
        //            break;
        //        }
        //    }
        //    sw.Stop();
        //    Console.WriteLine("ReadLoad = {0}s", ((float)sw.ElapsedMilliseconds / (float)1000).ToString("0.000"));
        //    Console.WriteLine("InsertTime min = {0}ms, avg = {1}ms, max = {2}ms", minInserTime, ((float)sumInsertTime / (float)loop).ToString("0.0"), maxInsertTime);
        //}
        #endregion

        /// <summary>
        /// once started, constantly sends the current statistics to the console output.
        /// </summary>
        /// <typeparam name="TData"></typeparam>
        /// <param name="cancelToken"></param>
        /// <param name="instance">can be null</param>
        /// <param name="importer"></param>
        /// <param name="processor"></param>
        private static async Task DisplayStatistics(CancellationToken cancelToken, IMessageGenerator instance, IStatisticsPrinter importer, MessageProcessor processor, Func<int> dbInsertBlocksWaiting, Func<int> batcherRecordsWaiting)
        {
            //xUpdateStatistics //yxDisplayStatistics

            int maxConnections = 0;
            Process proc = Process.GetCurrentProcess();  //required for memory statistics

            string category = ".NET CLR Memory";
            string counter = "% Time in GC";
            PerformanceCounter gcPerf;

            
            gcPerf = new PerformanceCounter(category, counter, proc.ProcessName /*required for % Time in GC counter*/);

             
            while (!cancelToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(1000);
                    if (instance != null)
                    {
                        maxConnections = Math.Max(maxConnections, instance.Connections);
                    }
                    else
                    {
                        maxConnections = 1;
                    }

                    if (maxConnections > 0)
                    {
                        Console.Clear();
                        Console.WriteLine("---------------V: {0}--------------------", Assembly.GetEntryAssembly().GetName().Version);

                        if (maxConnections > 20)
                        {
                            if (instance != null)
                            {
                                instance.PrintStatisticsAverage();
                            }
                        }
                        else
                        {
                            if (instance != null)
                            {
                                instance.PrintStatistics();
                            }
                        }

                        processor.PrintStatistics();
                        importer.PrintStatistics(new PrintStatisticsArgs()
                        {
                            DbInsertBlocksWaiting = dbInsertBlocksWaiting,
                            BatcherRecordsWaiting = batcherRecordsWaiting,
                        });


                        IEnumerable<CSVColumn> csvLine = Enumerable.Empty<CSVColumn>();

                        LinkedList<CSVColumn> csvLineBase = new LinkedList<CSVColumn>();

                        csvLineBase.AddLast(new CSVColumn("time", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss")));


                        csvLine = csvLine.Union(csvLineBase);
                        csvLine = csvLine.Union(importer.GetStatisticsLine());


                        LinkedList<CSVColumn> progCsvLine = new LinkedList<CSVColumn>();

                        long memoryUsed = GC.GetTotalMemory(false);
                        CSVColumn progMemoryUsed = new CSVColumn("ProgMemoryUsedByes");
                        progMemoryUsed.RowValue = memoryUsed.ToString();

                        Console.WriteLine("----------------------------------------");

                        int maxWorker, minWorker, availWorker;
                        int completionPort;
                        ThreadPool.GetMaxThreads(out maxWorker, out completionPort);
                        ThreadPool.GetMinThreads(out minWorker, out completionPort);
                        ThreadPool.GetAvailableThreads(out availWorker, out completionPort);
                        Console.WriteLine("ThreadPool: min/max {0:00000}/{1:00000} used:{2:00000}", minWorker, maxWorker, maxWorker - availWorker);

                        Console.WriteLine("Memory= Used: {0} Avail:{1} Time in GC:{2:00.00}%", NetworkUtils.ConvertBytesToHumanReadable(memoryUsed), NetworkUtils.ConvertBytesToHumanReadable(proc.PrivateMemorySize64), gcPerf.NextValue());

                        csvLine = csvLine.Union(progCsvLine);

                        Console.WriteLine();
                        importer.PrintMessages();

                        try
                        {
                            WriteCSVStatisticsLine(csvLine);
                        }
                        catch (Exception ex)
                        {
                            Debug.Assert(false, "Write CSV File failed: " + ex.Message);

                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error("Error during DisplayStatistics", ex);
                }
            }//while


            if (_csvFile != null)
            {
                _csvFile.Dispose();
                _csvTextFileBase.Close();
            }
            //proc.Dispose(); //unreachable code
        }

        private static void WriteCSVStatisticsLine(IEnumerable<CSVColumn> line)
        {
            if (_csvFile == null)
            {
                _csvFile = new CsvHelper.CsvWriter(_csvTextFileBase = File.CreateText(String.Format("statistics-{0:yyyy-MM-dd--HH-mm}.csv", DateTime.Now)));
                _csvFile.Configuration.Delimiter = ";";
                foreach (CSVColumn col in line)
                {
                    _csvFile.WriteField(col.ColumnName);
                }
                _csvFile.NextRecord();
            }

            foreach (CSVColumn col in line)
            {
                _csvFile.WriteField(col.RowValue);
            }
            _csvFile.NextRecord();
            _csvTextFileBase.Flush();
        }

        static void Run()
        {
            #region kb
            //Changing ProcessorAffinity has an effect (when only CPU 4 is used, CPU 2 and 3 have less preasure)
            //the app gets slower and reduced CPU 1 preasure does not increase throughput
            //Process.GetCurrentProcess().ProcessorAffinity = new System.IntPtr(15);  //new System.IntPtr(8); //CPU 4 //new System.IntPtr(15); //CPU 2-3-4  //http://stackoverflow.com/questions/13834588/to-set-the-affinity-of-cpus-using-c-sharp
            //ThreadPool.SetMinThreads(1, 1); 
            //ThreadPool.SetMaxThreads(3, 1); //http://stackoverflow.com/questions/11432084/limit-number-of-processors-used-in-threadpool
            #endregion

            #region cleanup
            //Console.WriteLine("Methode wählen");
            //Console.WriteLine("1 - async write to server");
            //Console.WriteLine("2 - sync write to server");
            //Console.WriteLine("3 - optimized blocking, async write to server");
            //Console.WriteLine("4 - optimized blocking, sync write to server");
            #endregion
            //Console.WriteLine("S - starten");
#if TCPServer
            Console.WriteLine("<Enter> - MessageReceiver (TCP Server) starten/stoppen");
#elif CyclicData
            Console.WriteLine("<Enter> - Zyklische Datenerzeugung starten/stoppen");
#elif PeriodData
            Console.WriteLine("<Enter> - Datenerzeugung mit Zukunftsdaten starten/stoppen");
#else
            Console.WriteLine("<Enter> - unbekannter Datengenerierungs-Modus");
#endif

            Console.WriteLine("X - Programm beenden");
            string input = Console.ReadLine();

            _TableBulkInsertSize = Properties.Settings.Default.TableBulkInsertSize;

            int statisticPerMessagesCount = BoostMiddlewareImporter.Properties.Settings.Default.TCPServer_StatisticsPerMessagesCount;

            /*
             * we can define that some tasks should be executed with more priority than other (zero = highest priority)
             * only configures how fast a queued task is started, does not influence how fast a task is processed
             */
            QueuedTaskScheduler qts_high = new QueuedTaskScheduler(Environment.ProcessorCount, threadPriority:ThreadPriority.Highest);
            QueuedTaskScheduler qts_low = new QueuedTaskScheduler(Environment.ProcessorCount, threadPriority: ThreadPriority.Lowest);

            //QueuedTaskScheduler qts_pool = new QueuedTaskScheduler(TaskScheduler.Default, 1); //still executes tasks in parallel
            QueuedTaskScheduler qts_pool = new QueuedTaskScheduler();
            
            //LimitedConcurrencyLevelTaskScheduler qts_limited = new LimitedConcurrencyLevelTaskScheduler(1); //limit the concurrency
            //LimitedConcurrencyLevelTaskScheduler qts_limited = new OrderedTaskScheduler(); //executes only one task at a time, throughput is not increased, we get more "MessageProcessor did not accept message within xxx ms"

            TaskScheduler pri0 = null;
            TaskScheduler pri10 = null;

            if (Settings.Default.ThreadPoolForDBInsert)
            {
                //commented out to test qts_limited
                pri0 = qts_pool.ActivateNewQueue(priority: Settings.Default.ThreadPoolPriority_DBInsert); //DBInsert //Batcher
                //pri0 = qts_limited;
            }
            else
            {
                pri0 = qts_high.ActivateNewQueue(priority: Settings.Default.ThreadPoolPriority_DBInsert); //DBInsert //Batcher
            }
            //new System.Threading.Tasks.Schedulers.IOTaskScheduler(); //does not get better performance
            //new System.Threading.Tasks.Schedulers.ThreadPerTaskScheduler(); //starts a new thread per task, very bad performance

            TaskScheduler pri5 = qts_pool.ActivateNewQueue(priority: Settings.Default.ThreadPoolPriority_Transformation); //all Transformations
            //pri5 = qts_limited; //test qts_limited

            if (Settings.Default.TCPServer_UseThreadPool)
            {
                //commented out to test qts_limited
                pri10 = qts_high.ActivateNewQueue(priority: Settings.Default.TreadPoolPriority_TCPServer); //AsyncServer
                //pri10 = qts_limited;
            }
            else
            {
                pri10 = qts_low.ActivateNewQueue(priority: Settings.Default.TreadPoolPriority_TCPServer); //AsyncServer
            }

#if Locally
            ConcurrentBag<TCPMessage<JSONContainer>> tcpMessageStore = new ConcurrentBag<TCPMessage<JSONContainer>>();
#elif CyclicData || PeriodData
            ConcurrentBag<TCPMessage<Measurand>> tcpMessageStore = new ConcurrentBag<TCPMessage<Measurand>>();
#endif

#if MSSQL
            MessageImporter importer = new MessageImporter();
#endif
#if RavenDB
            RavenMessageImporter importer = new RavenMessageImporter();
#endif
#if MongoDB
            MongoDbMessageImporter importer = new MongoDbMessageImporter();
#endif
#if Cassandra
            CassandraMessageImporter importer = new CassandraMessageImporter();
#endif
#if DocumentDB
            DocumentDBMessageImporter importer = new DocumentDBMessageImporter();

            if (!Environment.GetCommandLineArgs().Contains("/keepdb"))
            {
                Console.WriteLine("DocumentDB collection wird geloescht, falls vorhanden...");

                importer.DropCollectionAsync(
                    ConfigurationManager.AppSettings["documentdb.serviceEndPoint"],
                    ConfigurationManager.AppSettings["documentdb.authKey"],
                    ConfigurationManager.AppSettings["documentdb.database"],
                    ConfigurationManager.AppSettings["documentdb.collectionName"],
                    Microsoft.Azure.Documents.Client.ConnectionMode.Direct, Microsoft.Azure.Documents.Client.Protocol.Tcp
                    ).Wait();

                Console.WriteLine("DocumentDB collection wird geloescht, falls vorhanden...done");
            }

            Console.WriteLine("DocumentDB collection wird initialisiert (collection anlegen, stored procedures festlegen)...");

            importer.Init(
                ConfigurationManager.AppSettings["documentdb.serviceEndPoint"],
                ConfigurationManager.AppSettings["documentdb.authKey"],
                ConfigurationManager.AppSettings["documentdb.database"],
                ConfigurationManager.AppSettings["documentdb.collectionName"],
                Convert.ToBoolean(ConfigurationManager.AppSettings["documentdb.collectionPartitionKey"]),
                ConfigurationManager.AppSettings["documentdb.collectionTier"],
                Microsoft.Azure.Documents.Client.ConnectionMode.Direct, Microsoft.Azure.Documents.Client.Protocol.Tcp,
                Convert.ToBoolean(ConfigurationManager.AppSettings["documentdb.showDebugInfo"]),
                Convert.ToBoolean(ConfigurationManager.AppSettings["documentdb.showHttp429"])
                );

            Console.WriteLine("DocumentDB collection wird initialisiert (collection anlegen, stored procedures festlegen)...done");
#endif

#if MSSQL || RavenDB
            importer.TableBulkBatchSize = Properties.Settings.Default.TableBulkBatchSize;
#endif

            MessageProcessor processor = new MessageProcessor();

#if !(CyclicData || PeriodData)
            ITargetBlock<TCPMessage<JSONContainer>> firstBlock = null; //to this block the MessageGenerator sends the TCPMessages
#else //#if CyclicData || PeriodData
            ITargetBlock<TCPMessage<Measurand>> firstBlock = null; //to this block the MessageGenerator sends the TCPMessages
            ISourceBlock<TCPMessage<Measurand>> preBatcherBlock = null; //from this block the batcher gets the blocks
#endif

            while (input.ToUpper() != "X")
            {

            /*
             * Define the mesh / graph
             * of dataflow
             */

            #region cleanup
            /*
             *  BufferBlock not required, we can send the messages directly to the MsgBatcher-Buffer
             */
            //BufferBlock<TCPMessage<JSONContainer>> queue = new BufferBlock<TCPMessage<JSONContainer>>(new DataflowBlockOptions()
            //{
            //    BoundedCapacity = Properties.Settings.Default.BoundedCapacity, /*
            //                           * only store 500 items in the buffer. if the buffer is full, SendAsyc() will not continue
            //                           * prevents high memory usage of this app (default value is -1, no limit at all)
            //                           */
            //    TaskScheduler = pri0,
            //});
            

            /*
             *  Moved the transformation to the other transformation block, to reduce the overhead of moving messages between blocks
             */
            //TransformBlock<TCPMessage<JSONContainer>, TCPMessage<JSONContainer>> bytesToJSON = new TransformBlock<TCPMessage<JSONContainer>, TCPMessage<JSONContainer>>(
            //    (msg) => ,
            //    new ExecutionDataflowBlockOptions()
            //    {
            //        MaxDegreeOfParallelism = Properties.Settings.Default.MaxDegreeOfParallelism, //DefaultValue = 1
            //        BoundedCapacity = Properties.Settings.Default.BoundedCapacity,
            //    }
            //);
            //queue.LinkTo(bytesToJSON, new DataflowLinkOptions() { PropagateCompletion = true /* execute bytesToJSON.Complete() when queue completes */ });
            #endregion

#if !(CyclicData || PeriodData)
            /*
             * Convert the received bytes to a JSON-string and a PlantData object
             */
            TransformManyBlock<TCPMessage<JSONContainer>, TCPMessage<JSONContainer>> extractDataPoints = new TransformManyBlock<TCPMessage<JSONContainer>, TCPMessage<JSONContainer>>(
                (msg) => processor.ExtractDataPointsFromTCPMessage(msg),
                new ExecutionDataflowBlockOptions()
                {
                    MaxDegreeOfParallelism = Properties.Settings.Default.MaxDegreeOfParallelism, //DefaultValue = 1
                    BoundedCapacity = Properties.Settings.Default.BoundedCapacity, //DefaultValue = 0, no bounding
                    TaskScheduler = pri5,
                    MaxMessagesPerTask = 1000, //Reduce messages processed, that way this task should not get too much CPU, but not seems to have this effect //TODO
                }
            );

            firstBlock = extractDataPoints;
            preBatcherBlock = extractDataPoints;
#else //#if CyclicData || PeriodData
                var bufferBlock = new BufferBlock<TCPMessage<Measurand>>(new DataflowBlockOptions()
                {
                    BoundedCapacity = 100000
                });
                firstBlock = bufferBlock;
                preBatcherBlock = bufferBlock;
#endif

            /*
             * configure database connection
             */

#if!(CyclicData || PeriodData)
            List<BatchBlock<TCPMessage<JSONContainer>>> msgBatcherTasks = new List<BatchBlock<TCPMessage<JSONContainer>>>();
#else
                List<BatchBlock<TCPMessage<Measurand>>> msgBatcherTasks = new List<BatchBlock<TCPMessage<Measurand>>>();
#endif
#if MSSQL
            List<ActionBlock<RawValueData>> writeToDatabaseTasks = new List<ActionBlock<RawValueData>>();
            
            //List<ActionBlock<TCPMessage<JSONContainer>[]>> writeToDatabaseTasks = new List<ActionBlock<TCPMessage<JSONContainer>[]>>(); /*build RawValueData during the writeToDatabase-block*/
#endif
#if RavenDB
            List<DocumentStore> documentStores = new List<DocumentStore>();
            List<ActionBlock<IEnumerable<TCPMessage<JSONContainer>>>> writeToDatabaseTasks = new List<ActionBlock<IEnumerable<TCPMessage<JSONContainer>>>>();
#endif
#if MongoDB
            string mongoDb_connectionString = ConfigurationManager.AppSettings["mongodb.connectionstring"];
            string mongoDb_databaseName = ConfigurationManager.AppSettings["mongodb.database"];

            List<ActionBlock<IList<TCPMessage<JSONContainer>>>> writeToDatabaseTasks = new List<ActionBlock<IList<TCPMessage<JSONContainer>>>>();

#endif
#if Cassandra
            List<ISession> sessionList = new List<ISession>();
            List<ActionBlock<IEnumerable<TCPMessage<JSONContainer>>>> writeToDatabaseTasks = new List<ActionBlock<IEnumerable<TCPMessage<JSONContainer>>>>();


            //register mappings between objects and cassendra rows
            MappingConfiguration.Global.Define<CassandraMappings>(); 

            //define the connection to the cluster
            Cluster cluster = Cluster.Builder()
                //.WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(20000)) //longer connection timeout
                .AddContactPoint(ConfigurationManager.AppSettings["cassandra.contactpoint"]).Build(); 
#endif
#if DocumentDB
            List<ActionBlock<IList<TCPMessage<Measurand>>>> writeToDatabaseTasks = new List<ActionBlock<IList<TCPMessage<Measurand>>>>();
#endif


            //send the received tcp messages to this instance
#if !(CyclicData || PeriodData)
            Server.IMessageProcessor<JSONContainer> receiverOfMessages = new MessageReceiver(firstBlock);
#else
            Server.IMessageProcessor<Measurand> receiverOfMessages = new MeasurandMessageReceiver(firstBlock);
#endif

            if (!Properties.Settings.Default.TCPServer_PostMessageToNextBlock)
            {
                receiverOfMessages = null;
            }

#if Locally
                    //generate the tcp messages locally in a do-forever-loop
                    //tries to recycle tcp-message-objects, but that makes the processing slower
                    IMessageGenerator msgGenerator = new MessageGeneratorLocally(receiverOfMessages, pri10, tcpMessageStore);
#elif AsyncSocket
                    //use a tcp server to receive messages

                    const int DEFAULT_NUM_CONNECTIONS = 12000;
                    //const int DEFAULT_BUFFER_SIZE = Int16.MaxValue * 16;
                    const int DEFAULT_BUFFER_SIZE = 4000;

                    AsyncSocketServer<JSONContainer> asyncSocketServer;
                    IMessageGenerator msgGenerator = asyncSocketServer = new AsyncSocketServer<JSONContainer>(DEFAULT_NUM_CONNECTIONS, DEFAULT_BUFFER_SIZE, TCP_SERVER_PORT, receiverOfMessages, pri10, statisticPerMessagesCount);
                    asyncSocketServer.Init();

#elif CyclicData
                    CyclicMeasurandGenerator cyclicMsgGenerator;
                    IMessageGenerator msgGenerator = cyclicMsgGenerator = new CyclicMeasurandGenerator(receiverOfMessages, pri10, tcpMessageStore);
                    Console.Write("Anzahl simulierter Clients/Anlagen: ");
                    int plantCount = 1;
                    Int32.TryParse(Console.ReadLine().Trim(), out plantCount);
                    cyclicMsgGenerator.SetupTestdata(plantCount, Settings.Default.CyclicDataFile);
#elif PeriodData
                    PeriodMeasurandGenerator periodMsgGenerator;
                    int period = Settings.Default.CyclicPeriodDurationMinutes;
                    //int period = 24 * 60; //1Day
                    //int period = 24 * 60 * 30; //1Month
                    IMessageGenerator msgGenerator = periodMsgGenerator = new PeriodMeasurandGenerator(receiverOfMessages, pri10, tcpMessageStore, period);
                    Console.Write("Anzahl simulierter Clients/Anlagen: ");
                    int plantCount = 1;
                    Int32.TryParse(Console.ReadLine().Trim(), out plantCount);
                    periodMsgGenerator.SetupTestdata(plantCount, Settings.Default.CyclicDataFilePeriod);

                    importer.QueryPeriod = true;

#else
                    //use a tcp server to receive messages
                    IMessageGenerator msgGenerator = new AsyncServer<JSONContainer>(TCP_SERVER_PORT, receiverOfMessages, pri10, statisticPerMessagesCount);
#endif
                    /*
             * Create a BatchBlock and an ImportDatabase-ActionBlock for 6 different TargetTable values
             * that way we can test concurrent db access without creating a new db connection for each task.
             * 
             * MSSQL and MongoDB do not require this feature, they can use MaxDegreeOfParallelism to get concurrent import tasks
             */

#if !(CyclicData || PeriodData)
            int batcherRangeStart = 0;
            int batcherRange = 6 + 1;
#elif CyclicData
            int batcherRangeStart = cyclicMsgGenerator.GetFirstPlantId();
            int batcherRange = cyclicMsgGenerator.GetPlantCount();
#elif PeriodData
            int batcherRangeStart = periodMsgGenerator.GetFirstPlantId();
            int batcherRange = periodMsgGenerator.GetPlantCount();
#endif


#if DocumentDB
            DateTime minDate = DateTime.MaxValue;
            DateTime maxDate = DateTime.MinValue;
            HashSet<Tuple<string,string,string>> processedMeasurands = new HashSet<Tuple<string,string,string>>();

            ActionBlock<IList<TCPMessage<Measurand>>> writeToDatabase = new ActionBlock<IList<TCPMessage<Measurand>>>(
                async (data) =>
                {
                    try
                    {
                        Tuple<string, string, string> measurandRef = null;

                        //track all measurands for all plants that we imported
                        foreach (var item in data)
                        {
                            maxDate = maxDate >= item.Data.Timestamp ? maxDate : item.Data.Timestamp;
                            minDate = minDate <= item.Data.Timestamp ? minDate : item.Data.Timestamp;

                            measurandRef = new Tuple<string, string, string>(item.Data.Plant, item.Data.Equipment, item.Data.Name);
                            //if (!processedMeasurands.Contains(measurandRef)) {
                            processedMeasurands.Add(measurandRef);
                            //}
                        }

                        await importer.StoreInDBAsync(data, processor);
                    

#if CyclicData || PeriodData
                        //NOTE: recycling tcp messages makes the execution of the task slower
                        tcpMessageStore.AddFromEnumerable(data);  //all TCPMessage objects can be reused, no action needs them
#endif
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Exception in writeToDatabaseTask");
                        _logger.Error("Exception in writeToDatabaseTask", ex);
                    }

                },
                new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = Properties.Settings.Default.BoundedCapacity_DbInsert,
                    MaxDegreeOfParallelism = Properties.Settings.Default.MaxDegreeOfParallelismDBInsert, //DefaultValue = 1
                    TaskScheduler = pri0,
                }
                );

                ActionBlock<Tuple<Tuple<string,string,string>,DateTime, int>> calcAverages = new ActionBlock<Tuple<Tuple<string,string,string>,DateTime, int>>(
                async (data) =>
                {
                    
                    //xCalcAverages

                    /* plantname/equipmentname/measurandname */
                    Tuple<string, string, string> measurandRef = data.Item1;

                    try
                    {
                        BoostMiddlewareImporter.Database.DocumentDB.OperationReponseInfo opInfo = new BoostMiddlewareImporter.Database.DocumentDB.OperationReponseInfo();
                        await importer.CalcAndStoreAverageDocumentForPeriodAsync(measurandRef.Item1, measurandRef.Item2, measurandRef.Item3, data.Item2, data.Item3, true, opInfo);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }

                },
                new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = Properties.Settings.Default.BoundedCapacity_DbInsert,
                    MaxDegreeOfParallelism = Properties.Settings.Default.MaxDegreeOfParallelismAvgCalc, //DefaultValue = 1
                    TaskScheduler = pri0,
                }
                );

                Task avgCalcTask = null;

                if (Settings.Default.CalcAvgAfterInsert)
                {
                     avgCalcTask = writeToDatabase.Completion.ContinueWith(async t =>
                     {
                         _logger.Info("do calcing averages...");

                         //when all data is written, calculate the averages for all intervals that we inserted.


                         DateTime curDate = minDate;

                         curDate = new DateTime(curDate.Year, curDate.Month, curDate.Day, curDate.Hour, curDate.Minute, 0);

                         const int INTERVAL = 30;
                         maxDate = maxDate.AddMinutes(INTERVAL); //ensure that the interval with the maxdate is processed too

                         /*
                          *  calc the timeslots between the minDate and maxDate
                          *  send each timeslot to the calcAverages block that performs the AVG-calcing then
                          */

                         do
                         {
                             int restMinutes = curDate.Minute % INTERVAL;
                             curDate = curDate.AddMinutes(INTERVAL - restMinutes); //TODO!!!thk what happens when restMinutes==0 ???
                             foreach (var item in processedMeasurands)
                             {
                                 await calcAverages.SendAsync(new Tuple<Tuple<string, string, string>, DateTime, int>(item, curDate, INTERVAL));
                             }

                             _logger.Info(String.Format("Calc averages for {0} different measurands at {1:dd.MM.yyyy HH:mm}", processedMeasurands.Count, curDate));
                             curDate = curDate.AddMinutes(1);
                         } while (curDate <= maxDate);

                         calcAverages.Complete();
                         _logger.Info("do calcing averages...done");
                     });
                }
#endif

            foreach (int tableId in Enumerable.Range(batcherRangeStart,batcherRange)) {
                int curTableId = tableId;
                string curPlantId = tableId.ToString();

#if DocumentDB
                BatchBlock<TCPMessage<Measurand>> msgBatcher = new BatchBlock<TCPMessage<Measurand>>(_TableBulkInsertSize, new GroupingDataflowBlockOptions()
#else
                BatchBlock<TCPMessage<JSONContainer>> msgBatcher = new BatchBlock<TCPMessage<JSONContainer>>(_TableBulkInsertSize, new GroupingDataflowBlockOptions()
#endif
                {
                    //BoundedCapactiy must be bigger than the blocksize
                    BoundedCapacity = Math.Max(Properties.Settings.Default.BoundedCapacity, _TableBulkInsertSize),
                    TaskScheduler = pri0,
                    //MaxMessagesPerTask = 1000, //Reduce messages processed, that way this task should not get too much CPU, but not seems to have this effect //TODO
                });
#if !(CyclicData || PeriodData)
                preBatcherBlock.LinkTo(msgBatcher, new DataflowLinkOptions() { PropagateCompletion = true }, f => f.Data.PlantData == null || f.Data.PlantData.TargetTable == curTableId);
#else
                if (importer.UsePartitionKey)
                {
                    preBatcherBlock.LinkTo(msgBatcher, new DataflowLinkOptions() { PropagateCompletion = true }, f => f.Data.Plant == curPlantId);
                }
                else
                {
                    preBatcherBlock.LinkTo(msgBatcher, new DataflowLinkOptions() { PropagateCompletion = true});
                }
#endif
                //TODO there must be an external timer that executes msgBatcher.TriggerBatch(), otherwise, the data is stuck in this buffer


                #region cleanup
                //queue.Completion.ContinueWith((t) => msgBatcher.Complete());
                //queue.LinkTo(msgBatcher, new DataflowLinkOptions() { PropagateCompletion = true /* execute msgBatcher.Complete() when queue completes */ });
                #endregion


#if MSSQL
                TransformBlock<TCPMessage<JSONContainer>[], RawValueData> createDataTable = new TransformBlock<TCPMessage<JSONContainer>[], RawValueData>(
                    (msgList) => importer.TransformMessagesToRawValueData(msgList, processor),
                    new ExecutionDataflowBlockOptions()
                    {
                        MaxDegreeOfParallelism = Properties.Settings.Default.MaxDegreeOfParallelism, //DefaultValue = 1
                        BoundedCapacity = Properties.Settings.Default.BoundedCapacity_DbInsert,
                        TaskScheduler = pri5,
                    }
                );
                msgBatcher.LinkTo(createDataTable, new DataflowLinkOptions() { PropagateCompletion = true });


                ActionBlock<RawValueData> writeToDatabase = new ActionBlock<RawValueData>(
                    //ActionBlock<TCPMessage<JSONContainer>[]> writeToDatabase = new ActionBlock<TCPMessage<JSONContainer>[]>(   /*build RawValueData during the writeToDatabase-block*/
                    (data) =>
                        /*build RawValueData during the writeToDatabase-block*/
                        //importer.TransformMessagesToRawValueData(data, processor).ContinueWith(dt =>  
                        //importer.WriteToDatabaseAsync(connectionString, dt.Result)
                        importer.WriteToDatabaseAsync(connectionString, data)
                
                        //)
                        ,
                    new ExecutionDataflowBlockOptions()
                    {
                        BoundedCapacity = Properties.Settings.Default.BoundedCapacity_DbInsert,
                        MaxDegreeOfParallelism = Properties.Settings.Default.MaxDegreeOfParallelismDBInsert, //DefaultValue = 1
                        TaskScheduler = pri0,
                    }
                    );
                createDataTable.LinkTo(writeToDatabase, new DataflowLinkOptions() { PropagateCompletion = true });
                //msgBatcher.LinkTo(writeToDatabase, new DataflowLinkOptions() { PropagateCompletion = true }); /*build RawValueData during the writeToDatabase-block*/
#endif
#if RavenDB
                // ** Arrange.
                DocumentStore documentStore = new DocumentStore
                {
                    ConnectionStringName = ravenDBConnectionStringName,
                };

                documentStore.Initialize(); //can not be called in the TPL Block because this creates Tasks on the same TaskSheduler and waits for the completion (but the tasks will never start)

                documentStore.JsonRequestFactory.ConfigureRequest += (sender, args) =>
                {
                    //enabling UnsafeAuthenticatedConnectionSharing and PreAuthenticate, we need lesser HttpCommunication per Store command
                    var webRequestHandler = new WebRequestHandler();
                    webRequestHandler.UnsafeAuthenticatedConnectionSharing = true;
                    webRequestHandler.PreAuthenticate = true;
                    args.Client = new HttpClient(webRequestHandler);
                };

                documentStores.Add(documentStore);

                ActionBlock<IEnumerable<TCPMessage<JSONContainer>>> writeToDatabase = new ActionBlock<IEnumerable<TCPMessage<JSONContainer>>>(
                    (data) => importer.StoreInDocumentStore(documentStore, data, processor),
                    new ExecutionDataflowBlockOptions()
                    {
                        BoundedCapacity = Properties.Settings.Default.BoundedCapacity_DbInsert,
                        MaxDegreeOfParallelism = Properties.Settings.Default.MaxDegreeOfParallelismDBInsert, //DefaultValue = 1
                        TaskScheduler = pri0,
                    }
                    );
                msgBatcher.LinkTo(writeToDatabase, new DataflowLinkOptions() { PropagateCompletion = true });
#endif

#if MongoDB
                ActionBlock<IList<TCPMessage<JSONContainer>>> writeToDatabase = new ActionBlock<IList<TCPMessage<JSONContainer>>>(
                    (data) => 
                        {
                            //// Get a thread-safe client object by using a connection string
                            MongoClient mongoClient = new MongoClient(mongoDb_connectionString);
                            IMongoDatabase db = mongoClient.GetDatabase(mongoDb_databaseName);

                            IMongoCollection<PropertyData> collection = db.GetCollection<PropertyData>("propertyDatas");
                            importer.StoreInCollection(collection, data, processor);
#if Locally
                            //recycling tcp messages makes the execution of the task slower
                            tcpMessageStore.AddFromEnumerable(data);  //all TCPMessage objects can be reused, no action needs them
#endif
                        }
                        ,
                    new ExecutionDataflowBlockOptions()
                    {
                        BoundedCapacity = Properties.Settings.Default.BoundedCapacity_DbInsert, //DefaultValue = 0
                        MaxDegreeOfParallelism = Properties.Settings.Default.MaxDegreeOfParallelismDBInsert, //DefaultValue = 1
                        TaskScheduler = pri0,
                    }
                    );
                msgBatcher.LinkTo(writeToDatabase, new DataflowLinkOptions() { PropagateCompletion = true });
#endif

#if Cassandra

                
                ISession session = cluster.Connect("demo");

                sessionList.Add(session);

                ActionBlock<IEnumerable<TCPMessage<JSONContainer>>> writeToDatabase = new ActionBlock<IEnumerable<TCPMessage<JSONContainer>>>(
                    (data) => importer.StoreInKeySpace(session, data, processor),
                    new ExecutionDataflowBlockOptions()
                    {
                        BoundedCapacity = Properties.Settings.Default.BoundedCapacity_DbInsert,
                        MaxDegreeOfParallelism = Properties.Settings.Default.MaxDegreeOfParallelismDBInsert, //DefaultValue = 1
                        TaskScheduler = pri0,
                    }
                    );
                msgBatcher.LinkTo(writeToDatabase, new DataflowLinkOptions() { PropagateCompletion = true });

#endif
#if DocumentDB
                msgBatcher.LinkTo(writeToDatabase, new DataflowLinkOptions() { PropagateCompletion = false }); //do not propagate completition, because that would case the first msgBatcher that is finish to cause the writeTodatabase to be finish
#endif

                if (!writeToDatabaseTasks.Contains(writeToDatabase))
                {
                    writeToDatabaseTasks.Add(writeToDatabase);
                }
                msgBatcherTasks.Add(msgBatcher);
            }//foreach Plant

#if DocumentDB
            //when all batcherTasks for all Plants are finished, then finish the writeToDatabaseTask
            Task.WhenAll(msgBatcherTasks.Select(t => t.Completion).ToArray()).ContinueWith((t) => writeToDatabase.Complete());
#endif

            
                //if (input == "1" || input == "2" || input == "3" || input == "4")
                {
                    Stopwatch sw = Stopwatch.StartNew();
                    #region cleanup
                    ////Task t1 = Task.Run(() => GenerateLoad());
                    //Task t2 = null;
                    //switch (input)
                    //{
                    //    case "1":
                    //        t2 = Task.Run(() => ReadLoad(importer, 1));
                    //        break;
                    //    case "2":
                    //        t2 = Task.Run(() => ReadLoad(importer, 2));
                    //        break;
                    //    case "3":
                    //        t2 = Task.Run(() => ReadLoad(importer, 3));
                    //        break;
                    //    case "4":
                    //        t2 = Task.Run(() => ReadLoad(importer, 4));
                    //        break;
                    //}
                    #endregion

                    /*
                     * Start the application
                     */
                    CancellationTokenSource msgGneratorCancelTokenSource = new CancellationTokenSource();

                    Task msgGeneratorTask = msgGenerator.Run(msgGneratorCancelTokenSource.Token);


                    #region cleanup
                    //moved to MessageGeneratorLocally
                    //bool generateLocally = true;

                    //var localTaskFactory = new TaskFactory(
                    //CancellationToken.None, TaskCreationOptions.DenyChildAttach,
                    //TaskContinuationOptions.None, pri10);

                    //Task localTask = localTaskFactory.Create(async () => {
                    //    int targetTable = 0;
                    //    while (generateLocally) {
                    //        TCPMessage<JSONContainer> container;
                    //        if (!tcpMessageStore.TryTake(out container))
                    //        {
                    //            container = new TCPMessage<JSONContainer>();
                    //            container.Data = new JSONContainer();
                    //            container.Data.PlantData = new BoostMiddlewareObjects.PlantData();
                    //        }

                    //        container.Data.PlantData .PlantAddress = "127.0.0.1:4711";
                    //        container.Data.PlantData.TargetTable = (targetTable++ % 5) + 1; //Auf 5 Tables aufteilen

                    //        await processorOfServer.ProcessMessageAsync(container);
                    //    };
                    //});
                    //localTask.Start();
                    #endregion

                    CancellationTokenSource statisticsStopToken = new CancellationTokenSource();
                    //Print statistics in background thread until statisticsStopToken is canceled
                    Task taskDisplayStatistics = DisplayStatistics(statisticsStopToken.Token, msgGenerator, importer, processor, () => writeToDatabaseTasks.Select(x => x.InputCount).Sum(), () => msgBatcherTasks.Select(x => x.OutputCount).Sum());

#if DocumentDB
                    bool queryCalcedAvg = false;

                    try
                    {
                        queryCalcedAvg = Convert.ToBoolean(ConfigurationManager.AppSettings["documentdb.queryCalcedAvg"]);
                    }
                    catch
                    {
                        queryCalcedAvg = false;
                    }

                    Task queringTask = importer.StartQueryingDataSeriesAsync(statisticsStopToken.Token, queryCalcedAvg);
#endif

                    do
                    {
                        input = Console.ReadKey().KeyChar.ToString();
                    } while (input != "\r");

                    try
                    {
                        msgGenerator.Stop(); //signal stop to TCP Server
                        msgGneratorCancelTokenSource.Cancel(); //signal the generator to not generate more data
                        msgGeneratorTask.Wait(); //wait for the TCP Server to stop, we wait for all client tasks to stop
                        firstBlock.Complete(); //signal the message queue to not accept more messages, processes all messages in the buffers
                        Task.WaitAll(writeToDatabaseTasks.Select(t => t.Completion).ToArray()); //wait until all remaining data is sent to the database  //writeToDatabase.Completion.Wait(); 


                        //wait for the avgCalculation Task completed

                        if (avgCalcTask != null)
                        {
                            avgCalcTask.WaitForCompletionStatus();
                            calcAverages.Completion.Wait(); //add thk 09.08.2016 wait for the calcing ActionBlock to complete before ending the program
                        }

#if RavenDB
                    documentStores.ForEach(d => d.Dispose()); //dispose document stores
#endif
#if Cassandra
                    sessionList.ForEach(s => s.Dispose()); //dispose sessions
#endif
                        sw.Stop();
                        statisticsStopToken.Cancel(); //signal to stop printing statistics to the console
                        
                        taskDisplayStatistics.WaitForCompletionStatus(); //wait for the printing statitics task to finish
                        queringTask.WaitForCompletionStatus();
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(String.Format("Exception while stopping process: {0}", ex.ToDebugString()), ex);
                    }
                    Console.WriteLine("totaltime = {0}s", ((float)sw.ElapsedMilliseconds / (float)1000).ToString("0.000"));
                    //Console.WriteLine("endgültig stoppen?");
                    //Console.ReadLine();

                }
                
                Console.WriteLine("<Enter> (start)  - X (exit)");
                input = Console.ReadKey().KeyChar.ToString();

            }//while input != "X"

#if Cassandra
            cluster.Shutdown(); //close cluster connection
#endif
        }//Run

        static void Main(string[] args)
        {
            // Log4Net inizialization
            log4net.Config.XmlConfigurator.Configure();
            
            Run();
        }

    }
}

using BoostMiddlewareImporter.Processor;
using BoostMiddlewareImporter.Server;
using BoostMiddlewareObjects.Helpers;
using Intratec.Common.Services.Logging;
using Intratec.Common.Services.Logging.Log4Net;
using adb = Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using System.IO;
using BoostMiddlewareObjects;
using BoostMiddlewareImporter.Database.DocumentDB;
using System.Collections.ObjectModel;
using Newtonsoft.Json.Linq;

namespace BoostMiddlewareImporter.Database
{
    public class DocumentDBMessageImporter : BoostMiddlewareImporter.Database.IStatisticsPrinter
    {
        ILogger logger = new Log4NetLogger(typeof(DocumentDBMessageImporter));

        #region writeToDatabase
        private long _databaseImportTaskCount;
        private TroughputStopwatch _recordsThroughput = null;
        
        /// <summary>
        /// RUs currently consumed by any operation that is executed against the collection
        /// </summary>
        private TroughputStopwatch2 _rusConsumed = null;

        private long _insertDurationLatest;
        public ConcurrentQueue<long> _insertDurationList = new ConcurrentQueue<long>();

        private TroughputStopwatch2 _rusConsumedCalcAvg = null;
        private long _calcAvgDurationLatest;
        public ConcurrentQueue<long> _calcAvgDurationList = new ConcurrentQueue<long>();

        private MessageCache _msgCache = new MessageCache(Properties.Settings.Default.LoggingMessageCacheSize);

        private LinkedList<CSVColumn> _statisticsLine = new LinkedList<CSVColumn>();

        private double _insertDurationAverage;
        private long _insertDurationMax;
        
        private static DocumentClient _client;
        private static DocumentCollection _measurandsCollection;
        private static string _colSelfLink;
        private static string _bulkLoadSprocSelfLink;
        private static bool _displayHttp429Error;
        private static string _scriptFilePath = "."; //current directory
        private static bool _showDebugInfo;
        private Stopwatch _swSinceHttp429 = new Stopwatch();
        private bool _usePartitionKey;
        private OperationReponseInfo _lastQueryOpInfo;
        private List<Measurand> _lastQueryMeasurandList;
        private string _countSprocSelfLink;
        private string _cubeSprocSelfLink;

        public bool UsePartitionKey
        {
            get
            {
                return _usePartitionKey;
            }
        }

        #endregion


        public bool QueryPeriod { get; set; }
        
        public DocumentDBMessageImporter()
        {
            QueryPeriod = false;
        }

        //public Table<PropertyData> CreateSchema(ISession session)
        //{
        //    Table<PropertyData> table = session.GetTable<PropertyData>();
        //    if (!_importerAlreadyCreatedTable)
        //    {
        //        //only test once per importer instance
        //        table.CreateIfNotExists();
        //        _importerAlreadyCreatedTable = true;
        //    }

        //    return table;
        //}

        #region schema creation

        public async Task DropCollectionAsync(string endpointUrl, string authorizationKey, string databaseId, string collectionId, ConnectionMode conMode, Protocol conProtocol)
        {
            // create DocumentDb client and get database, collection, stored procedure etc
            EnsureClientInstance(endpointUrl, authorizationKey, conMode, conProtocol);

            // get database
            adb.Database database = await GetOrCreateDatabaseAsync(_client, databaseId);

            // get collection
            DocumentCollection measurandsCollection = _client.CreateDocumentCollectionQuery(database.SelfLink).Where(c => c.Id == collectionId).ToArray().FirstOrDefault();

            if (measurandsCollection != null)
            {
                await _client.DeleteDocumentCollectionAsync(measurandsCollection.SelfLink);
            }
        }

        //xInit
        public void Init(string endpointUrl, string authorizationKey, string databaseId, string collectionId, bool collectionPartitionKey, string collectionTier, ConnectionMode conMode, Protocol conProtocol, bool displayHttp429Error, bool showDebugInfo)
        {
            // client initialization
                InitAsync(endpointUrl, authorizationKey, databaseId, collectionId, collectionPartitionKey, collectionTier, conMode, conProtocol, displayHttp429Error, showDebugInfo).Wait();
            //if (_client == null)
            //{

            //    //try
            //    //{
                    
            //    //}
            //    //catch (DocumentClientException de)
            //    //{
            //    //    HandleDocumentClientException(de);
            //    //}
            //    //catch (AggregateException ae)
            //    //{
            //    //    // just a simplified example - AggregateException may be DocumentClientException or DocumentServiceQueryException
            //    //    if (ae.GetBaseException().GetType() == typeof(DocumentClientException))
            //    //    {
            //    //        DocumentClientException baseException = ae.GetBaseException() as DocumentClientException;
            //    //        HandleDocumentClientException(baseException);
            //    //    }
            //    //}
            //    //catch (Exception e)
            //    //{
            //    //    Exception baseException = e.GetBaseException();
            //    //    Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
            //    //}

            //}
        }

        //xInit
        private async Task InitAsync(string endpointUrl, string authorizationKey, string databaseId, string collectionId, bool collectionPartitionKey, string collectionTier, ConnectionMode conMode, Protocol conProtocol, bool displayHttp429Error, bool showDebugInfo)
        {
            _displayHttp429Error = displayHttp429Error;
            _showDebugInfo = showDebugInfo;
            _usePartitionKey = collectionPartitionKey;

            // create DocumentDb client and get database, collection, stored procedure etc
            EnsureClientInstance(endpointUrl, authorizationKey, conMode, conProtocol);

            // get database
            adb.Database database = await GetOrCreateDatabaseAsync(_client, databaseId);

            // get collection
            _measurandsCollection = _client.CreateDocumentCollectionQuery(database.SelfLink).Where(c => c.Id == collectionId).ToArray().FirstOrDefault();
            if (_measurandsCollection == null)
            {
                DocumentCollection docCollection = new DocumentCollection { Id = collectionId };
                docCollection.IndexingPolicy.IndexingMode = IndexingMode.Lazy; //Lazy reduces RUs cost by 20-25%
                
                /*use these two options to deactivate indexing*/
                //docCollection.IndexingPolicy.IndexingMode = IndexingMode.None;
                //docCollection.IndexingPolicy.Automatic = false;

                docCollection.IndexingPolicy.ExcludedPaths.Add(
                    new ExcludedPath
                    {
                        Path = "/*",
                        //Indexes = new Collection<Index> { 
                        //        new (DataType.String) { Precision = -1 }, 
                        //        new RangeIndex(DataType.Number) { Precision = -1 }
                        //    }
                    });

                docCollection.IndexingPolicy.IncludedPaths.Add(new IncludedPath
                {
                    Path = "/" + Measurand.TIMESTAMP_NAME + "/?",
                    Indexes = new Collection<Index> { 
                                new RangeIndex(DataType.String) { Precision = -1 }, 
                                //new RangeIndex(DataType.Number) { Precision = -1 }
                            }
                });

                //Timestamp for the 30-Minutes-Interval Avg Value
                docCollection.IndexingPolicy.IncludedPaths.Add(new IncludedPath
                { 
                    Path = "/" + Measurand.TIMESTAMP_NAME + "30" +  "/?",
                    Indexes = new Collection<Index> { 
                                new RangeIndex(DataType.String) { Precision = -1 }, 
                                //new RangeIndex(DataType.Number) { Precision = -1 }
                            }
                });

                docCollection.IndexingPolicy.IncludedPaths.Add(new IncludedPath
                {
                    Path = "/" + "name" + "/?",
                    Indexes = new Collection<Index> { 
                                new RangeIndex(DataType.String) { Precision = 10 }, 
                                //new RangeIndex(DataType.Number) { Precision = -1 }
                            }
                });

                docCollection.IndexingPolicy.IncludedPaths.Add(new IncludedPath
                {
                    Path = "/" + Measurand.EQUIPMENT_NAME + "/?",
                    Indexes = new Collection<Index> { 
                                new RangeIndex(DataType.String) { Precision = 10 }, 
                                //new RangeIndex(DataType.Number) { Precision = -1 }
                            }
                });

                docCollection.IndexingPolicy.IncludedPaths.Add(new IncludedPath
                {
                    Path = "/" + Measurand.PLANT_NAME + "/?",
                    Indexes = new Collection<Index> { 
                                new RangeIndex(DataType.String) { Precision = 10 }, 
                                //new RangeIndex(DataType.Number) { Precision = -1 }
                            }
                });

                //xPartitionKey
                if (collectionPartitionKey)
                {
                    docCollection.PartitionKey.Paths.Add("/" + Measurand.PLANT_NAME);
                }

                _measurandsCollection = await CreateDocumentCollectionWithRetriesAsync(
                    _client,
                    database,
                    docCollection, collectionTier);
            }

            _colSelfLink = _measurandsCollection.SelfLink;

            // get or create Stored procedure
            StoredProcedure bulkLoadSProc = await GetOrCreateStoredProcedureAsync("BulkImport.js");
            _bulkLoadSprocSelfLink = bulkLoadSProc.SelfLink;

            StoredProcedure countSProc = await GetOrCreateStoredProcedureAsync("Count.js");
            _countSprocSelfLink = bulkLoadSProc.SelfLink;

            StoredProcedure cubeSProc = await GetOrCreateStoredProcedureAsync("Cube.js");
            _cubeSprocSelfLink = cubeSProc.SelfLink;

            StoredProcedure calcAveragesSProc = await GetOrCreateStoredProcedureAsync("CalcAverages.js");
            _calcAveragesSprocSelfLink = calcAveragesSProc.SelfLink;

            UserDefinedFunction avgIntervalDate = await GetOrCreateUserDefinedFunctionAsync("AvgIntervalDate.js");
        }

        private static void EnsureClientInstance(string endpointUrl, string authorizationKey, ConnectionMode conMode, Protocol conProtocol)
        {
            if (_client == null)
            {
                // get a client
                _client = new DocumentClient(new Uri(endpointUrl), authorizationKey,
                new ConnectionPolicy
                {
                    ConnectionMode = conMode, //(ConnectionMode)Enum.Parse(typeof(ConnectionMode), ConfigurationManager.AppSettings["connectionMode"]),
                    ConnectionProtocol = conProtocol //(Protocol)Enum.Parse(typeof(Protocol), ConfigurationManager.AppSettings["protocol"])
                });
            }
        }

        /// <summary>
        /// Create a new database if not exists. If exists, get the database
        /// </summary>
        /// <param name="client">The DocumentDB client instance.</param>
        /// <param name="id">The id of the Database to search for, or create.</param>
        /// <returns>The created Database object</returns>
        public static async Task<adb.Database> GetOrCreateDatabaseAsync(DocumentClient client, string id)
        {
            adb.Database database = client.CreateDatabaseQuery().Where(db => db.Id == id).ToArray().FirstOrDefault();
            if (database == null)
            {
                database = await client.CreateDatabaseAsync(new adb.Database { Id = id });
            }

            return database;
        }

        /// <summary>
        /// Create a DocumentCollection, and retry if throttled.
        /// </summary>
        /// <param name="client">The DocumentDB client instance.</param>
        /// <param name="database">The database to use.</param>
        /// <param name="collectionDefinition">The collection definition to use.</param>
        /// <param name="offerType">The offer type for the collection.</param>
        /// <returns>The created DocumentCollection.</returns>
        private async Task<DocumentCollection> CreateDocumentCollectionWithRetriesAsync(
            DocumentClient client,
            adb.Database database,
            DocumentCollection collectionDefinition,
            string offerValue = "S1")
        {
            string offerType = null;
            int offerThroughputVal;
            int? offerThroughput = null;
            if (Int32.TryParse(offerValue, out offerThroughputVal))
            {
                //throughput value specified, use that value
                offerThroughput = offerThroughputVal;
            }
            else
            {
                //offerType specified "S1,S2,S3", use that value
                offerType = offerValue;
            }

            return (await ExecuteWithRetries(client, () => client.CreateDocumentCollectionAsync(
                         database.SelfLink,
                         collectionDefinition,
                         new RequestOptions
                         {
                             OfferType = offerType,
                             OfferThroughput = offerThroughput,
                         }), _displayHttp429Error)).Item1;
        }



        /// <summary>
        /// Create a new StoredProcedure for performance benchmarking.
        /// </summary>
        /// <param name="client">The DocumentDB client instance.</param>
        /// <param name="collection">The collection to create the StoredProcedure in.</param>
        /// <returns>The created StoredProcedure object</returns>
        private async Task<StoredProcedure> GetOrCreateStoredProcedureAsync(string scriptName)
        {
            //Use script name (minus file extension) for the procedure id
            string procedureId = Path.GetFileNameWithoutExtension(scriptName);

            StoredProcedure storedProcedure = _client.CreateStoredProcedureQuery(_colSelfLink).Where(s => s.Id == procedureId).AsEnumerable().FirstOrDefault();
            if (storedProcedure == null)
            {
                //Stored procedure doesn't exist.  Create it ...
                string scriptFileFullPath = string.Format(@"{0}\{1}", _scriptFilePath, scriptName);
                string contents = File.ReadAllText(scriptFileFullPath, Encoding.UTF8);

                storedProcedure = new StoredProcedure
                {
                    Id = procedureId,
                    Body = contents
                };
                storedProcedure = (await ExecuteWithRetries(
                _client,
                () => _client.CreateStoredProcedureAsync(_colSelfLink, storedProcedure), _displayHttp429Error)).Item1;

            }

            return storedProcedure;
        }



        private async Task<UserDefinedFunction> GetOrCreateUserDefinedFunctionAsync(string scriptName)
        {
            //Use script name (minus file extension) for the procedure id
            string udfId = Path.GetFileNameWithoutExtension(scriptName);

            UserDefinedFunction udf = _client.CreateUserDefinedFunctionQuery(_colSelfLink).Where(s => s.Id == udfId).AsEnumerable().FirstOrDefault();
            if (udf == null)
            {
                //Stored procedure doesn't exist.  Create it ...
                string scriptFileFullPath = string.Format(@"{0}\{1}", _scriptFilePath, scriptName);
                string contents = File.ReadAllText(scriptFileFullPath, Encoding.UTF8);

                udf = new UserDefinedFunction
                {
                    Id = udfId,
                    Body = contents
                };
                udf = (await ExecuteWithRetries(
                _client,
                () => _client.CreateUserDefinedFunctionAsync(_colSelfLink, udf), _displayHttp429Error)).Item1;

            }

            return udf;
        }


#endregion

        /// <summary>
        /// Execute the function with retries on throttle
        /// </summary>
        /// <typeparam name="V"></typeparam>
        /// <param name="client"></param>
        /// <param name="function"></param>
        /// <param name="showDebugInfo"></param>
        /// <param name="threadNumber"></param>
        /// <param name="iterationNumber"></param>
        /// <returns></returns>
        private async Task<Tuple<V, TimeSpan, int>> ExecuteWithRetries<V>(DocumentClient client, Func<Task<V>> function, bool showHttp429Error)
        {
            TimeSpan sleepTime = TimeSpan.Zero;

            Stopwatch stopwatch = new Stopwatch();

            int iterationnumber = 0;

            V funcResult;

            while (true)
            {
                iterationnumber++; //count number of retries
                try
                {
                    
                    stopwatch.Restart();
                    try
                    {
                        funcResult = await function();
                    }
                    finally
                    {
                        stopwatch.Stop();
                    }
                    return new Tuple<V, TimeSpan, int>(funcResult, stopwatch.Elapsed, iterationnumber);
                }
                catch (DocumentClientException de)
                {
                    if ((int)de.StatusCode != 429)
                    {
                        throw;
                    }
                    if (true == showHttp429Error)
                    {
                        _msgCache.AddMessage(@"{0}\t{1} resulted a HTTP 429 after {2:hh\:mm\:ss\.fff}", DateTime.UtcNow, function.Method.Name, stopwatch.Elapsed);
                        _swSinceHttp429.Restart();
                    }
                    sleepTime = de.RetryAfter;
                }
                catch (AggregateException ae)
                {
                    if (!(ae.InnerException is DocumentClientException))
                    {
                        throw;
                    }

                    DocumentClientException de = (DocumentClientException)ae.InnerException;
                    if ((int)de.StatusCode != 429)
                    {
                        throw;
                    }
                    if (true == showHttp429Error)
                    {
                        _msgCache.AddMessage("{0}\t{1} resulted a HTTP 429", DateTime.UtcNow, function.Method.Name);
                        _swSinceHttp429.Restart();
                    }
                    sleepTime = de.RetryAfter;
                }

                await Task.Delay(sleepTime);
            }
        }


        private void HandleDocumentClientException(DocumentClientException de)
        {
            // We can take custom actions (like Retry etc) based on the information obtained from DocumentClientException object
            _msgCache.AddMessage("\nActivityID: {0}", de.ActivityId);
            _msgCache.AddMessage("\nError: {0}", de.Error);
            _msgCache.AddMessage("\nHTTP Status Code: {0}", de.StatusCode.ToString());
            _msgCache.AddMessage("\nRetryAfter: {0}", de.RetryAfter.TotalSeconds.ToString());
            _msgCache.AddMessage("\nStack Trace: {0}", de.StackTrace);
            _msgCache.AddMessage("\nResponse Headers:\n ");
            if (de.ResponseHeaders != null && de.ResponseHeaders.Count > 0)
            {
                foreach (string key in de.ResponseHeaders)
                {
                    _msgCache.AddMessage("{0} {1}", key, de.ResponseHeaders[key]);
                }
            }
            Exception baseException = de.GetBaseException();
            _msgCache.AddMessage("Message: {0} \n {1}", de.Message, baseException.Message);

        }


        public async Task<int> StoreInDBAsync(IEnumerable<Server.TCPMessage<Measurand>> msgList, MessageProcessor processor)
        {
            int result = 0;

            Interlocked.Increment(ref _databaseImportTaskCount);

            if (_recordsThroughput == null)
            {
                _recordsThroughput = new TroughputStopwatch();
                _recordsThroughput.Start();
                
            }

            if (_rusConsumed == null)
            {
                _rusConsumed = new TroughputStopwatch2();
                _rusConsumed.Start();

            }

            Stopwatch sw = Stopwatch.StartNew();
            
            try
            {
                List<Measurand> measurandList = msgList.Select(x => x.Data).ToList();
                OperationReponseInfo opInfo = new OperationReponseInfo();
                int count = await BulkInsertDocumentsAsync(measurandList, _showDebugInfo, opInfo);
                sw.Stop();
                this._recordsThroughput.AddDataCount(count);

                this._rusConsumed.AddDataCount(opInfo.OpRequestCharge);

                ShowProgress(count, 0 , 0, sw.ElapsedMilliseconds);

                result = count;
            }
            catch (DocumentClientException de)
            {
                HandleDocumentClientException(de);
            }
            catch (AggregateException ae)
            {
                if (ae.GetBaseException().GetType() == typeof(DocumentClientException))
                {
                    DocumentClientException baseException = ae.GetBaseException() as DocumentClientException;
                    HandleDocumentClientException(baseException);
                }
                else
                {
                    Exception baseException = ae.GetBaseException();
                    _msgCache.AddMessage("ERROR BulkInsert (AggregateException): {0} \n {1}", ae.Message, baseException.Message);
                }
            }
            catch (Exception ex)
            {
                logger.Error("ERROR: BulkInsert", ex);
                _msgCache.AddMessage("ERROR: BulkInsert {0}", ex.Message);
            }

            Interlocked.Decrement(ref _databaseImportTaskCount);

            return result;
        }


        private void BulkInsertDocuments(List<Measurand> Students, bool showDebugInfo, OperationReponseInfo opsRespInfo /*, int threadNumber, int iterationNumber*/)
        {
            try
            {
                BulkInsertDocumentsAsync(Students, showDebugInfo, opsRespInfo /*, threadNumber, iterationNumber*/).Wait();
            }
            catch (DocumentClientException de)
            {
                HandleDocumentClientException(de);
            }
            catch (AggregateException ae)
            {
                if (ae.GetBaseException().GetType() == typeof(DocumentClientException))
                {
                    DocumentClientException baseException = ae.GetBaseException() as DocumentClientException;
                    HandleDocumentClientException(baseException);
                }
            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                _msgCache.AddMessage("Error: {0}, Message: {1}", e.Message, baseException.Message);
            }
        }

        private async Task<int> BulkInsertDocumentsAsync(List<Measurand> measurands, bool showDebugInfo, OperationReponseInfo opsRespInfo /*, int threadNumber, int iterationNumber */)
        {
            // Keep track of the number of documents loaded 
            int documentsLoaded = 0;

            // Save original batch size in case complete batch doesn't get loaded
            int batchSize = measurands.Count;
            do
            {
                RequestOptions requestOptions = new RequestOptions();
                if (_usePartitionKey && measurands.Any()) //prevent null pointer exception
                {
                    requestOptions.PartitionKey = new PartitionKey(measurands.FirstOrDefault().Plant);
                }
                Stopwatch clock = new Stopwatch();
                clock.Start();
                StoredProcedureResponse<int> scriptResult = (await ExecuteWithRetries(_client, () => _client.ExecuteStoredProcedureAsync<int>(_bulkLoadSprocSelfLink, requestOptions, measurands), _displayHttp429Error)).Item1;
                clock.Stop();

                int countOfLoadedDocs = scriptResult.Response;
                double lastRequestCharge = scriptResult.RequestCharge;

                if (opsRespInfo != null)
                {
                    opsRespInfo.ElapsedMilliseconds = clock.ElapsedMilliseconds;
                    opsRespInfo.OpRequestCharge = lastRequestCharge;
                }

                //Keep track of the number of documents we've loaded so far
                documentsLoaded += countOfLoadedDocs;

                //Check to see if the entire batch was loaded. 
                if (countOfLoadedDocs < measurands.Count)
                {
                    //Remove elements from the Measurands List<Measurand> that have already been loaded before we re-send the documents that weren't processed ...
                    measurands.RemoveRange(0, countOfLoadedDocs);
                }

                //for debug
                if (true == showDebugInfo)
                {
                    //Console.WriteLine("{0}\tBulk Insert of {1} documents by Thread:{2} and Iteration:{3}, # of RUs: {4}", DateTime.UtcNow, countOfLoadedDocs, threadNumber, iterationNumber, lastRequestCharge);
                    _msgCache.AddMessage("{0}\tBulk Insert of {1} documents, # of RUs: {2}", DateTime.UtcNow, countOfLoadedDocs, lastRequestCharge);
                }

            } while (documentsLoaded < batchSize);

            //Return count of documents loaded
            return documentsLoaded;

        }

        private async Task<FeedResponse<Measurand>> QuerySingleDocumentAsync(IDocumentQuery<Measurand> query)
        {
            return await query.ExecuteNextAsync<Measurand>();
        }

        private DateTime startDate = DateTime.UtcNow;
        private string _calcAveragesSprocSelfLink;

        private async Task<List<Measurand>> ReadMultiDocumentAsync(string plantName, string equipmentName, string measurandName, bool showDebugInfo, OperationReponseInfo opsRespInfo,int batchSize)
        {
            //suchwort: xMultiDoc

            DateTime untilDate;
            DateTime fromDate;
            CalcFromAndUntilDate(out untilDate, out fromDate);

            // Fetch records per batchSize 
            FeedOptions options = new FeedOptions { MaxItemCount = batchSize };
            //options.EnableCrossPartitionQuery = true; //when a multi partition collection is set and no partition key is defined or the partition key property is not part of the select statement
            options.PartitionKey = new PartitionKey(plantName); //partition key not required, the property is specified in the where condition

            string queryString = GetMeasurandTimeSeriesQuery(plantName, equipmentName, measurandName, untilDate, fromDate);
            //IDocumentQuery<dynamic> query = _client.CreateDocumentQuery(_colSelfLink, queryString, options).AsDocumentQuery();

            IDocumentQuery<Measurand> query = _client.CreateDocumentQuery<Measurand>(_colSelfLink, queryString, options).AsDocumentQuery();

            FeedResponse<Measurand> queryResponse = null;

            List<Measurand> measurandsRetrieved = new List<Measurand>();

            Stopwatch clock = new Stopwatch();
            var iterationNumber = 0;
            while ( true == query.HasMoreResults)
            {
                iterationNumber++;
                // start timer and measure time for running a query, including retries
                clock.Restart();
                queryResponse = (await ExecuteWithRetries(_client, () => QuerySingleDocumentAsync(query), _displayHttp429Error)).Item1;
                clock.Stop();

                // for debug
                if (true == showDebugInfo)
                {
                    _msgCache.AddMessage("{0}\t {1} Documents Read in {2} ms (iteration {4}), # of RUs: {3}", DateTime.UtcNow, queryResponse.Count,clock.ElapsedMilliseconds, queryResponse.RequestCharge, iterationNumber);
                    //_msgCache.AddMessage("Values: {0}", String.Join(",", queryResponse.Select(m => m.Value)));
                }

                // populate OperationReponseInfo counters that will use later for Summary
                if (opsRespInfo != null)
                {
                    opsRespInfo.ElapsedMilliseconds += clock.ElapsedMilliseconds;
                    opsRespInfo.OpRequestCharge += queryResponse.RequestCharge;
                    opsRespInfo.NumberOfDocumentsRead += queryResponse.Count;
                }

                //add to retrieved Measurands List
                measurandsRetrieved.AddRange(queryResponse.AsEnumerable());
            }
            

            return measurandsRetrieved;
        }

        private void CalcFromAndUntilDate(out DateTime untilDate, out DateTime fromDate)
        {
            untilDate = DateTime.Now;
            fromDate = untilDate.AddMinutes(-60);

            if (QueryPeriod)
            {
                fromDate = startDate;
                untilDate = fromDate.AddMinutes(60 * 24 * 30);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="plantName"></param>
        /// <param name="equipmentName"></param>
        /// <param name="measurandName"></param>
        /// <param name="untilDate"></param>
        /// <param name="fromDate"></param>
        /// <param name="interval">minutes</param>
        /// <returns></returns>
        private static string GetMeasurandTimeSeriesQuery(string plantName, string equipmentName, string measurandName, DateTime untilDate, DateTime fromDate, int? interval=null)
        {
            string fields = "s.v, s.cts";

            if (interval.HasValue)
            {
                fields += ", udf.AvgIntervalDate(s.cts, " + interval + ") intervaldate"; //minutes
            }

            string queryString = "SELECT "+ fields +" FROM Measurands s WHERE s.Name = '" + measurandName + "' and s.p = '" + plantName + "' and s.e = '" + equipmentName + "' and s.cts >= '" + fromDate.ToString("o") + "' and s.cts <= '" + untilDate.ToString("o") + "' order by s.cts";
            return queryString;
        }


        public async Task<int> CalcAndStoreAverageDocumentForPeriodAsync(string plantName, string equipmentName, string measurandName, DateTime untilDate, int intervalMinutes, bool showDebugInfo, OperationReponseInfo opsRespInfo)
        {
            //xCalcAndStoreAverageDocumentForPeriodAsync

             if (_rusConsumedCalcAvg == null)
            {
                _rusConsumedCalcAvg = new TroughputStopwatch2();
                _rusConsumedCalcAvg.Start();
            }

            Stopwatch sw = Stopwatch.StartNew();
         
            RequestOptions requestOptions = new RequestOptions();
            if (_usePartitionKey) //prevent null pointer exception
            {
                requestOptions.PartitionKey = new PartitionKey(plantName);
            }

            string measurandRefString = @"{
                 plant: '" + plantName + @"' 
                ,equipment: '" + equipmentName+ @"'
                ,name: '" + measurandName + @"'
                }";
            Object measurandRef = Newtonsoft.Json.JsonConvert.DeserializeObject<Object>(measurandRefString);

            Stopwatch clock = new Stopwatch();
            clock.Start();
            Tuple<StoredProcedureResponse<JObject>, TimeSpan, int> executeResult;
            
            executeResult =  (await ExecuteWithRetries(_client, () => _client.ExecuteStoredProcedureAsync<JObject>(_calcAveragesSprocSelfLink, requestOptions, measurandRef, untilDate, intervalMinutes, Measurand.VALUE_NAME), _displayHttp429Error));
            StoredProcedureResponse<JObject> scriptResult = executeResult.Item1;
            TimeSpan latestExecutionTime = executeResult.Item2;
            int executionNumber = executeResult.Item3;
            clock.Stop();

            double lastRequestCharge = scriptResult.RequestCharge;
            int countedDocuments = scriptResult.Response["count"].Value<int>();

            if (opsRespInfo != null)
            {
                opsRespInfo.ElapsedMilliseconds = clock.ElapsedMilliseconds;
                opsRespInfo.OpRequestCharge = lastRequestCharge;

                if (this._rusConsumed != null)
                {
                    this._rusConsumed.AddDataCount(opsRespInfo.OpRequestCharge);
                }

                _rusConsumedCalcAvg.AddDataCount(lastRequestCharge);

                _calcAvgDurationLatest = opsRespInfo.ElapsedMilliseconds;
                _calcAvgDurationList.Enqueue(opsRespInfo.ElapsedMilliseconds);
                if (_calcAvgDurationList.Count > 20)
                {
                    //we are only interested in the last 20 inserts
                    long value;
                    _calcAvgDurationList.TryDequeue(out value);
                }
            }

            //for debug
            if (true == showDebugInfo)
            {
                //Console.WriteLine("{0}\tBulk Insert of {1} documents by Thread:{2} and Iteration:{3}, # of RUs: {4}", DateTime.UtcNow, countOfLoadedDocs, threadNumber, iterationNumber, lastRequestCharge);
                string msg = String.Format(@"{0}\tCalced and stored average of {1} documents, # of RUs: {2} execution time '{3:hh\:mm\:ss\.fff}', '{4:hh\:mm\:ss\.fff}(last retry) tryCount:{5}", DateTime.UtcNow, countedDocuments, lastRequestCharge, clock.Elapsed, latestExecutionTime, executionNumber);
                logger.Info(msg);
                _msgCache.AddMessage(msg);
                
            }


            //Return count of documents summed up
            return countedDocuments;

        }


        /// <summary>
        /// Query the averaged values of the stored values.
        /// Average is calced by DocumentDB stored procedure using https://github.com/lmaccherone/documentdb-lumenize
        /// </summary>
        /// <param name="plantName"></param>
        /// <param name="equipmentName"></param>
        /// <param name="measurandName"></param>
        /// <param name="showDebugInfo"></param>
        /// <param name="opsRespInfo"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        private async Task<List<Measurand>> ReadAverageMeasurandAsync(string plantName, string equipmentName, string measurandName, bool showDebugInfo, OperationReponseInfo opsRespInfo,int batchSize)
        {
            DateTime untilDate;
            DateTime fromDate;
            CalcFromAndUntilDate(out untilDate, out fromDate);

         
            RequestOptions requestOptions = new RequestOptions();
            if (_usePartitionKey) //prevent null pointer exception
            {
                requestOptions.PartitionKey = new PartitionKey(plantName);
            }
            Stopwatch clock = new Stopwatch();
            clock.Start();

            string filterQuery = GetMeasurandTimeSeriesQuery(plantName, equipmentName, measurandName, untilDate, fromDate, 30 /*minutes*/); //query to fetch the time series for a measurand, add the AvgIntervalDate() UDF 
            
            filterQuery = filterQuery.Replace(@"'", @"\u0027"); //single quotes can not be part the value of a JSON object key, so replace it with the HTML Unicode repesentation of the single quote
            
            string configString = @"{
                    cubeConfig: {
                        groupBy: 'intervaldate', 
                        field: '" + Measurand.VALUE_NAME + @"', 
                        f: 'average'
                    }, 
                    filterQuery: '" + filterQuery + @"'
                }";
            Object config = Newtonsoft.Json.JsonConvert.DeserializeObject<Object>(configString);

            StoredProcedureResponse<JObject> scriptResult = null;
            JObject newConfig = null;

            int iterationNumber = 1;

            do
            {
                scriptResult = (await ExecuteWithRetries(_client, () => _client.ExecuteStoredProcedureAsync<JObject>(_cubeSprocSelfLink, requestOptions, config), _displayHttp429Error)).Item1;
                clock.Stop();

                //cellsAsCSVStyleArray contain the averaged values
                int numberOfAggregates = ((JArray)scriptResult.Response["savedCube"]["cellsAsCSVStyleArray"]).Count - 1;

                /*
                 * TODO merge the 1-n items in the cellsAsCSVStyleArray (0 item contains the column-names)
                 * with measurand records, so that the averaged values will be returned by the method
                 */

                // for debug
                if (true == showDebugInfo)
                {
                    _msgCache.AddMessage("{0}\t {1} averaged documents read in {2} ms (iteration: {4}), # of RUs: {3}", DateTime.UtcNow, numberOfAggregates, clock.ElapsedMilliseconds, scriptResult.RequestCharge, iterationNumber);
                }

                // populate OperationReponseInfo counters that will use later for Summary
                if (opsRespInfo != null)
                {
                    opsRespInfo.ElapsedMilliseconds += clock.ElapsedMilliseconds;
                    opsRespInfo.OpRequestCharge += scriptResult.RequestCharge;
                    opsRespInfo.NumberOfDocumentsRead += numberOfAggregates;
                }

                /*
                 * stillQueuing == false when the stored procedure is not allowed to process more records, but more records should be processed
                 */
                if (scriptResult.Response["continuation"] != null && (scriptResult.Response["stillQueueing"].Value<bool>() == false))
                {
                    /*continuation token available, do request again*/
                    newConfig = new JObject(); //when executing with the same configuration + the continuation-token, more averaged records will be returned
                    newConfig.Add("cubeConfig", scriptResult.Response["cubeConfig"]);
                    newConfig.Add("filterQuery", scriptResult.Response["filterQuery"]);
                    newConfig.Add("continuation", scriptResult.Response["continuation"]);

                    iterationNumber++;
                }
                else
                {
                    newConfig = null;
                }

                config = newConfig;
            }
            while (newConfig != null);

            return new List<Measurand>();
        }

        private void ShowProgress(int rowCount, long connectionDuration, long fillDuration, long insertDuration)
        {
            _insertDurationLatest = insertDuration;
            _insertDurationList.Enqueue(insertDuration);
            if (_insertDurationList.Count > 20)
            {
                //we are only interested in the last 20 inserts
                long value;
                _insertDurationList.TryDequeue(out value);
            }

            List<long> currentDurationList = _insertDurationList.ToList(); //make a copy so that it does not change during average/max calc
            _insertDurationAverage = currentDurationList.Average();
            _insertDurationMax = currentDurationList.Max();

            if (connectionDuration >= 2)
            {
                Console.WriteLine("connection={0}ms bulkinsert={1}ms rows={2}", connectionDuration, insertDuration, rowCount);
            }
            else
            {
                Console.Write(".");
            }

            if (fillDuration > 0)
            {
                Console.WriteLine("Fillduration: {0}ms", fillDuration);
            }
        }

        

        public void PrintStatistics(PrintStatisticsArgs args)
        {
            _statisticsLine.Clear();

            CSVColumn documentsTotal = new CSVColumn("DocumentsTotal");
            CSVColumn documentsThroughput = new CSVColumn("DocumentsThroughput");
            CSVColumn documentsThroughputAvg= new CSVColumn("DocumentsThroughputAvg20");
            CSVColumn documentsInsertDurationAvg= new CSVColumn("DocumentsInsertDurationAvg");
            CSVColumn documentsInsertDurationMax= new CSVColumn("DocumentsInsertDurationMax20");
            CSVColumn documentsInsertDurationLatest = new CSVColumn("DocumentsInsertDurationLatest");

            _statisticsLine.AddLast(documentsTotal);
            _statisticsLine.AddLast(documentsThroughput);
            _statisticsLine.AddLast(documentsThroughputAvg);
            _statisticsLine.AddLast(documentsInsertDurationAvg);
            _statisticsLine.AddLast(documentsInsertDurationMax);
            _statisticsLine.AddLast(documentsInsertDurationLatest);

            if (_recordsThroughput != null)
            {
                documentsTotal.RowValue = _recordsThroughput.DataCount.ToString("000000000");
                documentsThroughput.RowValue = _recordsThroughput.GetCurrentThroughput().ToString("0000.00");
                documentsThroughputAvg.RowValue = _recordsThroughput.GetAverageThroughput().ToString("00000.00");

                documentsInsertDurationAvg.RowValue = _insertDurationAverage.ToString("000.0");
                documentsInsertDurationMax.RowValue = _insertDurationMax.ToString("000");
                documentsInsertDurationLatest.RowValue = _insertDurationLatest.ToString("000");

                Console.WriteLine("Documents= Total:{0} Throughput:{1}/s AvgThroughput:{2}/s   " + Environment.NewLine + 
                                  "InsertDuration= Avg:{3}ms Max(20):{4}ms Latest:{5:000}",
                                  documentsTotal.RowValue, documentsThroughput.RowValue, documentsThroughputAvg.RowValue,
                                  documentsInsertDurationAvg.RowValue, documentsInsertDurationMax.RowValue, documentsInsertDurationLatest.RowValue);
            }

            CSVColumn rusTotal = new CSVColumn("RUsTotal");
            CSVColumn rusConsumed = new CSVColumn("RUsConsumed");
            CSVColumn rusConsumedAvg = new CSVColumn("RUsAvgConsumed");
            CSVColumn timeSincehttp429 = new CSVColumn("TimeSincehttp429");
            

            _statisticsLine.AddLast(rusTotal);
            _statisticsLine.AddLast(rusConsumed);
            _statisticsLine.AddLast(rusConsumedAvg);
            _statisticsLine.AddLast(timeSincehttp429);
            
            if (_rusConsumed != null)
            {
                rusTotal.RowValue = _rusConsumed.DataCount.ToString("000000000");
                rusConsumed.RowValue = _rusConsumed.GetCurrentThroughput().ToString("0000.00");
                rusConsumedAvg.RowValue = _rusConsumed.GetAverageThroughput().ToString("00000.00");

                Console.WriteLine("RUs= Total:{0} Consumed:{1}/s AvgConsumed:{2}/s",
                                  rusTotal.RowValue, rusConsumed.RowValue, rusConsumedAvg.RowValue
                                  );
            }

            

            timeSincehttp429.RowValue = _swSinceHttp429.IsRunning ? _swSinceHttp429.Elapsed.ToString(@"hh\:mm\:ss") : "";
            Console.WriteLine("Infos= TimeSinceHTTP429:{0}", timeSincehttp429.RowValue);

            CSVColumn procDBImportTasksCount = new CSVColumn("ProcDBImportTasksCount");
            _statisticsLine.AddLast(procDBImportTasksCount);
            
            CSVColumn procBatcherWaitingCount = new CSVColumn("ProcBatcherWaitingCount");
            _statisticsLine.AddLast(procBatcherWaitingCount);

            CSVColumn procDBImportWaitingCount = new CSVColumn("ProcDBImportWaitingCount");
            _statisticsLine.AddLast(procDBImportWaitingCount);
            
            procDBImportTasksCount.RowValue = _databaseImportTaskCount.ToString("0");
            if (args.BatcherRecordsWaiting != null) {
                procBatcherWaitingCount.RowValue = args.BatcherRecordsWaiting.Invoke().ToString("0000");
            }

            if (args.DbInsertBlocksWaiting != null) {
                procDBImportWaitingCount.RowValue = args.DbInsertBlocksWaiting.Invoke().ToString("0000");
            }

            Console.WriteLine("Batcher Records Waiting: {0}", procBatcherWaitingCount.RowValue);
            Console.WriteLine("ImportTasks running:{0} blocksWaiting:{1}", procDBImportTasksCount.RowValue, procDBImportWaitingCount.RowValue);



            CSVColumn queryRUsLatest = new CSVColumn("QueryRUsLatest");
            CSVColumn queryDurationLatest = new CSVColumn("QueryDurationLatest");
            CSVColumn queryItemsCount = new CSVColumn("QueryItemsCount");

            _statisticsLine.AddLast(queryRUsLatest);
            _statisticsLine.AddLast(queryDurationLatest);
            _statisticsLine.AddLast(queryItemsCount);

            List<Measurand> measurandList = _lastQueryMeasurandList;
            OperationReponseInfo opInfo = _lastQueryOpInfo;
            if (opInfo != null)
            {
                queryRUsLatest.RowValue = opInfo.OpRequestCharge.ToString();
                queryDurationLatest.RowValue = opInfo.ElapsedMilliseconds.ToString();
                queryItemsCount.RowValue = opInfo.NumberOfDocumentsRead.ToString();

                Console.WriteLine("LastQuery= RUs{0} duration={1}ms itemsFetched={2}", queryRUsLatest.RowValue, queryDurationLatest.RowValue, queryItemsCount.RowValue);
            }


            CSVColumn calcAvgRUsConsumed = new CSVColumn("CalcAVG_RUsConsumed");
            CSVColumn calcAvgRUsConsumedAvg = new CSVColumn("CalcAVG_RUsAvg");
            CSVColumn calcAvgExecuteDurationAvg = new CSVColumn("CalcAVG_ExecuteDurationAvg");
            CSVColumn calcAvgExecuteDurationMax = new CSVColumn("CalcAVG_ExecuteDurationMax");
            CSVColumn calcAvgExecuteDurationLatest = new CSVColumn("CalcAVG_ExecuteDurationLatest");


            _statisticsLine.AddLast(calcAvgRUsConsumed);
            _statisticsLine.AddLast(calcAvgRUsConsumedAvg);
            _statisticsLine.AddLast(calcAvgExecuteDurationAvg);
            _statisticsLine.AddLast(calcAvgExecuteDurationLatest);

            if (_rusConsumedCalcAvg != null)
            {
                calcAvgRUsConsumed.RowValue = _rusConsumedCalcAvg.GetCurrentThroughput().ToString("0000.00");
                calcAvgRUsConsumedAvg.RowValue = _rusConsumedCalcAvg.GetAverageThroughput().ToString("00000.00");

                calcAvgExecuteDurationAvg.RowValue = _calcAvgDurationList.Average().ToString("000.0");
                calcAvgExecuteDurationMax.RowValue = _calcAvgDurationList.Max().ToString("000.0");
                calcAvgExecuteDurationLatest.RowValue = _calcAvgDurationLatest.ToString("000");

                Console.WriteLine("CalcAvg -- RUs:{1}/s AvgRUs:{2}/s   " + Environment.NewLine +
                                  "        CalcDuration= Avg:{3}ms Max(20):{4}ms Latest:{5:000}",
                                0, calcAvgRUsConsumed.RowValue, calcAvgRUsConsumedAvg.RowValue,
                                calcAvgExecuteDurationAvg.RowValue, calcAvgExecuteDurationMax.RowValue, calcAvgExecuteDurationLatest.RowValue);
            }
        }


        public IEnumerable<CSVColumn> GetStatisticsLine()
        {
            return _statisticsLine;
        }

        public void AddMessage(string msg)
        {
            _msgCache.AddMessage(msg);
        } 

        public void PrintMessages()
        {
            _msgCache.PrintMessagesToConsole();
        }

        public async Task StartQueryingDataSeriesAsync(CancellationToken cancelToken, bool queryCalcedAvgValue)
        {
            Stopwatch delay = new Stopwatch();
            while (!cancelToken.IsCancellationRequested)
            {
                delay.Start();
                _lastQueryOpInfo = new OperationReponseInfo();
                try
                {
                    _lastQueryMeasurandList = await ReadMultiDocumentAsync("1000", "Kessel", "Drehzahl_0", true, _lastQueryOpInfo, 10440 /*batchSize*/);

                    if (this._rusConsumed != null)
                    {
                        this._rusConsumed.AddDataCount(_lastQueryOpInfo.OpRequestCharge);
                    }

                    if (queryCalcedAvgValue)
                    {
                        //xAvgQuery
                        //following line runs a query that calcs the average values for the given measurand during querying
                        await ReadAverageMeasurandAsync("1000", "Kessel", "Drehzahl_0", true, _lastQueryOpInfo, 1000 /*batchSize not used yet*/);
                    }
                }
                catch (Exception ex)
                {
                    _msgCache.AddMessage("QueryException:" + ex.Message);
                }

                delay.Stop();


                //await Task.Delay((int)Math.Max((long)0, (long)(1000 - delay.ElapsedMilliseconds)));
                await Task.Delay(1000);
            }
        }



       
        
    }
}

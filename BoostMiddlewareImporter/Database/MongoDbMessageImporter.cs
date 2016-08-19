using BoostMiddlewareImporter.Processor;
using BoostMiddlewareImporter.Server;
using BoostMiddlewareObjects.Helpers;
using Intratec.Common.Services.Logging;
using Intratec.Common.Services.Logging.Log4Net;
using MongoDB.Driver;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BoostMiddlewareImporter.Database
{
    public class MongoDbMessageImporter : BoostMiddlewareImporter.Database.IStatisticsPrinter
    {
        ILogger logger = new Log4NetLogger(typeof(MongoDbMessageImporter));

        private ConcurrentBag<List<PropertyData>> propertyDataStore = new ConcurrentBag<List<PropertyData>>();

        #region writeToDatabase
        private long _databaseImportTaskCount;
        private TroughputStopwatch _recordsThroughput = null;
        public ConcurrentQueue<long> _insertDurationList = new ConcurrentQueue<long>();
        private double _insertDurationAverage;
        private long _insertDurationMax;
        private long _insertDurationLatest;
        #endregion


        public MongoDbMessageImporter()
        {
            TableBulkBatchSize = 0;
            
        }

        private List<PropertyData> GetPropertyDataStore(int count)
        {
            List<PropertyData> result;
            if (!propertyDataStore.TryTake(out result))
            {
                result = new List<PropertyData>();
            }

            while (result.Count < count)
            {
                result.Add(new PropertyData());
            }

            while (result.Count > count)
            {
                result.RemoveAt(result.Count - 1);
            }

            return result;
        }

        private void PutPropertyDataStore(List<PropertyData> list)
        {
            propertyDataStore.Add(list);
        }

        public /*async Task<int> */ int StoreInCollection(IMongoCollection<PropertyData> collection, IList<Server.TCPMessage<JSONContainer>> msgList, MessageProcessor processor)
        {
            int result = 0;

            Interlocked.Increment(ref _databaseImportTaskCount);

            if (_recordsThroughput == null)
            {
                _recordsThroughput = new TroughputStopwatch();
                _recordsThroughput.Start();
                
            }

            Stopwatch sw = Stopwatch.StartNew();
            
            try
            {
                //IList<WriteModel<PropertyData>> bulkList = new List<WriteModel<PropertyData>>();

            
                // ** Act.
                // Now insert this quickly into the db.

                List<PropertyData> list = GetPropertyDataStore(msgList.Count);
                sw.Stop();
                long connectionDuration = sw.ElapsedMilliseconds;
                sw.Reset(); // reset elapsed time
                sw.Restart();
                int count = 0;

                foreach (TCPMessage<JSONContainer> msg in msgList)
                {
                    PropertyData record = list[count];
                    count++;

                    //stuff.Id = "stuffs/" + stuff.Id;

                    if (msg.Data == null)
                    {
                        processor.TransformBytesToJson(msg); //we call this here because a simple TPL Block just for this call causes a bad performance
                    }

                    // use Port of PlantAddress as propertyID
                    int property = 0;
                    DateTime timestampClient = DateTime.Today;
                    int thread = 0;

                    if (msg.Data.PlantData != null)
                    {
                        property = Convert.ToInt32(msg.Data.PlantData.PlantAddress.Substring(msg.Data.PlantData.PlantAddress.IndexOf(":") + 1));
                        timestampClient = msg.Data.PlantData.TimestampClient;
                        thread = msg.Data.PlantData.Thread;
                    }

                    //bulkList.Add(new InsertOneModel<PropertyData>(record));
                   
#if MongoDB 
                    //Id has a different type when not compiled for MongoDB
                    record.Id = MongoDB.Bson.ObjectId.Empty;
#endif
                    record.ClientTimestampUTC = timestampClient;
                    record.JSON = msg.Data.JSONString;
                    record.PropertyID = property;
                    record.Thread = thread;
                    record.Value = null;
                    record.ValueTimestampLocal = msg.TimestampReceivedUTC.ToLocalTime();
                    record.ValueTimestampUTC = msg.TimestampReceivedUTC;
                }
                //long fillDuration = sw.ElapsedMilliseconds;
                //sw.Restart();
                
                //await collection.InsertManyAsync(list);  //await macht das einfügen langsamer
                collection.InsertMany(list);  //the non-async function works 100% faster in our setup !!!!thk 19.01.2016
                //bulkList = null;
                PutPropertyDataStore(list);

                sw.Stop();
                this._recordsThroughput.AddDataCount(count);
                ShowProgress(count, connectionDuration,0,  sw.ElapsedMilliseconds);


                /*cleanup messages*/
                msgList.Select(x => x.Client = null).ToList();

                //makes the completion of the task very slow
                //GC.Collect();

                result = count;
            }
            catch (Exception ex)
            {
                logger.Error("ERROR: InsertMany", ex);
                Console.WriteLine("ERROR: InsertMany {0}", ex.Message);
            }

            Interlocked.Decrement(ref _databaseImportTaskCount);

            return result;
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

            Console.WriteLine("Fillduration: {0}ms", fillDuration);
        }

        public void PrintStatistics(PrintStatisticsArgs args)
        {
            if (_recordsThroughput != null)
            {
                Console.WriteLine("Documents= Total:{0:000000000} Throughput:{1:0000.00}/s AvgThroughput:{2:00000.00}/s   " + Environment.NewLine + 
                                  "InsertDuration= Avg:{3:000.0}ms Max(20):{4:000}ms Latest:{5:000}",
                                  _recordsThroughput.DataCount, _recordsThroughput.GetCurrentThroughput(), _recordsThroughput.GetAverageThroughput(),
                                  _insertDurationAverage, _insertDurationMax, _insertDurationLatest);
                Console.WriteLine("ImportTasks running:{0}", _databaseImportTaskCount);
            }
            
        }

        public IEnumerable<CSVColumn> GetStatisticsLine()
        {
            return null;
        }

        public void PrintMessages()
        {

        }

        public int TableBulkBatchSize { get; set; }
    }
}

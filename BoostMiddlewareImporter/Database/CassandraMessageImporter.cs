using BoostMiddlewareImporter.Processor;
using BoostMiddlewareImporter.Server;
using BoostMiddlewareObjects.Helpers;
using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Intratec.Common.Services.Logging;
using Intratec.Common.Services.Logging.Log4Net;
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

namespace BoostMiddlewareImporter.Database
{
    public class CassandraMessageImporter : BoostMiddlewareImporter.Database.IStatisticsPrinter
    {
        ILogger logger = new Log4NetLogger(typeof(CassandraMessageImporter));

        bool _importerAlreadyCreatedTable = false;

        #region writeToDatabase
        private long _databaseImportTaskCount;
        private TroughputStopwatch _recordsThroughput = null;
        public ConcurrentQueue<long> _insertDurationList = new ConcurrentQueue<long>();
        private double _insertDurationAverage;
        private long _insertDurationMax;
        private long _insertDurationLatest;
        #endregion

        
        public CassandraMessageImporter()
        {
        }

        public Table<PropertyData> CreateSchema(ISession session)
        {
            Table<PropertyData> table = session.GetTable<PropertyData>();
            if (!_importerAlreadyCreatedTable)
            {
                //only test once per importer instance
                table.CreateIfNotExists();
                _importerAlreadyCreatedTable = true;
            }

            return table;
        }


        public /*async Task<int>*/ int StoreInKeySpace(ISession session, IEnumerable<Server.TCPMessage<JSONContainer>> msgList, MessageProcessor processor)
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
                Table<PropertyData> table = CreateSchema(session);

                PropertyData record = null;

                // ** Act.
                // Now insert this quickly into the db.

                Batch batch = session.CreateBatch();
                
                sw.Stop();
                long connectionDuration = sw.ElapsedMilliseconds;
                sw.Reset(); // reset elapsed time
                sw.Restart();
                int count = 0;
                foreach (TCPMessage<JSONContainer> msg in msgList)
                {
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
                    int measurementId = 0;

                    if (msg.Data.PlantData != null)
                    {
                        property = Convert.ToInt32(msg.Data.PlantData.PlantAddress.Substring(msg.Data.PlantData.PlantAddress.IndexOf(":") + 1));
                        timestampClient = msg.Data.PlantData.TimestampClient;
                        thread = msg.Data.PlantData.Thread;
                        measurementId = msg.Data.PlantData.MeasurementId;
                    }

                    record = new PropertyData()
                    {
#if Cassandra
                        Id = Guid.NewGuid().ToString(),
#endif
                        ClientTimestampUTC = timestampClient,
                        JSON = msg.Data.JSONString,
                        PropertyID = property,
                        MeasurementId = measurementId,
                        Thread = thread,
                        Value = null,
                        ValueTimestampLocal = msg.TimestampReceivedUTC.ToLocalTime(),
                        ValueTimestampUTC = msg.TimestampReceivedUTC
                    };

                    batch.Append(table.Insert(record));
                }
                //long fillDuration = sw.ElapsedMilliseconds;
                //sw.Restart();
                //await batch.ExecuteAsync(); //sends records to db
                batch.Execute(); //sends records to db
                batch = null;

                sw.Stop();
                this._recordsThroughput.AddDataCount(count);
                ShowProgress(count, connectionDuration,0,  sw.ElapsedMilliseconds);

                result = count;
            }
            catch (Exception ex)
            {
                logger.Error("ERROR: BulkInsert", ex);
                Console.WriteLine("ERROR: BulkInsert {0}", ex.Message);
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
    }
}

using BoostMiddlewareImporter.Processor;
using BoostMiddlewareImporter.Server;
using BoostMiddlewareObjects.Helpers;
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
    public class RavenMessageImporter : BoostMiddlewareImporter.Database.IStatisticsPrinter
    {
        ILogger logger = new Log4NetLogger(typeof(RavenMessageImporter));

        #region writeToDatabase
        private long _databaseImportTaskCount;
        private TroughputStopwatch _recordsThroughput = null;
        public ConcurrentQueue<long> _insertDurationList = new ConcurrentQueue<long>();
        private double _insertDurationAverage;
        private long _insertDurationMax;
        private long _insertDurationLatest;
        #endregion

        
        public RavenMessageImporter()
        {
            TableBulkBatchSize = 0;
        }


        public async Task<int> StoreInDocumentStore(DocumentStore documentStore, IEnumerable<Server.TCPMessage<JSONContainer>> msgList, MessageProcessor processor)
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
                PropertyData record = null;

                BulkInsertOptions bulkInsertOptions = new BulkInsertOptions();

                if (TableBulkBatchSize > 0)
                {
                    bulkInsertOptions.BatchSize = this.TableBulkBatchSize;
                }

            
                // ** Act.
                // Now insert this quickly into the db.
                BulkInsertOperation bulkInsert = documentStore.BulkInsert(options: bulkInsertOptions);
                try
                {
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

                        if (msg.Data.PlantData != null)
                        {
                            property = Convert.ToInt32(msg.Data.PlantData.PlantAddress.Substring(msg.Data.PlantData.PlantAddress.IndexOf(":") + 1));
                            timestampClient = msg.Data.PlantData.TimestampClient;
                            thread = msg.Data.PlantData.Thread;
                        }

                        record = new PropertyData()
                        {
#if RavenDB
                            Id = Guid.NewGuid().ToString(),
#endif
                            ClientTimestampUTC = timestampClient,
                            JSON = msg.Data.JSONString,
                            PropertyID = property,
                            Thread = thread,
                            Value = null,
                            ValueTimestampLocal = msg.TimestampReceivedUTC.ToLocalTime(),
                            ValueTimestampUTC = msg.TimestampReceivedUTC
                        };

                        bulkInsert.Store(record);

                    }
                    //long fillDuration = sw.ElapsedMilliseconds;
                    //sw.Restart();
                    await bulkInsert.DisposeAsync(); //sends records not yet send during the batch
                    bulkInsert = null;

                    sw.Stop();
                    this._recordsThroughput.AddDataCount(count);
                    ShowProgress(count, connectionDuration,0,  sw.ElapsedMilliseconds);

                    result = count;
                    
                }
                finally
                {
                    if (bulkInsert != null) {
                        try
                        {
                            //bulkInsert might already be disposed, but not set to null yet
                            bulkInsert.Dispose();
                        }
                        catch (Exception)
                        {
                            //TODO remove this try catch block
                        }
                    }
                }
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

        public int TableBulkBatchSize { get; set; }
    }
}

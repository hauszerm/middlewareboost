using BoostMiddlewareImporter.Processor;
using BoostMiddlewareImporter.Server;
using BoostMiddlewareObjects.Helpers;
using Intratec.Common.Services.Logging;
using Intratec.Common.Services.Logging.Log4Net;
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
using System.Threading.Tasks.Dataflow;

namespace BoostMiddlewareImporter.Database
{
    public class MessageImporter : IStatisticsPrinter
    {
        ILogger logger = new Log4NetLogger(typeof(MessageImporter));

        private BufferBlock<RawValueData> dataTableStore = new BufferBlock<RawValueData>();

        #region writeToDatabase
        private long _databaseImportTaskCount;
        private TroughputStopwatch _recordsThroughput = null;
        public ConcurrentQueue<long> _insertDurationList = new ConcurrentQueue<long>();
        private double _insertDurationAverage;
        private long _insertDurationMax;
        private long _insertDurationLatest;
        #endregion

        #region fillDataTable
        private long _dataTableFillTaskCount;
        private TroughputStopwatch _dataTableFillThroughput = null;
        public ConcurrentQueue<long> _fillDataTableDurationList = new ConcurrentQueue<long>();
        private double _fillDataTableDurationAverage;
        private long _fillDataTableDurationMax;
        private long _fillDataTableDurationLatest;
        #endregion

        public MessageImporter()
        {
            TableBulkBatchSize = 0;
        }

        public Task<Database.RawValueData> TransformMessagesToRawValueData(Server.TCPMessage<JSONContainer>[] msgList, MessageProcessor processor)
        {
            Interlocked.Increment(ref _dataTableFillTaskCount);

            if (_dataTableFillThroughput == null)
            {
                _dataTableFillThroughput = new TroughputStopwatch();
                _dataTableFillThroughput.Start();

            }

            Stopwatch sw = Stopwatch.StartNew();

            const string TABLENAME = "PropertyData";

            RawValueData table;

            if (!dataTableStore.TryReceive(out table)) {
                table = new RawValueData(TABLENAME);
            }
            else
            {
                table.Clear();
            }

            bool bFirstRun = true;

            foreach (Server.TCPMessage<JSONContainer> msg in msgList)
            {
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

                    if (bFirstRun && (msg.Data.PlantData.TargetTable > 0))
                    {
                        //insert records to a different table
                        table.TableName = TABLENAME + msg.Data.PlantData.TargetTable.ToString();
                    }
                }

                //table.AddValue(property, msg.TimestampReceivedUTC.ToLocalTime(), msg.TimestampReceivedUTC, null, msg.Data.JSONString, timestampClient, thread); //when JSONString is not stored, a number of records that can be stored is increased by 60%
                table.AddValue(property, msg.TimestampReceivedUTC.ToLocalTime(), msg.TimestampReceivedUTC, null, null, timestampClient, thread);
                bFirstRun = false;
            }
        
            sw.Stop();
            _dataTableFillThroughput.AddDataCount(table.Rows.Count);

            _fillDataTableDurationLatest = sw.ElapsedMilliseconds;
            _fillDataTableDurationList.Enqueue(_fillDataTableDurationLatest);
            if (_fillDataTableDurationList.Count > 20)
            {
                //we are only interested in the last 20 inserts
                long value;
                _fillDataTableDurationList.TryDequeue(out value);
            }

            List<long> currentDurationList = _fillDataTableDurationList.ToList(); //make a copy so that it does not change during average/max calc
            _fillDataTableDurationAverage = currentDurationList.Average();
            _fillDataTableDurationMax = currentDurationList.Max();

            Interlocked.Decrement(ref _dataTableFillTaskCount);

            return Task.FromResult(table);
        }

        //public int WriteToDatabaseSync(string connectionString, DataTable dt)
        //{
        //    if (_recordsThroughput == null)
        //    {
        //        _recordsThroughput = new TroughputStopwatch();
        //        _recordsThroughput.Start();
        //    }

        //    //same Code as WriteToDatabaseAsync !!!!!

        //    SqlBulkCopy sbc = null;

        //    if (dt.Rows.Count == 0)
        //    {
        //        return dt.Rows.Count;
        //    }
        //    int result = 0;
        //    using (SqlConnection connection = new SqlConnection(connectionString))
        //    {
        //        try
        //        {
        //            Stopwatch sw = Stopwatch.StartNew();
        //            connection.Open();
        //            sw.Stop();
        //            long connectionDuration = sw.ElapsedMilliseconds;
        //            sw.Reset(); // reset elapsed time
        //            sw.Restart();
        //            sbc = new SqlBulkCopy(connection, SqlBulkCopyOptions.UseInternalTransaction, null);
        //            sbc.BulkCopyTimeout = 60;

        //            sbc.DestinationTableName = dt.TableName.ToString();
        //            sbc.WriteToServer(dt);

        //            sw.Stop();
        //            ShowProgress(dt.Rows.Count, connectionDuration, sw.ElapsedMilliseconds);

        //            result = dt.Rows.Count;
        //        }
        //        catch (Exception ex)
        //        {
        //            logger.Error("ERROR: WriteToServer", ex);
        //            Console.WriteLine("ERROR: WriteToServer {0}", ex.Message);
        //        }
        //    }
        //    return result;
        //}

        public async Task<int> WriteToDatabaseAsync(string connectionString, RawValueData dt)
        {
            try
            {
                //await Task.Yield(); //ensure execute task on different thread  //does not increase throughput significally
                Interlocked.Increment(ref _databaseImportTaskCount);

                if (_recordsThroughput == null)
                {
                    _recordsThroughput = new TroughputStopwatch();
                    _recordsThroughput.Start();

                }

                //Custom IDataReader implementation http://www.developerfusion.com/article/122498/using-sqlbulkcopy-for-high-performance-inserts/

                //same Code as WriteToDatabaseSync !!!!!

                SqlBulkCopy sbc = null;

                int result = dt.Rows.Count;

                if (dt.Rows.Count > 0)
                {
                    using (SqlConnection connection = new SqlConnection(connectionString)) //sharing SqlConnection between task calls does not increase performance, tk+mh 11.12.2015
                    {
                        try
                        {
                            Stopwatch sw = Stopwatch.StartNew();
                            connection.Open();
                            sw.Stop();
                            long connectionDuration = sw.ElapsedMilliseconds;
                            sw.Reset(); // reset elapsed time
                            sw.Restart();
                            sbc = new SqlBulkCopy(connection, SqlBulkCopyOptions.UseInternalTransaction | SqlBulkCopyOptions.TableLock, null); //sharing SqlBulkCopy between task calls does not increase performance, tk+mh 11.12.2015
                            sbc.BulkCopyTimeout = 10; //default is 30 seconds
                            sbc.DestinationTableName = dt.TableName.ToString();
                            sbc.BatchSize = TableBulkBatchSize;

                            await sbc.WriteToServerAsync(dt); //.ConfigureAwait(false); //reduces performance
                            //Using Wait() does not increase performance
                            //sbc.WriteToServer(dt); //best measured performance when using sync (MessageGeneratorLocally with BatchSize 30000/5000 -> 45 Batch-requests-per-Second (sp_askBrent) 

                            sw.Stop();
                            this._recordsThroughput.AddDataCount(dt.Rows.Count);
                            ShowProgress(dt.Rows.Count, connectionDuration, sw.ElapsedMilliseconds);

                            result = dt.Rows.Count;
                        }
                        catch (Exception ex)
                        {
                            logger.Error("ERROR: WriteToServer", ex);
                            Console.WriteLine("ERROR: WriteToServer {0}", ex.Message);
                        }
                    }
                }

                dataTableStore.Post(dt);

                Interlocked.Decrement(ref _databaseImportTaskCount);

                return result;
            }
            catch (Exception ex)
            {
                Console.WriteLine("MSSQL Importer Exception: " + ex.Message);
                logger.Error(ex);
            }

            return -1;
        }

        private void ShowProgress(int rowCount, long connectionDuration, long insertDuration)
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
        }

        public void PrintStatistics(PrintStatisticsArgs args)
        {
            if (_recordsThroughput != null)
            {
                Console.WriteLine("Batcher Records Waiting: {0}", args.BatcherRecordsWaiting.Invoke());

                Console.WriteLine("DataTableFill-Records= Total:{0:000000000} Throughput:{1:0000.00}/s AvgThroughput:{2:00000.00}/s   " + Environment.NewLine +
                                  "DataTableFillDuration= Avg:{3:000.0}ms Max(20):{4:000}ms Latest:{5:000}",
                                  _dataTableFillThroughput.DataCount, _dataTableFillThroughput.GetCurrentThroughput(), _dataTableFillThroughput.GetAverageThroughput(),
                                  _fillDataTableDurationAverage, _fillDataTableDurationMax, _fillDataTableDurationLatest);
                Console.WriteLine("DataTableFill-Tasks running:{0}", _dataTableFillTaskCount);

                Console.WriteLine("Records= Total:{0:000000000} Throughput:{1:0000.00}/s AvgThroughput:{2:00000.00}/s   " + Environment.NewLine + 
                                  "InsertDuration= Avg:{3:000.0}ms Max(20):{4:000}ms Latest:{5:000}",
                                  _recordsThroughput.DataCount, _recordsThroughput.GetCurrentThroughput(), _recordsThroughput.GetAverageThroughput(),
                                  _insertDurationAverage, _insertDurationMax, _insertDurationLatest);
                Console.WriteLine("ImportTasks running:{0} blocksWaiting:{1}", _databaseImportTaskCount, args.DbInsertBlocksWaiting.Invoke());
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

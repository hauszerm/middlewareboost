using BoostMiddlewareImporter.Server;
using BoostMiddlewareObjects;
using BoostMiddlewareObjects.Helpers;
using Intratec.Common.Services.Logging;
using Intratec.Common.Services.Logging.Log4Net;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BoostMiddlewareImporter.Processor
{
    public class MessageProcessor
    {
        ILogger logger = new Log4NetLogger(typeof(MessageProcessor));

        private long _jsonTaskCount = 0;
        private long _extraDataTaskCount = 0;
        private TroughputStopwatch _jsonThroughput = null;
        private TroughputStopwatch _extraDataThroughput;

        private long _memoryIncreased = 0;

        public Server.TCPMessage<JSONContainer> TransformBytesToJson(Server.TCPMessage<JSONContainer> msg)
        {
            long totalMemory = GC.GetTotalMemory(false);

            Interlocked.Increment(ref _jsonTaskCount);

            if (_jsonThroughput == null)
            {
                _jsonThroughput = new TroughputStopwatch();
                _jsonThroughput.Start();
            }

            //convert ByteArray to UTF-8 string
            msg.Data = new JSONContainer()
            {
                JSONString = System.Text.Encoding.UTF8.GetString(msg.RawData),
            };

            try
            {
                /*for performances*/
                //Thread.SpinWait(10000);
                //msg.Data.PlantData = PlantData.CreatePlantData();
                msg.Data.PlantData = JsonConvert.DeserializeObject<PlantData>(msg.Data.JSONString);

                _jsonThroughput.AddDataCount(1);
            }
            catch (Exception ex)
            {
                logger.Error(String.Format("ERROR: TransformBytesToJson. JSON: {0}", msg.Data.JSONString), ex);
                Console.WriteLine("ERROR: TransformBytesToJson {0}", ex.Message);
            }

            Interlocked.Decrement(ref _jsonTaskCount);

            lock (this)
            {
                _memoryIncreased = GC.GetTotalMemory(false) - totalMemory;
            }
            
            return msg;
        }

        /// <summary>
        /// Convert one message from the client to multiple DataRecords for the database
        /// </summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        public List<Server.TCPMessage<JSONContainer>> ExtractDataPointsFromTCPMessage(Server.TCPMessage<JSONContainer> msg)
        {
            Interlocked.Increment(ref _extraDataTaskCount);

            if (_extraDataThroughput == null)
            {
                _extraDataThroughput = new TroughputStopwatch();
                _extraDataThroughput.Start();
            }

            if (msg.Data == null)
            {
                msg = TransformBytesToJson(msg);
            }

            List<Server.TCPMessage<JSONContainer>> result = new List<Server.TCPMessage<JSONContainer>>();

            result.Add(msg);

            if (msg.Data != null && msg.Data.PlantData != null)
            {
                //nur Messages mit gültigen JSON Daten weiterverarbeiten

                if (msg.Data.PlantData.MeasurmentList != null)
                {
                    foreach (PlantData mData in msg.Data.PlantData.MeasurmentList)
                    {
                        result.Add(new Server.TCPMessage<JSONContainer>()
                        {
                            Client = msg.Client,
                            Data = new JSONContainer() { PlantData = mData },
                            RawData = null,
                            TimestampReceivedUTC = msg.TimestampReceivedUTC,
                        });
                    }
                }
            
                _extraDataThroughput.AddDataCount(result.Count);
            }
            Interlocked.Decrement(ref _extraDataTaskCount);

            return result;
        }


        public void PrintStatistics()
        {
            if (_extraDataThroughput != null)
            {
                Console.WriteLine("Processor-Extra= Records:{0:000000} Throughput:{1:0000.00}/s AvgThroughput:{2:00000.00}/s Memory(delta):{3}",
                                  _extraDataThroughput.DataCount, _extraDataThroughput.GetCurrentThroughput(), _extraDataThroughput.GetAverageThroughput(), NetworkUtils.ConvertBytesToHumanReadable(_memoryIncreased));
            }

            Console.WriteLine(String.Format("ProcessorTasks running= JSON {0:00}: ExtraData:{1:00}", _jsonTaskCount, _extraDataTaskCount));
        }
    }
}

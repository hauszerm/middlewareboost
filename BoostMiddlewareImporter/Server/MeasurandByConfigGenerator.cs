using BoostMiddlewareImporter.Processor;
using BoostMiddlewareImporter.Server.CyclicMeasurandGeneratorConfig;
using BoostMiddlewareObjects;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BoostMiddlewareImporter.Server
{
    public abstract class MeasurandByConfigGenerator : IMessageGenerator
    {
        protected TaskFactory _localTaskFactory;
        protected ConcurrentBag<TCPMessage<Measurand>> _tcpMessageStore;
        protected IMessageProcessor<Measurand> _processor;
        protected List<PlantConfiguration> _config;
        

        public const int PLANT_ID_FIRST = 1000;

        public bool GenerateMessages { get; protected set; }

        public int Connections
        {
            get
            {
                return 1;
            }
        }

        public MeasurandByConfigGenerator(IMessageProcessor<Measurand> processor, TaskScheduler ts, ConcurrentBag<TCPMessage<Measurand>> tcpMessageStore)
        {
            if (ts == null)
            {
                ts = TaskScheduler.Default;
            }

            _localTaskFactory = new TaskFactory(
                    CancellationToken.None, TaskCreationOptions.DenyChildAttach,
                    TaskContinuationOptions.None, ts);

            GenerateMessages = false; //set to true in Run

            _tcpMessageStore = tcpMessageStore;
            if (_tcpMessageStore == null)
            {
                _tcpMessageStore = new ConcurrentBag<TCPMessage<Measurand>>();
            }

            _processor = processor;
        }

        public void SetupTestdata(int plantCount, string configFileName)
        {
            try
            {
                
                _config = new List<PlantConfiguration>();
                foreach (int plantId in Enumerable.Range(PLANT_ID_FIRST, plantCount))
                {
                    _config.Add(new PlantConfiguration() {
                        Id = plantId,
                        ClientConfig = new List<ClientConfiguration>(),
                    });
                }
                

                var reader = new StreamReader(File.OpenRead(configFileName));
                List<ClientConfiguration> configList = new List<ClientConfiguration>();

                // read first line = header
                string line = reader.ReadLine();

                while (!reader.EndOfStream)
                {
                    line = reader.ReadLine();
                    string[] values = line.Split(';'); //Cycle;Menge;Name;Wert
                    foreach (int plantId in Enumerable.Range(PLANT_ID_FIRST, plantCount))
                    {
                        PlantConfiguration pConfig = _config.FirstOrDefault(p => p.Id == plantId);
                        Debug.Assert(pConfig != null, String.Format("PlantId nicht gefunden {0}", plantId));


                        ClientConfiguration config = new ClientConfiguration();
                        pConfig.ClientConfig.Add(config);
                        config.cycle = Convert.ToInt32(values[0]);
                        config.count = Convert.ToInt32(values[1]);
                        config.measurands = new List<Measurand>(config.count);

                        pConfig.SmallestInterval = Math.Min(pConfig.SmallestInterval, config.cycle);
                        
                        Random rnd = new Random();
                        for (int i = 0; i < config.count; i++)
                        {
                            Measurand measurand = new Measurand();
                            // change name and value
                            measurand.Name = values[2] + "_" + i.ToString();
                            measurand.Value = Convert.ToDouble(values[3]);
                            measurand.Plant = plantId.ToString();
                            measurand.Equipment = values[4];
                            measurand.RandomizeValue(rnd);
                            config.measurands.Add(measurand);
                        }
                    }

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
                return;
            }
        }


        public void PrintStatistics()
        {
            
        }

        public void PrintStatisticsAverage()
        {
            
        }

        public abstract Task Run(CancellationToken cancelToken);
        

        public void Stop()
        {
            GenerateMessages = false;
        }



        public int GetFirstPlantId()
        {
            return PLANT_ID_FIRST;
        }

        public int GetPlantCount()
        {
            return _config.Count;
        }
    }
}

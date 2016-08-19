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

    /// <summary>
    /// Generate Measurands based on a cyclicData ConfigFile but generate all Measurands that would be generated in the next 24 hours.
    /// </summary>
    public class PeriodMeasurandGenerator : MeasurandByConfigGenerator
    {
        private List<Task> _generatedTasks;
        private int _periodMinutes;


        public PeriodMeasurandGenerator(IMessageProcessor<Measurand> processor, TaskScheduler ts, ConcurrentBag<TCPMessage<Measurand>> tcpMessageStore, int periodMinutes = (60 * 24 /*1 day*/))
            : base(processor, ts, tcpMessageStore)
        {
            _periodMinutes = periodMinutes;
        }

   
        public override Task Run(CancellationToken cancelToken)
        {
            _generatedTasks = new List<Task>();

            if (!GenerateMessages)
            {
                GenerateMessages = true;

                List<Task> plantTasks = new List<Task>();

                foreach (PlantConfiguration pConfig in _config)
                {
                    Task localTask = _localTaskFactory.Create(async (plantConfigState) =>
                    {
                        Random rnd = new Random();
                        List<TCPMessage<Measurand>> measurandsToSend = new List<TCPMessage<Measurand>>();
                        PlantConfiguration plantConfig = plantConfigState as PlantConfiguration;
                        DateTime utcStart = DateTime.UtcNow;
                        while (GenerateMessages && (!cancelToken.IsCancellationRequested))
                        {
                            foreach (ClientConfiguration clientConfig in plantConfig.ClientConfig)
                            {
                                foreach(Measurand m in clientConfig.measurands) {

                                    TCPMessage<Measurand> container;
                                    if (!_tcpMessageStore.TryTake(out container))
                                    {
                                        container = new TCPMessage<Measurand>();
                                        container.Data = new Measurand();
                                    }

                                    Measurand.UpdateMeasurand(m, container.Data);
                                    container.Data.Timestamp = clientConfig.NextCycleStartUTC;
                                    container.Data.RandomizeValue(rnd);

                                    measurandsToSend.Add(container);
                                }

                                //set next timestamp to the timestamp ahead by the cylic interval
                                clientConfig.NextCycleStartUTC = clientConfig.NextCycleStartUTC.AddSeconds(clientConfig.cycle);
                            }

                            foreach (TCPMessage<Measurand> msg in measurandsToSend)
                            {
                                if (cancelToken.IsCancellationRequested || (!GenerateMessages))
                                {
                                    break;
                                }

                                if (!_processor.ProcessMessage(msg))
                                {
                                    Console.WriteLine("Message konnte nicht weitergegeben werden, warte 1000ms...");
                                    await Task.Delay(1000);
                                }
                            }

                            measurandsToSend.Clear();

                            if (plantConfig.ClientConfig.Select(c => c.NextCycleStartUTC).Min() > utcStart.AddMinutes(_periodMinutes))
                            {
                                /*we have now generated all measurands for the required period, stop generating more*/
                                GenerateMessages = false;
                            }
                            
                        }; //while GenerateMessages

                    }, pConfig, TaskCreationOptions.LongRunning);

                    localTask.Start();
                    plantTasks.Add(localTask);

                    _generatedTasks.Add(localTask);
                }//foreach pConfig in _config

                Task.WaitAll(plantTasks.ToArray());
                _processor.FinishProcessing(); //inform that processor that no more messages will be generated
            }

            return Task.WhenAll(_generatedTasks);
        }
    }
}

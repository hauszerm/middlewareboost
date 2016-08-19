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
    public class CyclicMeasurandGenerator : MeasurandByConfigGenerator
    {
        private List<Task> _generatedTasks;


        public CyclicMeasurandGenerator(IMessageProcessor<Measurand> processor, TaskScheduler ts, ConcurrentBag<TCPMessage<Measurand>> tcpMessageStore):base(processor, ts, tcpMessageStore)
        {
        }


        public override Task Run(CancellationToken cancelToken)
        {
            _generatedTasks = new List<Task>();

            if (!GenerateMessages)
            {
                GenerateMessages = true;

                foreach (PlantConfiguration pConfig in _config)
                {
                    Task localTask = _localTaskFactory.Create(async (plantConfigState) =>
                    {
                        Random rnd = new Random();
                        List<TCPMessage<Measurand>> measurandsToSend = new List<TCPMessage<Measurand>>();
                        PlantConfiguration plantConfig = plantConfigState as PlantConfiguration;
                        DateTime utcNow;
                        while (GenerateMessages && (!cancelToken.IsCancellationRequested))
                        {
                            utcNow = DateTime.UtcNow;
                            foreach (ClientConfiguration clientConfig in plantConfig.ClientConfig)
                            {
                                if (clientConfig.NextCycleStartUTC <= utcNow)
                                {
                                    foreach(Measurand m in clientConfig.measurands) {

                                        TCPMessage<Measurand> container;
                                        if (!_tcpMessageStore.TryTake(out container))
                                        {
                                            container = new TCPMessage<Measurand>();
                                            container.Data = new Measurand();
                                        }

                                        Measurand.UpdateMeasurand(m, container.Data);
                                        container.Data.NewTimestamp();
                                        container.Data.RandomizeValue(rnd);

                                        measurandsToSend.Add(container);
                                    }

                                    clientConfig.NextCycleStartUTC = utcNow.AddSeconds(clientConfig.cycle);
                                }

                            }

                            foreach (TCPMessage<Measurand> msg in measurandsToSend)
                            {
                                if (cancelToken.IsCancellationRequested)
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

                            if (GenerateMessages)
                            {
                                Thread.Sleep(Math.Min(5, plantConfig.SmallestInterval) * 1000);
                            }
                        };
                    }, pConfig, TaskCreationOptions.LongRunning);

                    localTask.Start();

                    _generatedTasks.Add(localTask);
                }//foreach pConfig in _config
            }

            return Task.WhenAll(_generatedTasks);
        }
    }
}

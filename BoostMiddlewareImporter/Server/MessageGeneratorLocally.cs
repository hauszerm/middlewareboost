using BoostMiddlewareImporter.Processor;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BoostMiddlewareImporter.Server
{
    public class MessageGeneratorLocally : IMessageGenerator
    {
        private Task<Task> _localTask;
        public TaskFactory _localTaskFactory;
        private ConcurrentBag<TCPMessage<JSONContainer>> _tcpMessageStore;
        private IMessageProcessor<JSONContainer> _processor;

        public MessageGeneratorLocally(IMessageProcessor<JSONContainer> processor, TaskScheduler ts, ConcurrentBag<TCPMessage<JSONContainer>> tcpMessageStore)
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
                _tcpMessageStore = new ConcurrentBag<TCPMessage<JSONContainer>>();
            }

            _processor = processor;
        }

        public void PrintStatistics()
        {
            
        }

        public void PrintStatisticsAverage()
        {
            
        }

        public Task Run(CancellationToken cancelToken)
        {
            const int TARGET_TABLE_COUNT = 1;

            if (!GenerateMessages)
            {
                GenerateMessages = true;

                _localTask = _localTaskFactory.Create(async () =>
                {
                    int targetTable = 0;
                    while (GenerateMessages && (!cancelToken.IsCancellationRequested))
                    {
                        TCPMessage<JSONContainer> container;
                        if (!_tcpMessageStore.TryTake(out container))
                        {
                            container = new TCPMessage<JSONContainer>();
                            container.Data = new JSONContainer();
                            container.Data.PlantData = new BoostMiddlewareObjects.PlantData();
                            container.Data.PlantData.TimestampClient = DateTime.Now;
                        }

                        container.Data.PlantData.PlantAddress = "127.0.0.1:4711";
                        container.Data.PlantData.TargetTable = (targetTable++ % TARGET_TABLE_COUNT) + 1; //Auf TARGET_TABLE_COUNT Tables aufteilen

                        await _processor.ProcessMessageAsync(container, cancelToken);
                    };
                });

                _localTask.Start();
            }
            return _localTask;
        }

        public void Stop()
        {
            GenerateMessages = false;
        }



        public bool GenerateMessages { get; protected set; }

        public int Connections
        {
            get
            {
                return 1;
            }
        }
    }
}

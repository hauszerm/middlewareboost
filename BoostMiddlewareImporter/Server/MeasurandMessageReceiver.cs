using BoostMiddlewareImporter.Database;
using BoostMiddlewareImporter.Processor;
using BoostMiddlewareObjects;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BoostMiddlewareImporter.Server
{
    public class MeasurandMessageReceiver : IMessageProcessor<Measurand>
    {
        private static ITargetBlock<TCPMessage<Measurand>> _queue = null;
        //private ConcurrentQueue<TCPMessage<Measurand>> _queue2;

        public MeasurandMessageReceiver(ITargetBlock<TCPMessage<Measurand>> queue)
        {
            _queue = queue;
            //_queue2 = new ConcurrentQueue<TCPMessage<Measurand>>(); //for testing how fast we can add messages to this queue
        }

        
        public async Task<bool> ProcessMessageAsync(TCPMessage<Measurand> message, CancellationToken cancelToken)
        {
            //RawValueData values = new RawValueData("PropertyData");
            //values.AddValue(1, message.TimestampReceivedUTC.ToLocalTime(), message.TimestampReceivedUTC, null);

            return await _queue.SendAsync(message, cancelToken).ConfigureAwait(false);
        }

        public bool ProcessMessage(TCPMessage<Measurand> message)
        {
            return _queue.Post(message);
            //_queue2.Enqueue(message);
            //return true;
        }

        public void FinishProcessing()
        {
            _queue.Complete();
        }
    }
}

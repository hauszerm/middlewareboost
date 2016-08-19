using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BoostMiddlewareImporter.Server
{
    public interface IMessageProcessor<TData>
    {
        Task<bool> ProcessMessageAsync(TCPMessage<TData> message, CancellationToken cancelToken);

        bool ProcessMessage(TCPMessage<TData> message);

        void FinishProcessing();
    }
}

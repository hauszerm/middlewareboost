using System;
using System.Threading;
namespace BoostMiddlewareImporter.Server
{
    public interface IMessageGenerator
    {
        void PrintStatistics();
        void PrintStatisticsAverage();
        System.Threading.Tasks.Task Run(CancellationToken cancelToken);
        void Stop();

        int Connections { get; }
    }
}

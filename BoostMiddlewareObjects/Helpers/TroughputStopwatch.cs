using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BoostMiddlewareObjects.Helpers
{
    /// <summary>
    /// Troughput of a value with long datatype
    /// </summary>
    public class TroughputStopwatch
    {
        private Stopwatch _stopwatch;
        private long _dataCount = 0;
        private long _backupDataCount = 0;
        private long _totalPeriod = 0;
        private Queue<double> _averageThroughput = new Queue<double>();

        public long DataCount { get { return _dataCount; } }

        public TroughputStopwatch()
        {
            _stopwatch = new Stopwatch();
        }

        public void Start()
        {
            _stopwatch.Start();
            _dataCount = 0;
            _backupDataCount = 0;
            _totalPeriod = 0;
        }

        public void AddDataCount(long count)
        {
            Interlocked.Add(ref _dataCount, count);
        }

        public double GetCurrentThroughput()
        {
            lock (this)
            {
                long curDataCount = _dataCount - _backupDataCount;
                _backupDataCount = _dataCount;
                _stopwatch.Stop();
                long period = _stopwatch.ElapsedMilliseconds; //milliseconds
                _totalPeriod += period;
                _stopwatch.Restart();

                double throughput = CalcThroughput(curDataCount, period);

                
                _averageThroughput.Enqueue(throughput);

                if (_averageThroughput.Count > 20)
                {
                    _averageThroughput.Dequeue();
                }

                return throughput;
            }
        }

        private double CalcThroughput(long dataCount, long periodMilliseconds)
        {
            return ((double)dataCount / (double)periodMilliseconds) * 1000; // bytes per second // ( * 1000) converts milliseconds to seconds
        }

        public double GetAverageThroughput()
        {
            lock (this)
            {
                return _averageThroughput.Average();
            }
        }

        
    }
}

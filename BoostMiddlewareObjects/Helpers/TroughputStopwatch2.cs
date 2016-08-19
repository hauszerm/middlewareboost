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
    /// Troughput of a value with double datatype
    /// </summary>
    public class TroughputStopwatch2
    {
        private Stopwatch _stopwatch;
        private double _dataCount = 0;
        private double _backupDataCount = 0;
        private double _totalPeriod = 0;
        private Queue<double> _averageThroughput = new Queue<double>();

        public double DataCount { get { return _dataCount; } }

        public TroughputStopwatch2()
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

        public void AddDataCount(double count)
        {
            lock (this)
            {
               _dataCount += count;
            }
        }

        public double GetCurrentThroughput()
        {
            lock (this)
            {
                double curDataCount = _dataCount - _backupDataCount;
                _backupDataCount = _dataCount;
                _stopwatch.Stop();
                double period = _stopwatch.ElapsedMilliseconds; //milliseconds
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

        private double CalcThroughput(double dataCount, double periodMilliseconds)
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

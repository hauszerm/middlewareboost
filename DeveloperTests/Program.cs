using BoostMiddlewareObjects.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeveloperTests
{
    class Program
    {
        static void Main(string[] args)
        {
            DateTime nextEval = DateTime.Now.AddSeconds(2.5);
            TroughputStopwatch sw = new TroughputStopwatch();
            sw.Start();

            bool bContinue = true;

            while (bContinue)
            {
                if (nextEval <= DateTime.Now)
                {
                    
                    sw.AddDataCount(20000);
                    Console.WriteLine("Troughput: " + sw.GetCurrentThroughput());
                    nextEval = DateTime.Now.AddSeconds(2.5);
                }
            }
        }
    }
}

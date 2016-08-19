using BoostMiddlewareObjects.Helpers;
using System;
using System.Collections.Generic;
namespace BoostMiddlewareImporter.Database
{
    public interface IStatisticsPrinter
    {
        void PrintStatistics(PrintStatisticsArgs args);

        
        IEnumerable<CSVColumn> GetStatisticsLine();

        void PrintMessages();
    }

    public class PrintStatisticsArgs
    {
        public Func<int> DbInsertBlocksWaiting {get;set;}

        public Func<int> BatcherRecordsWaiting {get;set;}
    }
}

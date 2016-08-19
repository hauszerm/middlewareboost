using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BoostMiddlewareObjects.Helpers
{
    public class CSVColumn
    {
        public CSVColumn(string name=null,  string value=null)
        {
            ColumnName = name;
            RowValue = value;
        }

        public string ColumnName { get; set; }
        public string RowValue { get; set; }
    }
}

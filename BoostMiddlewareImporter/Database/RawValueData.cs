using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BoostMiddlewareImporter.Database
{
    public class RawValueData : DataTable
    {
        private DataColumn colModulPropertiesID = new DataColumn("PropertyID", typeof(int));
        private DataColumn colValueTimestampLocal = new DataColumn("ValueTimestampLocal", typeof(DateTime));
        private DataColumn colValueTimestampUTC = new DataColumn("ValueTimestampUTC", typeof(DateTime));
        private DataColumn colValue = new DataColumn("Value", typeof(float));
        private DataColumn colJSON = new DataColumn("JSON", typeof(string));
        private DataColumn colClientTimestampUTC = new DataColumn("ClientTimestampUTC", typeof(DateTime));
        private DataColumn colThread = new DataColumn("Thread", typeof(Int32));

        public RawValueData(string strName)
            : base(strName)
        {
            Initialize();
        }

        private void Initialize()
        {
            colModulPropertiesID.AllowDBNull = false;
            Columns.Add(colModulPropertiesID);
            Columns.Add(colValueTimestampLocal);
            Columns.Add(colValueTimestampUTC);
            Columns.Add(colValue);
            Columns.Add(colJSON);
            Columns.Add(colClientTimestampUTC);
            Columns.Add(colThread);
        }

        public bool AddValue(int iPropertyID, DateTime dtLocal, DateTime dtUTC, float? value, string json, DateTime clientTimestamp)
        {
            DataRow dr = NewRow();

            dr[colModulPropertiesID] = iPropertyID;
            dr[colValueTimestampLocal] = dtLocal;
            dr[colValueTimestampUTC] = dtUTC;
            if (value == null)
            {
                dr[colValue] = DBNull.Value;
            }
            else
            {
                dr[colValue] = value;
            }
            dr[colJSON] = json;
            dr[colClientTimestampUTC] = clientTimestamp;
            Rows.Add(dr);
            return true;
        }

        public bool AddValue(int iPropertyID, DateTime dtLocal, DateTime dtUTC, float? value, string json, DateTime clientTimestamp, Int32 Thread)
        {
            DataRow dr = NewRow();

            dr[colModulPropertiesID] = iPropertyID;
            dr[colValueTimestampLocal] = dtLocal;
            dr[colValueTimestampUTC] = dtUTC;
            if (value == null)
            {
                dr[colValue] = DBNull.Value;
            }
            else
            {
                dr[colValue] = value;
            }
            dr[colJSON] = json;
            dr[colClientTimestampUTC] = clientTimestamp;
            dr[colThread] = Thread;
            Rows.Add(dr);
            return true;
        }

    
    }
}

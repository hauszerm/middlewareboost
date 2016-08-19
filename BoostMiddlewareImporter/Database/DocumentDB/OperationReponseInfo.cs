using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BoostMiddlewareImporter.Database.DocumentDB
{
    public class OperationReponseInfo
    {
        //public List<Student> ListOfStudents { get; set; }

        public int NumberOfDocumentsRead { get; set; }
        public int NumberOfDocumentsInserted { get; set; }
        public double OpRequestCharge { get; set; }
        public long ElapsedMilliseconds { get; set; }

        public OperationReponseInfo()
        {
            //ListOfStudents = null;
            NumberOfDocumentsRead = 0;
            NumberOfDocumentsInserted = 0;
            OpRequestCharge = 0.0;
            ElapsedMilliseconds = 0;
        }
    }
}

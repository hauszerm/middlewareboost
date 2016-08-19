using BoostMiddlewareObjects.Helpers;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BoostMiddlewareImporter.Server
{
    public class AsyncUserToken<TData>
    {
        #region length prefix message processing
        internal const int RECEIVE_PREFIX_LENGTH = 4;

        internal Int32 lengthOfCurrentIncomingMessage;

        //the bufferOffset + the prefixLength (prefixLength is subtracted in the PrefixHandler)
        internal readonly Int32 permanentReceiveMessageOffset;

        //receiveMessageOffset is used to mark the byte position where the message
        //begins in the receive buffer. This value can sometimes be out of
        //bounds for the data stream just received. But, if it is out of bounds, the 
        //code will not access it.
        internal Int32 receiveMessageOffset;
        internal Byte[] byteArrayForPrefix;
        
        internal Int32 receivedPrefixBytesDoneCount = 0;
        internal Int32 receivedMessageBytesDoneCount = 0;
        //This variable will be needed to calculate the value of the
        //receiveMessageOffset variable in one situation. Notice that the
        //name is similar but the usage is different from the variable
        //receiveSendToken.receivePrefixBytesDone.
        internal Int32 recPrefixBytesDoneThisOp = 0;


        public int receivePrefixLength
        {
            get
            {
                return AsyncUserToken<TData>.RECEIVE_PREFIX_LENGTH;
            }
        }

        #endregion


        public System.Net.Sockets.Socket Socket { get; set; }

        #region statistics / througput calc

#if TCPOnly
        public AsyncTcpServer.LogInfo Stats { get; set; }
#else
        public LogInfo Stats { get; set; }
#endif

        public long byteCountTotal;
        public long byteCountTotalBackup;
        public TroughputStopwatch MsgTroughput;

        public Stopwatch Stopwatch;
        public int bufferOffsetReceive;

        #endregion

        public AsyncUserToken(Int32 rOffset)
        {
            //this.idOfThisObject = identifier;
            this.bufferOffsetReceive = rOffset;
            //Create a Mediator that has a reference to the SAEA object.
            //this.theMediator = new Mediator(e);
            //this.receivePrefixLength = receivePrefixLength;
            //this.sendPrefixLength = sendPrefixLength;
            this.receiveMessageOffset = rOffset + this.receivePrefixLength;
            this.permanentReceiveMessageOffset = this.receiveMessageOffset;

            Reset();

            byteCountTotal = 0;
            byteCountTotalBackup = 0;
            MsgTroughput = new TroughputStopwatch();
            Stopwatch = new Stopwatch();
        }

        public void Reset(int increaseMessageOffset=0)
        {
            this.receivedPrefixBytesDoneCount = 0;
            this.receivedMessageBytesDoneCount = 0;
            this.recPrefixBytesDoneThisOp = 0;
            if (increaseMessageOffset == 0)
            {
                //reset to the original position of the buffer
                this.receiveMessageOffset = this.permanentReceiveMessageOffset;
            }
            else
            {
                //increase the start position in the buffer, include the prefixLength (prefixLength is included in "permanentReceiveMessageOffset" too
                this.receiveMessageOffset += (increaseMessageOffset + this.receivePrefixLength);
            }
        }

        public TCPMessage<TData> theDataHolder { get; set; }

        

        internal string GetClientInfo()
        {
            if (Socket == null) return null;

            return Socket.RemoteEndPoint.ToString();
        }
    }
}

using System;
using System.Collections.Generic;
using System.Text;
using Kernel.Common;
using System.Collections.Concurrent;

namespace Kernel.QueueManager
{
    public interface IQueueManager
    {
     
        void PutMessage(xmlMessage msg);

        xmlMessage GetMessage();

        int GetCount();

        void ClearQueue();
    }

    public class QueueManager : IQueueManager
    {
        private Dictionary<string, ConcurrentQueue<xmlMessage>> _MsgQueueList;
        private const string MQTTManager = "MQTTManager";


        public  QueueManager()
        {
            this._MsgQueueList = new Dictionary<string, ConcurrentQueue<xmlMessage>>();
            ConcurrentQueue<xmlMessage> value = new ConcurrentQueue<xmlMessage>();
            this._MsgQueueList.Add(MQTTManager, value);
        }



        public void PutMessage(xmlMessage msg)
        {
            this._MsgQueueList[MQTTManager].Enqueue(msg);
        }

        public xmlMessage GetMessage()
        {
            xmlMessage result = null;
            if (this._MsgQueueList[MQTTManager].Count > 0)
            {
                this._MsgQueueList[MQTTManager].TryDequeue(out result);
            }
            return result;
        }

        public int GetCount()
        {
          
                return this._MsgQueueList[MQTTManager].Count;

        }

        public void ClearQueue()
        {

            lock (this._MsgQueueList)
            {
                this._MsgQueueList[MQTTManager] = new ConcurrentQueue<xmlMessage>();
            }
        }
    }
}

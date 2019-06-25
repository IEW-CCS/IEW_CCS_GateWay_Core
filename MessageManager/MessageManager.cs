using System;
using System.Xml;
using System.Threading;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Kernel.Common;
using Kernel.Interface;
using System.Linq;


namespace Kernel.MessageManager
{
    public interface IMessageManager
    {
        void MessageDispatch(string name, object[] parameters);
    }


    public class MessageManager: IMessageManager
    {
       
        private readonly ILogger _logger;
     //   private readonly ILogger<Connector> _Connectlogger;
        private readonly IServiceProvider _serviceProvider;

        private object _syncObject = new object();
        public bool _stopFlag = false;
        private double _concurrentWorkCount;
        private Dictionary<string, List<Connector>> _messageMapping;

        public MessageManager(ILoggerFactory loggerFactory, IServiceProvider service)
        {
            _logger = loggerFactory.CreateLogger("Core");
            //   _logger = loggerFactory.CreateLogger<MessageManager>();
            //   _Connectlogger = loggerFactory.CreateLogger<Connector>();
            _serviceProvider = service;

            //----初始化 ------
            Init();
        }

        public void Init()
        {
            _logger.LogInformation("Message Management Initial");

            try
            {
                string Message_Path = AppContext.BaseDirectory + "/settings/Message.xml";

                XmlDocument document = new XmlDocument();
                document.Load(Message_Path);

                Dictionary<string, List<Connector>> dictionary = new Dictionary<string, List<Connector>>();
                XmlNode documentElement = document.DocumentElement;
                foreach (XmlNode childNode in documentElement.ChildNodes)
                {
                    if (childNode.NodeType == XmlNodeType.Element)
                    {
                        this.MessageMap_Registered(childNode, dictionary);
                    }
                    else if (childNode.NodeType == XmlNodeType.EntityReference)
                    {
                        foreach (XmlNode childNode2 in childNode.ChildNodes)
                        {
                            if (childNode2.NodeType == XmlNodeType.Element)
                            {
                                this.MessageMap_Registered(childNode2, dictionary);
                            }
                        }
                    }
                }

                this._messageMapping = dictionary;
            }
            catch (Exception ex)
            {
                _logger.LogError("Initial Error Error, Msg = " + ex.Message);
            }
        }

        private void MessageMap_Registered(XmlNode parentNode, Dictionary<string, List<Connector>> mapping)
        {
            
            string value = parentNode.Attributes["name"].Value;
            if (parentNode.HasChildNodes)
            {
                List<Connector> list = new List<Connector>();
                foreach (XmlNode childNode in parentNode.ChildNodes)
                {
                    Connector item = new Connector(_logger);
                    item.Init(childNode.Attributes["id"].Value, childNode.Attributes["method"].Value);
                    //item.Service = (_serviceProvider.GetService<IObjectManager>()) as ObjectManager.IService;  
                    var ObjectManager = _serviceProvider.GetServices<IService>();
                    var obj = ObjectManager.Where(o => o.ServiceName.Equals(item.ObjectId)).FirstOrDefault();
                    if (obj!= null)
                    {
                        item.Service = (obj) as IService;
                        list.Add(item);
                    }
                    else
                    {
                        _logger.LogError("Object ID : "+ item.ObjectId + " Not Exist in Register Table");
                    }
                }
                try
                {
                    mapping.Add(value, list);
                }
                catch (Exception)
                {
                    throw new Exception(string.Format("message=[{0}] is Add Error .", value));
                }
                return;
            }
            throw new Exception(string.Format("Message=[{0}] is not setting handler information.", value));
            
        }

        public void MessageDispatch(string name, object[] parameters)
        {
            lock (this._syncObject)
            {
                this._concurrentWorkCount += 1.0;
                if (this._concurrentWorkCount > double.MaxValue)
                {
                    this._concurrentWorkCount = 1.0;
                }
            }
            if (!this._stopFlag)
            {
                if (this._messageMapping.ContainsKey(name))
                {
                    foreach (Connector item in this._messageMapping[name])
                    {
                        if (!item.BeginInvoke(parameters))
                        {
                            throw new Exception(string.Format("Message {0},objectID {1},Method {2}, Queue User Work Item Error.", name, item.ObjectId, item.MethodName));
                        }
                    }
                }
                return;
            }
            throw new Exception("Message Dispatch is Stop!");
        }
    }
}

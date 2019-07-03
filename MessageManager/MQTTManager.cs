using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Text;
using System.Threading;
using Kernel.Interface;

using MQTTnet;
using MQTTnet.Client;
using System.Xml.Linq;
using Kernel.MessageManager;
using Kernel.QueueManager;
using Kernel.Common;

// --- Using MQTTNet 2.8.3

namespace Kernel.MQTTManager
{

    public interface IMQTTManager
    {
       
    }

    public class MQTTManager: IMQTTManager, IDisposable
    {
        private readonly IQueueManager _QueueManager;
        private readonly IMessageManager _MessageManager;
        private readonly ILogger<MQTTManager> _logger;

        //- 1. 宣告 MQTT 實體物件 
        private  IMqttClient client = new MqttFactory().CreateMqttClient();

        //--2. Setting MQTT Topic from ini setting
        private  Dictionary<string, string> dic_MQTT_Basic = null;
        private  Dictionary<string, string> dic_MQTT_Recv = null;
        private  Dictionary<string, string> dic_MQTT_Send = null;
        private Thread _sendTask;
        private int _TaskSleepPeriodMs = 50;
        private bool _IsRunning = true;

        // 傳入本體使用  IServiceProvider service,
        public MQTTManager(ILoggerFactory loggerFactory, IMessageManager MessageManage, IQueueManager QueueManager)
        {
            _logger = loggerFactory.CreateLogger<MQTTManager>();
            _QueueManager = QueueManager;
            _MessageManager = MessageManage;
            string Config_Path = AppContext.BaseDirectory + "/settings/Setting.xml";
            Load_Xml_Config_To_Dict(Config_Path);

            try
            {
                
                _logger.LogInformation("Connecting MQTT Broker ...");
                _logger.LogInformation("IP/Port: " + dic_MQTT_Basic["BrokerIP"] + "/" + dic_MQTT_Basic["BrokerPort"]);
                _logger.LogInformation("ClientID: " + dic_MQTT_Basic["ClinetID"]);

                
                var options = new MqttClientOptionsBuilder()
                    .WithClientId(dic_MQTT_Basic["ClinetID"])
                    .WithTcpServer(dic_MQTT_Basic["BrokerIP"], Convert.ToInt32(dic_MQTT_Basic["BrokerPort"]))
                    .Build();

                //- 1. setting receive topic defect # for all
                client.Connected += async (s, e) =>
                {
                    foreach (KeyValuePair<string, string> kvp in dic_MQTT_Recv)
                    {
                        _logger.LogInformation("Subscribe Topic : " + kvp.Value.ToString());
                        await client.SubscribeAsync(new TopicFilterBuilder().WithTopic(kvp.Value.ToString()).WithAtMostOnceQoS().Build());
                    }
                    _logger.LogInformation("MQTT Connect / Initial successful");
                };

                //- 2. if disconnected try to re-connect 
                client.Disconnected += async (s, e) =>
                {
                    _logger.LogWarning("Disconnecting MQTT Broker");
                    _logger.LogWarning("Waitting 10 Seconds Try to Reconnect.");
                    await System.Threading.Tasks.Task.Delay(TimeSpan.FromSeconds(10));
                    try
                    {
                        _logger.LogWarning("Reconnectint MQTT Broker.");
                        await client.ConnectAsync(options);
                    }
                    catch
                    {
                        _logger.LogError("Reconnecting MQTT Broker Failed.");
                    }
                };
                client.ConnectAsync(options);

                //- 3. receive 委派到 client_PublishArrived 
                client.ApplicationMessageReceived += client_PublishArrived;

                //----------Setting Thread Task for SendOut MQTT -----
                this._sendTask = new Thread(new ThreadStart(SendTaskProc));
                this._sendTask.IsBackground = true;
                this._sendTask.Start();
            }

            catch (Exception ex)
            {
                _logger.LogError("Initial MQTTManager Failed, ex : " + ex.Message);

            }

        }

        public void Dispose()
        {

            this.client.DisconnectAsync();
            this.client = null;
            _IsRunning = false;
        }

        private void Load_Xml_Config_To_Dict(string config_path)
        {
            XElement SettingFromFile = XElement.Load(config_path);
            XElement MQTT_Setting = SettingFromFile.Element("MQTT");
            XElement Basic_Setting = MQTT_Setting.Element("Basic_Setting");
            XElement Receive_Topic = MQTT_Setting.Element("Receive_Topic");
            XElement Send_Topic = MQTT_Setting.Element("Send_Topic");

            dic_MQTT_Basic = new Dictionary<string, string>();
            dic_MQTT_Recv = new Dictionary<string, string>();
            dic_MQTT_Send = new Dictionary<string, string>();

            if (Basic_Setting != null)
            {
                dic_MQTT_Basic.Clear();
                foreach (var el in Basic_Setting.Elements())
                {
                    dic_MQTT_Basic.Add(el.Name.LocalName, el.Value);
                }
            }

            if (Receive_Topic != null)
            {
                foreach (var el in Receive_Topic.Elements())
                {
                    dic_MQTT_Recv.Add(el.Name.LocalName, el.Value);
                }
            }

            if (Send_Topic != null)
            {
                foreach (var el in Send_Topic.Elements())
                {
                    dic_MQTT_Send.Add(el.Name.LocalName, el.Value);
                }
            }
        }

        private  string GetSubscribeAliasTopic(string Topic)
        {
            string AliasTopic = Topic;
            string ReturnAliasTopic = string.Empty;
            string[] tmpTopic = Topic.Split('/');
            List<string> Compare = new List<string>();
            int IndexLimit = 0;

            foreach (KeyValuePair<string, string> kvp in dic_MQTT_Recv)
            {
                Compare.Clear();
                string[] tmpSource = kvp.Value.Split('/');
                if (tmpSource[tmpSource.Length - 1] != "#" && tmpSource.Length < tmpTopic.Length)
                    continue;

                IndexLimit = Math.Min(tmpSource.Length, tmpTopic.Length);
                for (int i = 0; i < IndexLimit; i++)
                {
                    if (tmpSource[i] == "")
                        continue;

                    if (tmpSource[i] == tmpTopic[i])
                    {
                        Compare.Add(tmpSource[i]);
                    }
                    if (tmpSource[i] == "+")
                    {
                        Compare.Add(tmpSource[i]);
                    }
                    if (tmpSource[i] == "#")
                    {
                        Compare.Add(tmpSource[i]);
                        break;
                    }
                }
                string CompareResult = "/" + String.Join("/", Compare).ToString();
                if (kvp.Value.ToString() == CompareResult)
                {
                    AliasTopic = kvp.Value.ToString();
                    break;
                }

            }

            ReturnAliasTopic = AliasTopic;
            return ReturnAliasTopic;
        }

        // ---------- Handle MQTT Subscribe
        private void client_PublishArrived(object sender, MqttApplicationMessageReceivedEventArgs e)
        {

            string topic = e.ApplicationMessage.Topic;
            string payload = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);
            string message = GetSubscribeAliasTopic(topic);

            _logger.LogTrace(string.Format("Receive MQTT Topic = {0}.", topic));
            _logger.LogTrace(string.Format("Receive MQTT Payload = {0}.", payload));

            xmlMessage Message = new xmlMessage();
            Message.MQTTTopic = topic;
            Message.MQTTPayload = payload;
            _MessageManager.MessageDispatch(message, new object[] { Message });

        }

        private void SendTaskProc()
        {
            int count = 0;
            try
            {
                while (_IsRunning)
                {
                    try
                    {
                        count++;
                        DoSendProc();
                        int queue_deap = _QueueManager.GetCount();
                        if (queue_deap <= 0)
                        {
                            Thread.Sleep(this._TaskSleepPeriodMs);
                            continue;
                        }

                        if (count > int.MaxValue)
                        {
                            count = 0;
                            Thread.Sleep(this._TaskSleepPeriodMs);
                        }

                    }
                    catch { }
                }

            }
            catch { }
        }

        private void DoSendProc()
        {
            xmlMessage msg = _QueueManager.GetMessage();
            if (msg != null)
            {
                string Messsage = msg.MQTTPayload as string;
                string Topic = GetPulishTopic(msg);
                try
                {
                    if (Topic != string.Empty)
                    {
                        client_Publish_To_Broker(Topic, Messsage);
                    }
                    else
                    {
                        _logger.LogError(string.Format("Message Topic is Empty , ToMQTTTopicKey = {0}.", msg.MQTTTopic));
                       
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(string.Format("Exception Error msg = {0}.", ex.Message));
                }
            }
        }

        public void client_Publish_To_Broker(string Topic, string Payload)
        {
            if (client.IsConnected == true)
            {
                var message = new MqttApplicationMessageBuilder()
                    .WithTopic(Topic)
                    .WithPayload(Payload)
                    .Build();
                client.PublishAsync(message);
            }
        }

        private string GetPulishTopic(xmlMessage msg)
        {

            string topic = string.Empty;
            if (dic_MQTT_Send.ContainsKey(msg.MQTTTopic))
            {
                topic = dic_MQTT_Send[msg.MQTTTopic];
            }

            return topic.Replace("{LineID}", msg.LineID).Replace("{DeviceID}", msg.DeviceID); ;
        }


    }
}

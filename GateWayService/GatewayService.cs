using System;
using Kernel.Interface;
using Kernel.Common;
using Kernel.QueueManager;
using ObjectManager;
using System.Collections.Concurrent;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Newtonsoft.Json;



namespace GatewayService
{
    public class GatewayService : IService, IDisposable
    {
        private ConcurrentQueue<cls_ProcRecv_CollectData> _Update_ProcRecv_CollectData_Queue = new ConcurrentQueue<cls_ProcRecv_CollectData>();
        public delegate void Put_ProcRecv_CollectData_Event(cls_ProcRecv_CollectData obj_CollectData);

        public Put_ProcRecv_CollectData_Event Put_ProcRecv_CollectData;

        private string _SeviceName = "GatewayService";
        public string ServiceName
        {
            get
            {
                return this._SeviceName;
            }

        }

        private readonly IQueueManager _QueueManager;
        private readonly IManagement _ObjectManager;
        private readonly ILogger<GatewayService> _logger;

        private ObjectManager.ObjectManager _objectmanager = null;

        private Thread _th_Proc_Flow =null;
        private System.Threading.Timer timer_heartbet;
        private bool _run = false;



        public GatewayService(ILoggerFactory loggerFactory,IServiceProvider serviceProvider)
        {
            _QueueManager = serviceProvider.GetService<IQueueManager>();
            _ObjectManager = serviceProvider.GetServices<IManagement>().Where(o => o.ManageName.Equals("ObjectManager")).FirstOrDefault();
            _logger = loggerFactory.CreateLogger<GatewayService>();
        }


        public void Init()
        {
            _objectmanager = (ObjectManager.ObjectManager)_ObjectManager.GetInstance;
            
            this._run = true;
            this._th_Proc_Flow = new Thread(new ThreadStart(Proc_Flow));
            this._th_Proc_Flow.Start();

            Timer_Report_HeartBet(60);


        }

        public void Dispose()
        {
            _run = false;
        }


        private void Timer_Report_HeartBet(int interval)
        {
            if (interval == 0)
                interval = 10000;  // 10s

            //使用匿名方法，建立帶有參數的委派
            System.Threading.Thread Thread_Timer_Report_EDC = new System.Threading.Thread
            (
               delegate (object value)
               {
                   int Interval = Convert.ToInt32(value);
                   timer_heartbet = new System.Threading.Timer(new System.Threading.TimerCallback(HeartBet_TimerTask), null, 1000, Interval);
               }
            );
            Thread_Timer_Report_EDC.Start(interval);
        }

        private void HeartBet_TimerTask(object timerState)
        {
            try
            {
               

               
            }
            catch (Exception ex)
            {
               
            }
        }



        public void Proc_Flow()
        {
            while (_run)
            {
                
                if (_Update_ProcRecv_CollectData_Queue.Count > 0)
                {
                    cls_ProcRecv_CollectData _msg = null;
                    while (_Update_ProcRecv_CollectData_Queue.TryDequeue(out _msg))
                    {
                        string Gateway_ID = _msg.GateWayID;
                        string Device_ID = _msg.Device_ID;
                       
                        // Update Normal Tag
                        UpdateGatewayTagInfo(_msg);

                        // Update Calculate Tag
                        // ProcCalcTag(Gateway_ID, Device_ID);

                        // 結合EDC and other Information 送MQTT
                        Organize_EDCPartaker(Gateway_ID, Device_ID);
                    }
                }
                Thread.Sleep(10);
            }
        }


        public void UpdateGatewayTagInfo(cls_ProcRecv_CollectData ProcData)
        {
            _objectmanager.GatewayManager_Set_TagValue(ProcData);
        }


        public void Organize_EDCPartaker(string GateWayID, string Device_ID)
        {
            //--- 等待EDC List information 

            if (_objectmanager.EDCManager == null)
                return;

            List<ObjectManager.cls_EDC_Info> lst_EDCInfo = _objectmanager.EDCManager.gateway_edc.Where(p => p.gateway_id == GateWayID && p.device_id == Device_ID && p.enable == true).ToList();

            foreach (cls_EDC_Info _EDC in lst_EDCInfo)
            {
                EDCPartaker EDCReporter = new EDCPartaker(_EDC);
                EDCReporter.timestapm = DateTime.Now;

                cls_Gateway_Info gateway = _objectmanager.GatewayManager.gateway_list.Where(p => p.gateway_id == GateWayID).FirstOrDefault();
                if (gateway != null)
                {
                    cls_Device_Info device = gateway.device_info.Where(p => p.device_name == Device_ID).FirstOrDefault();
                    if (device != null)
                    {
                        // Assembly Normal Tag info
                        foreach (Tuple<string, string> _Items in _EDC.tag_info)
                        {
                            cls_EDC_Body_Item edctiem = new cls_EDC_Body_Item();
                            edctiem.item_name = _Items.Item1;
                            edctiem.item_type = "X";
                            if (device.tag_info.ContainsKey(_Items.Item2))
                                edctiem.item_value = device.tag_info[_Items.Item2].Value;
                            else
                                edctiem.item_value = string.Empty;

                            EDCReporter.edcitem_info.Add(edctiem);
                        }

                        // Assembly Calc Tag info
                        foreach (Tuple<string, string> _Items in _EDC.calc_tag_info)
                        {
                            cls_EDC_Body_Item edctiem = new cls_EDC_Body_Item();
                            edctiem.item_name = _Items.Item1;
                            edctiem.item_type = "X";

                            if (device.calc_tag_info.ContainsKey(_Items.Item2))
                                edctiem.item_value = device.calc_tag_info[_Items.Item2].Value;
                            else
                                edctiem.item_value = string.Empty;

                            EDCReporter.edcitem_info.Add(edctiem);
                        }
                    }
                }

                //----- Send MQTT-----
                xmlMessage SendOutMsg = new xmlMessage();
                SendOutMsg.LineID = GateWayID;     // GateID
                SendOutMsg.DeviceID = Device_ID;   // DeviceID
                SendOutMsg.MQTTTopic = "EDCService";
                SendOutMsg.MQTTPayload = JsonConvert.SerializeObject(EDCReporter, Newtonsoft.Json.Formatting.Indented);
                _QueueManager.PutMessage(SendOutMsg);
            }
        }



        public void _Update_ProcRecv_CollectData(cls_ProcRecv_CollectData obj_CollectData)
        {
            this._Update_ProcRecv_CollectData_Queue.Enqueue(obj_CollectData);
        }

        public void ReceiveMQTTData(xmlMessage InputData)
        {
            // Parse Mqtt Topic
            string[] Topic = InputData.MQTTTopic.Split('/');    // /IEW/GateWay/Device/ReplyData
            string GateWayID = Topic[2].ToString();
            string DeviceID = Topic[3].ToString();

            _logger.LogInformation(string.Format("GateWay Service Handle Receive Data GateWayID = {0}, DeviceID = {1}, Topic = {2}.", GateWayID, DeviceID, InputData.MQTTTopic));
            _logger.LogDebug(string.Format("GateWay Service Handle Receive Data  Topic = {0}, Payload = {1}.",  InputData.MQTTTopic, InputData.MQTTPayload));

            if (_objectmanager.GatewayManager != null)
            {
                cls_Gateway_Info Gateway = _objectmanager.GatewayManager.gateway_list.Where(p => p.gateway_id == GateWayID).FirstOrDefault();
                if (Gateway != null)
                {
                    cls_Device_Info Device = Gateway.device_info.Where(p => p.device_name == DeviceID).FirstOrDefault();
                    if (Device != null)
                    {
                        try
                        {
                            ProcCollectData Function = new ProcCollectData(Device, GateWayID, DeviceID, _Update_ProcRecv_CollectData);
                            ThreadPool.QueueUserWorkItem(o => Function.ThreadPool_Proc(InputData.MQTTPayload.ToString()));
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError("Gateway Services Handle Receive MQTT Message Error :" + ex.Message);
                           
                        }
                    }
                }
            }

        }

    }
}

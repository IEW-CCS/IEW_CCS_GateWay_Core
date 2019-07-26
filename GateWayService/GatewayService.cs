using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Kernel.Interface;
using Kernel.Common;
using Kernel.QueueManager;
using System.Xml.Linq;
using ObjectManager;

namespace GatewayService
{
    public class GatewayService : IService, IDisposable
    {
        private string _SeviceName = "GatewayService";
        public string ServiceName
        {
            get
            {
                return this._SeviceName;
            }

        }

        public delegate void Put_ProcRecv_CollectData_Event(cls_ProcRecv_CollectData obj_CollectData);
        private ConcurrentQueue<cls_ProcRecv_CollectData> _Update_ProcRecv_CollectData_Queue = new ConcurrentQueue<cls_ProcRecv_CollectData>();

        public Put_ProcRecv_CollectData_Event Put_ProcRecv_CollectData;

        // For IOC/DI Used
        private readonly IQueueManager _QueueManager;
        private readonly IManagement _ObjectManager;
        private readonly ILogger<GatewayService> _logger;

        private ObjectManager.ObjectManager _objectmanager = null;

        private Thread _th_Proc_Flow =null;

        private System.Threading.Timer timer_routine_job;
        private bool _run = false;

        private ConcurrentDictionary<string, List<string>> _Wait_Function_Config_Reply = new ConcurrentDictionary<string, List<string>>();

        private Dictionary<string, string> _dic_Basicsetting = new Dictionary<string, string>();

        public string GateWayStatus = "Init";

        private string _Version = "1.0.0";

        private const string _GatewayID = "GatewayID";


        public GatewayService(ILoggerFactory loggerFactory,IServiceProvider serviceProvider)
        {
            _QueueManager = serviceProvider.GetService<IQueueManager>();
            _ObjectManager = serviceProvider.GetServices<IManagement>().Where(o => o.ManageName.Equals("ObjectManager")).FirstOrDefault();
            _logger = loggerFactory.CreateLogger<GatewayService>();
        }

        public void Init()
        {
            try
            {
                _objectmanager = (ObjectManager.ObjectManager)_ObjectManager.GetInstance;

                string Config_Path = AppContext.BaseDirectory + "/settings/System.xml";
                Load_Xml_Config_To_Dict(Config_Path);


                this._run = true;
                this._th_Proc_Flow = new Thread(new ThreadStart(Proc_Flow));
                this._th_Proc_Flow.Start();
                _logger.LogTrace("GateWay Service Initial Finished");

                GateWayStatus = "Init";


                Timer_Routine_Job(_dic_Basicsetting["HeartBeat_Interval"]);  //execute routine job 
            }
            catch (Exception ex)
            {
                Dispose();
                _logger.LogTrace("EDC Service Initial Faild, Exception Msg = " + ex.Message);
            }

        }

        public void Dispose()
        {
            _run = false;
        }

        private void Load_Xml_Config_To_Dict(string config_path)
        {
            XElement SettingFromFile = XElement.Load(config_path);
            XElement System_Setting = SettingFromFile.Element("system");

            _dic_Basicsetting.Clear();

            if (System_Setting != null)
            {
                foreach (var el in System_Setting.Elements())
                {
                    _dic_Basicsetting.Add(el.Name.LocalName, el.Value);
                }
            }
        }


        #region for Routine Job Used
        private void Timer_Routine_Job(string interval)
        {
            int reportInterval = 10000;
            int temp = 0;

            if (Int32.TryParse(interval, out temp))
            {

                reportInterval = temp * 1000;
            }
            else
            {
                reportInterval = 10000;
            }


            System.Threading.Thread Thread_Timer_Report_EDC = new System.Threading.Thread
            (
               delegate (object value)
               {
                   int Interval = Convert.ToInt32(value);
                   timer_routine_job = new System.Threading.Timer(new System.Threading.TimerCallback(Routine_TimerTask), null, 1000, Interval);
               }
            );
            Thread_Timer_Report_EDC.Start(reportInterval);
        }
        private void Routine_TimerTask(object timerState)
        {
            try
            {
                xmlMessage SendOutMsg = new xmlMessage();
                SendOutMsg.GatewayID = _dic_Basicsetting[_GatewayID];     // GateID
                SendOutMsg.DeviceID = _dic_Basicsetting[_GatewayID];
                SendOutMsg.MQTTTopic = "HeartBeat";
                SendOutMsg.MQTTPayload = JsonConvert.SerializeObject(new { Trace_ID = DateTime.Now.ToString("yyyyMMddHHmmssfff"),Status = GateWayStatus }, Formatting.Indented);
                _QueueManager.PutMessage(SendOutMsg);
                _logger.LogInformation("Report HeartBeat G/W Status = " + GateWayStatus);

            }
            catch (Exception ex)
            {
               
            }
        }
        #endregion

        #region Receive Config 
        // 1. Start Receive MQTT Config 



        public void ReceiveConfigEvent(xmlMessage InputData)
        {
            GateWayStatus = "Conf";
            // / IEW / gateway / device / Cmd / Config
            string[] Topic = InputData.MQTTTopic.Split('/');
            string GateWayID = Topic[2].ToString();
            string DeviceID = Topic[3].ToString();

            _logger.LogInformation(string.Format("GateWay Service Handle ConfigEvent GateWayID = {0}, DeviceID = {1}, Topic = {2}.", GateWayID, DeviceID, InputData.MQTTTopic));
            _logger.LogDebug(string.Format("GateWay Service Handle ConfigEvent Topic = {0}, Payload = {1}.", InputData.MQTTTopic, InputData.MQTTPayload));

            try
            {
                _objectmanager.GatewayManager_Config(GateWayID, DeviceID, InputData.MQTTPayload);
                var gateway = _objectmanager.GatewayManager.gateway_list.Where(o => o.gateway_id.Equals(GateWayID)).FirstOrDefault();
                if (gateway != null)
                {
                    string WaitReplyQueue_Key = string.Concat(GateWayID, "_", DeviceID);
                    List<string> WaitReplyQueue = this._Wait_Function_Config_Reply.GetOrAdd(WaitReplyQueue_Key, new List<string>());
                    WaitReplyQueue.Clear();

                    foreach (string function in gateway.function_list)
                    {
                        xmlMessage SendOutMsg = new xmlMessage();
                        SendOutMsg.GatewayID = GateWayID;     // GateID
                        SendOutMsg.DeviceID = DeviceID;   // DeviceID
                        SendOutMsg.MQTTTopic = string.Concat(function, "_Config_Query");
                        SendOutMsg.MQTTPayload = JsonConvert.SerializeObject(new { Trace_ID = DateTime.Now.ToString("yyyyMMddHHmmssfff") }, Formatting.Indented);
                        _QueueManager.PutMessage(SendOutMsg);
                        WaitReplyQueue.Add(function);
                        _logger.LogDebug(string.Format("Send Function_Config_Query {0}, Function :{1}", WaitReplyQueue_Key, function));
                    }
                    this._Wait_Function_Config_Reply.AddOrUpdate(WaitReplyQueue_Key, WaitReplyQueue, (key, oldvalue) => WaitReplyQueue);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Gateway Services Handle ConfigEvent Error Message :" + ex.Message);
                SendConfigAckEvent(GateWayID, DeviceID, "NG");


            }
        }

        public void SendConfigAckEvent(string GateWayID, string DeviceID, string _Cmd_Result)
        {
            xmlMessage SendOutMsg = new xmlMessage();
            SendOutMsg.GatewayID = GateWayID;     // GateID
            SendOutMsg.DeviceID = DeviceID;   // DeviceID
            SendOutMsg.MQTTTopic = "Config_Ack";
            SendOutMsg.MQTTPayload = JsonConvert.SerializeObject(new { Cmd_Result = _Cmd_Result.ToString(), Trace_ID = DateTime.Now.ToString("yyyyMMddHHmmssfff") }, Formatting.Indented);
            _QueueManager.PutMessage(SendOutMsg);
            GateWayStatus = "Ready";

        }

        public void Check_Wait_Function_Config_Reply(string GateWayID, string DeviceID)
        {
            string WaitReplyQueue_Key = string.Concat(GateWayID, "_", DeviceID);
            List<string> WaitReplyQueue = this._Wait_Function_Config_Reply.GetOrAdd(WaitReplyQueue_Key, new List<string>());
            List<string> removequeue = null; 
            if (WaitReplyQueue.Count() != 0)
            {
                return;
            }
            else
            {
                SendConfigAckEvent(GateWayID, DeviceID, "OK");
                this._Wait_Function_Config_Reply.TryRemove(WaitReplyQueue_Key, out removequeue);
                _logger.LogInformation(string.Format("GateWay Service Send Config_Ack Event GateWayID = {0}, DeviceID = {1}.", GateWayID, DeviceID));
            }

        }

        public void ReceiveDBConfigEvent(xmlMessage InputData)
        {
            // / IEW / gateway / device / Cmd / Config
            string[] Topic = InputData.MQTTTopic.Split('/');
            string GateWayID = Topic[2].ToString();
            string DeviceID = Topic[3].ToString();
            string FunctionKey = "DB";

            _logger.LogInformation(string.Format("GateWay Service Handle ConfigEvent GateWayID = {0}, DeviceID = {1}, Topic = {2}.", GateWayID, DeviceID, InputData.MQTTTopic));
            _logger.LogDebug(string.Format("GateWay Service Handle ConfigEvent Topic = {0}, Payload = {1}.", InputData.MQTTTopic, InputData.MQTTPayload));

            try
            {
                _objectmanager.DBManager_Config(GateWayID, DeviceID, InputData.MQTTPayload);

                string WaitReplyQueue_Key = string.Concat(GateWayID, "_", DeviceID);
                List<string> WaitReplyQueue = this._Wait_Function_Config_Reply.GetOrAdd(WaitReplyQueue_Key, new List<string>());
                WaitReplyQueue.Remove(FunctionKey);
                this._Wait_Function_Config_Reply.AddOrUpdate(WaitReplyQueue_Key, WaitReplyQueue, (key, oldvalue) => WaitReplyQueue);
                Check_Wait_Function_Config_Reply(GateWayID, DeviceID);
            }
            catch (Exception ex)
            {
                _logger.LogError("Gateway Services Handle DB Config Event Error Message :" + ex.Message);
                SendConfigAckEvent(GateWayID, DeviceID, "NG");
            }

        }

        public void ReceiveEDCConfigEvent(xmlMessage InputData)
        {
            // /IEW / gateway / device / Cmd / Config / EDC / Reply
            string[] Topic = InputData.MQTTTopic.Split('/');
            string GateWayID = Topic[2].ToString();
            string DeviceID = Topic[3].ToString();
            string FunctionKey = "EDC";

            _logger.LogInformation(string.Format("GateWay Service Handle ConfigEvent GateWayID = {0}, DeviceID = {1}, Topic = {2}.", GateWayID, DeviceID, InputData.MQTTTopic));
            _logger.LogDebug(string.Format("GateWay Service Handle ConfigEvent Topic = {0}, Payload = {1}.", InputData.MQTTTopic, InputData.MQTTPayload));

            try
            {
                _objectmanager.EDCManager_Config(GateWayID, DeviceID, InputData.MQTTPayload);

                string WaitReplyQueue_Key = string.Concat(GateWayID, "_", DeviceID);
                List<string> WaitReplyQueue = this._Wait_Function_Config_Reply.GetOrAdd(WaitReplyQueue_Key, new List<string>());
                WaitReplyQueue.Remove(FunctionKey);
                this._Wait_Function_Config_Reply.AddOrUpdate(WaitReplyQueue_Key, WaitReplyQueue, (key, oldvalue) => WaitReplyQueue);
                Check_Wait_Function_Config_Reply(GateWayID, DeviceID);
            }
            catch (Exception ex)
            {
                _logger.LogError("Gateway Services Handle EDC Config Event Error Message :" + ex.Message);
                SendConfigAckEvent(GateWayID, DeviceID, "NG");
            }

        }

        #endregion


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
                        ProcCalcTag(Gateway_ID, Device_ID);

                        // 結合EDC and other Information 送MQTT
                        Organize_EDCPartaker(Gateway_ID, Device_ID);

                        // 結合DB Information 送MQTT
                        Organize_DBPartaker(Gateway_ID, Device_ID);

                    }
                }
                Thread.Sleep(10);
            }
        }

        public void UpdateGatewayTagInfo(cls_ProcRecv_CollectData ProcData)
        {
            _objectmanager.GatewayManager_Set_TagValue(ProcData);
        }

        public void ProcCalcTag(string GateWayID, string Device_ID)
        {
          
            cls_Gateway_Info gateway = _objectmanager.GatewayManager.gateway_list.Where(p => p.gateway_id == GateWayID).FirstOrDefault();
            if (gateway != null)
            {
                cls_Device_Info device = gateway.device_info.Where(p => p.device_name == Device_ID).FirstOrDefault();
                if (device != null)
                {
                    foreach (KeyValuePair<string, cls_CalcTag> kvp in device.calc_tag_info)
                    {
                        cls_Tag tagA = null;
                        cls_Tag tagB = null;
                        cls_Tag tagC = null;
                        cls_Tag tagD = null;
                        cls_Tag tagE = null;
                        cls_Tag tagF = null;
                        cls_Tag tagG = null;
                        cls_Tag tagH = null;

                        Double douA = -1;
                        Double douB = -1;
                        Double douC = -1;
                        Double douD = -1;
                        Double douE = -1;
                        Double douF = -1;
                        Double douG = -1;
                        Double douH = -1;
                        Double douResult = -999999.999;

                        if (kvp.Value.ParamA.Trim() != "")
                        {
                            // for vic verify dictionary key exist or not.
                            if (device.tag_info.ContainsKey(kvp.Value.ParamA))
                                tagA = device.tag_info[kvp.Value.ParamA];
                        }

                        if (kvp.Value.ParamB.Trim() != "")
                        {
                            tagB = device.tag_info[kvp.Value.ParamB];
                        }

                        if (kvp.Value.ParamC.Trim() != "")
                        {
                            tagC = device.tag_info[kvp.Value.ParamC];
                        }

                        if (kvp.Value.ParamD.Trim() != "")
                        {
                            tagD = device.tag_info[kvp.Value.ParamD];
                        }

                        if (kvp.Value.ParamE.Trim() != "")
                        {
                            tagE = device.tag_info[kvp.Value.ParamE];
                        }

                        if (kvp.Value.ParamF.Trim() != "")
                        {
                            tagF = device.tag_info[kvp.Value.ParamF];
                        }

                        if (kvp.Value.ParamG.Trim() != "")
                        {
                            tagG = device.tag_info[kvp.Value.ParamG];
                        }

                        if (kvp.Value.ParamH.Trim() != "")
                        {
                            tagH = device.tag_info[kvp.Value.ParamH];
                        }

                        try
                        {
                            douA = Convert.ToDouble(tagA.Value);
                            douB = Convert.ToDouble(tagB.Value);
                            douC = Convert.ToDouble(tagC.Value);
                            douD = Convert.ToDouble(tagD.Value);
                            douE = Convert.ToDouble(tagE.Value);
                            douF = Convert.ToDouble(tagF.Value);
                            douG = Convert.ToDouble(tagG.Value);
                            douH = Convert.ToDouble(tagH.Value);

                            ExpressionCalculator exp_calc = new ExpressionCalculator(kvp.Value.Expression, douA, douB, douC, douD, douE, douF, douG, douH);
                            douResult = exp_calc.Evaluate();
                            kvp.Value.Value = douResult.ToString();

                        }
                        catch (Exception ex)
                        {
                            douResult = -999999.999;
                        }
                    }
                }
            }
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
                            cls_EDC_Body_Item edcitem = new cls_EDC_Body_Item();
                            edcitem.item_name = _Items.Item1;
                            edcitem.item_type = "X";

                            if (device.tag_info.ContainsKey(_Items.Item2))
                            {
                                edcitem.item_value = device.tag_info[_Items.Item2].Value;
                                edcitem.orig_item_type = device.tag_info[_Items.Item2].Expression;
                            }
                            else
                                edcitem.item_value = string.Empty;

                            EDCReporter.edcitem_info.Add(edcitem);
                        }

                        // Assembly Calc Tag info
                        foreach (Tuple<string, string> _Items in _EDC.calc_tag_info)
                        {
                            cls_EDC_Body_Item edcitem = new cls_EDC_Body_Item();
                            edcitem.item_name = _Items.Item1;
                            edcitem.item_type = "X";

                            if (device.calc_tag_info.ContainsKey(_Items.Item2))
                            {
                                edcitem.item_value = device.calc_tag_info[_Items.Item2].Value;
                                edcitem.orig_item_type = "DOUBLE";
                            }
                            else
                                edcitem.item_value = string.Empty;

                            EDCReporter.edcitem_info.Add(edcitem);
                        }
                    }
                }

                //----- Send MQTT-----
                xmlMessage SendOutMsg = new xmlMessage();
                SendOutMsg.GatewayID = GateWayID;     // GateID
                SendOutMsg.DeviceID = Device_ID;   // DeviceID
                SendOutMsg.MQTTTopic = "EDCService";
                SendOutMsg.MQTTPayload = JsonConvert.SerializeObject(EDCReporter, Newtonsoft.Json.Formatting.Indented);
                _QueueManager.PutMessage(SendOutMsg);

               
            }
        }

        public void Organize_DBPartaker(string GateWayID, string Device_ID)
        {
            //--- Check DB List information 
            if (_objectmanager.DBManager == null)
                return;

            List<ObjectManager.cls_DB_Info> lst_DBInfo = _objectmanager.DBManager.dbconfig_list.Where(p => p.gateway_id == GateWayID && p.device_id == Device_ID && p.enable == true).ToList();

            foreach (cls_DB_Info _DB in lst_DBInfo)
            {
                DBPartaker DBReporter = new DBPartaker(_DB);
                cls_Gateway_Info gateway = _objectmanager.GatewayManager.gateway_list.Where(p => p.gateway_id == GateWayID).FirstOrDefault();
                if (gateway != null)
                {
                    cls_Device_Info device = gateway.device_info.Where(p => p.device_name == Device_ID).FirstOrDefault();
                    if (device != null)
                    {
                        // Assembly Normal Tag info
                        foreach (Tuple<string, string> _Items in _DB.tag_info)
                        {
                            string ReportItemName = _Items.Item1;
                            string ReportItemValue = string.Empty;
                            if (device.tag_info.ContainsKey(_Items.Item2))
                            {
                                ReportItemValue = device.tag_info[_Items.Item2].Value;
                                DBReporter.Report_Item.Add(Tuple.Create(ReportItemName, ReportItemValue, device.tag_info[_Items.Item2].Expression));
                            }

                           
                        }

                        // Assembly Calc Tag info
                        foreach (Tuple<string, string> _Items in _DB.calc_tag_info)
                        {
                            string ReportItemName = _Items.Item1;
                            string ReportItemValue = string.Empty;
                            if (device.tag_info.ContainsKey(_Items.Item2))
                            {
                                ReportItemValue = device.tag_info[_Items.Item2].Value;
                                DBReporter.Report_Item.Add(Tuple.Create(ReportItemName, ReportItemValue, device.tag_info[_Items.Item2].Expression));
                            }
                           
                        }
                    }
                }

                //----- Send MQTT-----
                xmlMessage SendOutMsg = new xmlMessage();
                SendOutMsg.GatewayID = GateWayID;     // GateID
                SendOutMsg.DeviceID = Device_ID;    // DeviceID
                SendOutMsg.MQTTTopic = "DBService";
                SendOutMsg.MQTTPayload = JsonConvert.SerializeObject(DBReporter, Newtonsoft.Json.Formatting.Indented);
                _QueueManager.PutMessage(SendOutMsg);

            }
        }


        public void _Update_ProcRecv_CollectData_Enqueue(cls_ProcRecv_CollectData obj_CollectData)
        {
            this._Update_ProcRecv_CollectData_Queue.Enqueue(obj_CollectData);
        }

        public void Receive_TEXOL_MQTT(xmlMessage InputData)
        {
            if (GateWayStatus != "Run" && GateWayStatus != "Ready")
                return;

            string receive_topic = InputData.MQTTTopic;
            if (_objectmanager.GatewayManager != null)
            {
                List<cls_Gateway_Info> Gateways = _objectmanager.GatewayManager.gateway_list.Where(p => p.virtual_flag==true && p.virtual_publish_topic.Equals(receive_topic)).ToList();
                if (Gateways.Count == 0)
                    return;
                else
                {
                    foreach (cls_Gateway_Info Gateway in Gateways)
                    {
                        try
                        {
                            Proc_TEXOL_Data Function = new Proc_TEXOL_Data(Gateway, _Update_ProcRecv_CollectData_Enqueue);
                            ThreadPool.QueueUserWorkItem(o => Function.ThreadPool_Proc(InputData.MQTTPayload.ToString()));
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError("Gateway Services Handle TEXOL Data Message Error :" + ex.Message);

                        }
                    }
                }
            }
        }

        public void ReceiveMQTTData(xmlMessage InputData)
        {
            if (GateWayStatus != "Run" && GateWayStatus != "Ready")
                return;
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
                             ProcCollectData Function = new ProcCollectData(Device, GateWayID, DeviceID, _Update_ProcRecv_CollectData_Enqueue);
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

        public void ReceiveReadDataEvent(xmlMessage InputData)
        {

            GateWayStatus = "Run";


        }

        public void ReceiveOTAEvent(xmlMessage InputData)
        {
            Process currentProcess = Process.GetCurrentProcess();
            string pid = currentProcess.Id.ToString();

            xmlMessage SendOutMsg = new xmlMessage();
            SendOutMsg.GatewayID = _dic_Basicsetting[_GatewayID]; ;     // GateID
            SendOutMsg.DeviceID = _dic_Basicsetting[_GatewayID]; ;   // DeviceID
            SendOutMsg.MQTTTopic = "OTA_Ack";

            SendOutMsg.MQTTPayload = JsonConvert.SerializeObject(new { Version = _Version, HBDatetime = DateTime.Now.ToString("yyyyMMddHHmmssfff"), Status = "OTA", ProcrssID = pid }, Formatting.Indented);
            _QueueManager.PutMessage(SendOutMsg);

            _logger.LogError("Gateway Services Handle OTA Process System Will be shutdown in 5 seconds.");

            Thread.Sleep(5000);


            System.Environment.Exit(System.Environment.ExitCode);

        }
    }
}

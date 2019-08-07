using System;
using System.Collections.Generic;
using System.Text;
using ObjectManager;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Linq;
    
namespace GatewayService
{
    class Proc_IR_Temperature_DataXOL_Data
    {

        private cls_Gateway_Info _Gateway = null;
        private cls_Device_Info _Device = null;
        private const string _LogName = "Service";

        public delegate void Put_ProcRecv_CollectData_Event(cls_ProcRecv_CollectData obj_CollectData);
        public Put_ProcRecv_CollectData_Event Put_ProcRecv_CollectData;

        public Proc_IR_Temperature_DataXOL_Data(cls_Gateway_Info Gateway, Put_ProcRecv_CollectData_Event _delegate_Method)
        {
            _Gateway = Gateway;
            this.Put_ProcRecv_CollectData = _delegate_Method;

        }
        public void ThreadPool_Proc(string msg)
        {
            try
            {

                JObject jsonObj = JObject.Parse(msg.ToString());
                Dictionary<string, object> dictObj = jsonObj.ToObject<Dictionary<string, object>>();

                if (dictObj.ContainsKey("MCUName") == false)
                    return;
                else
                {
                    cls_ProcRecv_CollectData ProcRecv_CollectData = new cls_ProcRecv_CollectData();
                    ProcRecv_CollectData.GateWayID = _Gateway.gateway_id;

                    cls_Device_Info Device = _Gateway.device_info.Where(p => p.device_name == dictObj["MCUName"].ToString()).FirstOrDefault();
                    if (Device == null)
                        return;
                    else
                    {
                        ProcRecv_CollectData.Device_ID = Device.device_name;

                        foreach (KeyValuePair<string, cls_Tag> kvp in Device.tag_info)
                        {
                            if (dictObj.ContainsKey(kvp.Value.TagName) == false)
                                continue;
                            else
                            {
                                kvp.Value.Value = dictObj[kvp.Value.TagName].ToString();
                                ProcRecv_CollectData.Prod_EDC_Data.Enqueue(Tuple.Create(kvp.Key, kvp.Value.Value));
                            }
                        }

                        this.Put_ProcRecv_CollectData(ProcRecv_CollectData);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Handle IR Sensor Data Error Message Error [{0}].", ex.Message));
            }
        }
    }
}


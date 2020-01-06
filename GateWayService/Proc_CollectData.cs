using System;
using System.Collections.Generic;
using System.Text;
using ObjectManager;
using Newtonsoft.Json;
using System.Threading.Tasks;
using System.Linq;


namespace GatewayService
{
    public class ProcCollectData
    {
        private string _GatewayID = string.Empty;
        private string _DeviceID = string.Empty;
        private cls_Device_Info _Device = null;
        private const string _LogName = "Service";

        private string[] CheckWordType = { "W", "D", "ZR" };
        public delegate void Put_ProcRecv_CollectData_Event(cls_ProcRecv_CollectData obj_CollectData);
        public Put_ProcRecv_CollectData_Event Put_ProcRecv_CollectData;

        public ProcCollectData(cls_Device_Info Device, string GatewayID, string DeviceID, Put_ProcRecv_CollectData_Event _delegate_Method)
        {
            _GatewayID = GatewayID;
            _DeviceID = DeviceID;
            _Device = Device;
            this.Put_ProcRecv_CollectData = _delegate_Method;

        }
        public void ThreadPool_Proc_Physical(string msg)
        {
            try
            {
                cls_read_data_reply CollectData = null;
                CollectData = JsonConvert.DeserializeObject<cls_read_data_reply>(msg.ToString());

                cls_ProcRecv_CollectData ProcRecv_CollectData = new cls_ProcRecv_CollectData();
                ProcRecv_CollectData.GateWayID = _GatewayID;
                ProcRecv_CollectData.Device_ID = _DeviceID;

                if (CollectData != null)
                {
                    // 直接平行處理對應所有Tag
                    Parallel.ForEach(CollectData.EDC_Data, p =>
                    {
                        cls_Tag Device_Tag = _Device.tag_info[p.DATA_NAME];
                        string Tag_Value = p.DATA_VALUE;
                        int BitPoints = 0;
                        double tmp_output = 0;
                        string Output = string.Empty;

                        if (Device_Tag.Type == "PLC")
                        {
                            if (CheckWordType.Any(x => Device_Tag.UUID_Address.Contains(x)))
                            {
                                if (Tag_Value.Length % 4 != 0)
                                    return;
                                switch (Device_Tag.Expression)
                                {
                                    case "BIT":
                                        BitPoints = CalcBitPoints(Device_Tag.UUID_Address);
                                        if (BitPoints != 0)
                                            Output = HexToBit(0, BitPoints, Tag_Value);
                                        else
                                            Output = string.Empty;
                                        break;

                                    case "UINT":
                                        BitPoints = 16;   // Int固定16 bit ;
                                        tmp_output = HexToInt(BitPoints, Tag_Value);
                                        tmp_output = (Device_Tag.scale * tmp_output) + Device_Tag.offset;
                                        Output = tmp_output.ToString();
                                        break;

                                    case "ULONG":
                                        BitPoints = 32;
                                        tmp_output = HexToLong(BitPoints, Tag_Value);
                                        tmp_output = (Device_Tag.scale * tmp_output) + Device_Tag.offset;
                                        Output = tmp_output.ToString();
                                        break;
                                    case "SINT":
                                        BitPoints = 16;
                                        tmp_output = HexToSInt(BitPoints, Tag_Value);
                                        tmp_output = (Device_Tag.scale * tmp_output) + Device_Tag.offset;
                                        Output = tmp_output.ToString();
                                        break;
                                    case "SLONG":
                                        BitPoints = 32;
                                        tmp_output = HexToSLong(BitPoints, Tag_Value);
                                        tmp_output = (Device_Tag.scale * tmp_output) + Device_Tag.offset;
                                        Output = tmp_output.ToString();
                                        break;

                                    case "ASC":
                                        Output = HexToASCII(Tag_Value);  //  目前Decode 全部都解析解析完再考慮長度
                                        break;
                                    default:
                                        Output = string.Empty;
                                        break;
                                }
                            }
                        }

                        if (Output != string.Empty)
                        {
                            ProcRecv_CollectData.Prod_EDC_Data.Enqueue(Tuple.Create(p.DATA_NAME, Output));
                        }

                    });
                }

                this.Put_ProcRecv_CollectData(ProcRecv_CollectData);

            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Handle ProcCollectData Message Error [{0}].", ex.Message));
            }
        }

        public void ThreadPool_Proc_Virtual(string msg)
        {
            try
            {
                cls_read_data_reply CollectData = null;
                CollectData = JsonConvert.DeserializeObject<cls_read_data_reply>(msg.ToString());

                cls_ProcRecv_CollectData ProcRecv_CollectData = new cls_ProcRecv_CollectData();
                ProcRecv_CollectData.GateWayID = _GatewayID;
                ProcRecv_CollectData.Device_ID = _DeviceID;

                if (CollectData != null)
                {
                    // 直接平行處理對應所有Tag
                    Parallel.ForEach(CollectData.EDC_Data, p =>
                    {
                       ProcRecv_CollectData.Prod_EDC_Data.Enqueue(Tuple.Create(p.DATA_NAME, p.DATA_VALUE));
                    });
                }

                this.Put_ProcRecv_CollectData(ProcRecv_CollectData);

            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Handle ProcCollectData Message Error [{0}].", ex.Message));
            }
        }

        // -----  decode method  -----

        public byte[] HexToByte(string hexString)
        {
            //運算後的位元組長度:16進位數字字串長/2
            byte[] byteOUT = new byte[hexString.Length / 2];
            for (int i = 0; i < hexString.Length; i = i + 2)
            {
                //每2位16進位數字轉換為一個10進位整數
                byteOUT[i / 2] = Convert.ToByte(hexString.Substring(i, 2), 16);
            }
            return byteOUT;
        }

        public ushort HexToInt(int itemBPoints, string iodata)
        {
            int itemBOffset = 0;
            ushort result = 0;
            byte[] buffer = new byte[8];  // Max long 4 word
            int bufOffset = 0;
            byte[] ary_bytiodata = HexToByte(iodata);
            int iodataOffset = 0;

            Buffer.BlockCopy(ary_bytiodata, iodataOffset, buffer, bufOffset, ary_bytiodata.Length);

            ulong[] value = new ulong[1];
            Buffer.BlockCopy(buffer, 0, value, 0, 8);
            result = (ushort)(value[0] >> (itemBOffset & 63) & (ulong)(Math.Pow(2, (double)itemBPoints) - 1));
            return result;
        }

        public uint HexToLong(int itemBPoints, string iodata)
        {
            int itemBOffset = 0;
            uint result = 0;
            byte[] buffer = new byte[8];  // Max long 4 word
            int bufOffset = 0;
            byte[] ary_bytiodata = HexToByte(iodata);
            int iodataOffset = 0;

            Buffer.BlockCopy(ary_bytiodata, iodataOffset, buffer, bufOffset, ary_bytiodata.Length);

            ulong[] value = new ulong[1];
            Buffer.BlockCopy(buffer, 0, value, 0, 8);
            result = (uint)(value[0] >> (itemBOffset & 63) & (ulong)(Math.Pow(2, (double)itemBPoints) - 1));
            return result;
        }

        public short HexToSInt(int itemBPoints, string iodata)
        {
            int itemBOffset = 0;
            short result = 0;
            byte[] buffer = new byte[8];  // Max long 4 word
            int bufOffset = 0;
            byte[] ary_bytiodata = HexToByte(iodata);
            int iodataOffset = 0;

            Buffer.BlockCopy(ary_bytiodata, iodataOffset, buffer, bufOffset, ary_bytiodata.Length);

            ulong[] value = new ulong[1];
            Buffer.BlockCopy(buffer, 0, value, 0, 8);
            result = (short)(value[0] >> (itemBOffset & 63) & (ulong)(Math.Pow(2, (double)itemBPoints) - 1));
            return result;
        }

        public int HexToSLong(int itemBPoints, string iodata)
        {
            int itemBOffset = 0;
            int result = 0;
            byte[] buffer = new byte[8];  // Max long 4 word
            int bufOffset = 0;
            byte[] ary_bytiodata = HexToByte(iodata);
            int iodataOffset = 0;

            Buffer.BlockCopy(ary_bytiodata, iodataOffset, buffer, bufOffset, ary_bytiodata.Length);

            ulong[] value = new ulong[1];
            Buffer.BlockCopy(buffer, 0, value, 0, 8);
            result = (int)(value[0] >> (itemBOffset & 63) & (ulong)(Math.Pow(2, (double)itemBPoints) - 1));
            return result;
        }

        public string HexToASCII(string iodata)
        {
            StringBuilder sb = new StringBuilder();
            byte[] ary_bytiodata = HexToByte(iodata);
            int bitoffset = 0;
            for (int i = 0; i < ary_bytiodata.Length; i++)
            {
                sb.Append((char)(ary_bytiodata[i] >> ((bitoffset * 8) & 31) & 255));
            }
            return sb.ToString().Replace('\0', ' ');
        }

        public string HexToBit(int itemBOffset, int itemBPoints, string iodata)
        {
            StringBuilder sb = new StringBuilder();

            byte[] ary_bytiodata = HexToByte(iodata);
            int wstart = (itemBOffset) / 16;
            int bstart = (itemBOffset) % 16;

            for (int p = 0; p < itemBPoints; p++)
            {
                if (bstart > 15)
                {
                    bstart = 0;
                    wstart++;
                }
                int mask = 1 << (bstart & 31);
                sb.Append((iodata[wstart] & mask) / mask);
                bstart++;
            }
            return sb.ToString();
        }

        public int CalcBitPoints(string UUID_Address)
        {
            char[] delimiterChars = { '.', ':' };
            int first_colon_index = -1;
            int first_dot_index = -1;

            string[] Split_Words = UUID_Address.Split(delimiterChars);

            first_colon_index = UUID_Address.IndexOf(":");
            first_dot_index = UUID_Address.IndexOf(".");

            //----- 討論不合法表示是是否該回傳整個word or o
            if ((first_colon_index == -1) && first_dot_index == -1)  // W1000  沒有寫 dot or :
            {
                //return 0;
                return 16;
            }
            else if (first_colon_index == -1)                       // 忘記標誌 : 則代表全部
            {
                //return 0;
                return 16;
            }
            else if (first_dot_index < first_colon_index)          //W1000.3:5   
            {
                string[] tempSplit_Words = UUID_Address.Substring(first_dot_index).Split(delimiterChars);
                int start = int.Parse(tempSplit_Words[1]);
                int end = int.Parse(tempSplit_Words[2]);
                int diff = (end - start) + 1;
                return diff;
            }
            else   // 表示不合法的表示式for bit return 0 
            {
                return 0;
            }
        }

        public int CalcWordPoints(string UUID_Address)
        {
            char[] delimiterChars = { '.', ':' };
            int first_colon_index = -1;
            int first_dot_index = -1;

            string[] Split_Words = UUID_Address.Split(delimiterChars);

            //---- Address 用法  W1000:3000
            first_colon_index = UUID_Address.IndexOf(":");
            first_dot_index = UUID_Address.IndexOf(".");

            if ((first_colon_index > -1) && first_dot_index == -1)   // W1000:3000
            {
                int start = int.Parse(Split_Words[0].Replace("W", "").Replace("w", ""));
                int end = int.Parse(Split_Words[1].Replace("W", "").Replace("w", ""));
                int diff = (end - start) + 1;
                return diff;

            }
            else
            {
                return 1;
            }
        }
    }
}

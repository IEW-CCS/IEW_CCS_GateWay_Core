using System;
using System.Collections.Generic;
using System.Text;
using ObjectManager;
using System.Linq;

namespace EDCService
{
    class EDC_Interval_Partaker
    {
        DateTime _InitialDatatime ;
        double report_interval;
        EDCPartaker _BaseEDCInfo = new EDCPartaker();
        List<List<cls_EDC_Body_Item>> _EDCHisInfo = null;
        List<cls_EDC_Body_Item> _lst_EDC_Body_Item = null;

        public EDC_Interval_Partaker (EDCPartaker BaseEDCInfo)
        {
            _InitialDatatime = DateTime.Now;
            report_interval = BaseEDCInfo.report_interval;
            _BaseEDCInfo =(EDCPartaker) BaseEDCInfo.Clone();
            _EDCHisInfo = new List<List<cls_EDC_Body_Item>>();
            _lst_EDC_Body_Item = new List<cls_EDC_Body_Item>(_BaseEDCInfo.edcitem_info.ToArray());

            // Export Clear 原始的  如果不清除就是保留最新的一份;
            _BaseEDCInfo.edcitem_info.Clear();

        }

        public string GetEDCPath ()
        {

            return _BaseEDCInfo.ReportEDCPath;

        }

        public bool Checkexpired (DateTime Current)
        {
            TimeSpan ts = Current - _InitialDatatime;

            double diff = ts.TotalSeconds;
            if (diff > report_interval)
                return true;
            else
                return false;
           
        }

        public void Add_EDC2Queue(List<cls_EDC_Body_Item> input_EDC_Items)
        {
            List<cls_EDC_Body_Item> t = new List<cls_EDC_Body_Item>(input_EDC_Items.ToArray());
            _EDCHisInfo.Add(t);
        }

        public string Generate_EDC()
        {
            string _strresult = string.Empty;


            foreach (cls_EDC_Body_Item key in _lst_EDC_Body_Item)
            {
               
                IEnumerable<cls_EDC_Body_Item> selectManyQuery = _EDCHisInfo.SelectMany(o => o.ToList());
                List<cls_EDC_Body_Item> EDC_BodyItems = selectManyQuery.Where(o => o.item_name.Equals(key.item_name)).ToList();
                List<double> Item1Value = EDC_BodyItems.Select(o => Convert.ToDouble(o.item_value)).ToList();

                foreach (string founction in _BaseEDCInfo.interval_function)
                {

                    cls_EDC_Body_Item EDC_Interval_Func = new cls_EDC_Body_Item();
                    EDC_Interval_Func.item_name = string.Concat(key.item_name, "_", founction);
                    EDC_Interval_Func.item_type = "X";
                    switch (founction)
                    {
                        case "MAX":
                            EDC_Interval_Func.item_value = Item1Value.Max().ToString();
                            _BaseEDCInfo.edcitem_info.Add(EDC_Interval_Func);
                            break;

                        case "MIN":
                            EDC_Interval_Func.item_value = Item1Value.Min().ToString();
                            _BaseEDCInfo.edcitem_info.Add(EDC_Interval_Func);
                            break;

                        case "AVG":
                            EDC_Interval_Func.item_value = Item1Value.Average().ToString();
                            _BaseEDCInfo.edcitem_info.Add(EDC_Interval_Func);
                            break;

                        default:

                            break;
                    }

                }

            }
            _strresult = _BaseEDCInfo.xml_string();
            _EDCHisInfo.Clear();
            return _strresult;
        }


    }
}

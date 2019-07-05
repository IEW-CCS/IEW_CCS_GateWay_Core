using System;
using System.Collections.Generic;
using System.Text;

namespace DBService.DBContext
{
    public class IOT_DEVICE
    {
        public int id { get; set; }

        public string gateway_id {get; set; }

        public string device_id { get; set; }

        public string device_desc { get; set; }

        public string device_type { get; set; }

        public string status { get; set; }

        public string location { get; set; }

        public double value { get; set; }

        public double lcl { get; set; }

        public double ucl { get; set; }

        public double lspec { get; set; }

        public double uspec { get; set; }

        public DateTime clm_date_time { get; set; }

        public string clm_user { get; set; }

        public int rpt_interval { get; set; }

        public int col_interval { get; set; }

        public string ooc_flg { get; set; }

        public string oos_flg { get; set; }

        public string alarm_flg { get; set; }

        public string eqp_id { get; set; }

        public string sub_eqp_id { get; set; }

        public int device_no { get; set; }

        public object Clone()
        {
            return this.MemberwiseClone();
        }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;


namespace DBService.DBContext
{
    public class IOT_DEVICE_EDC_LABEL
    {
        public int id { get; set; }
        [DisplayName("Device ID")]
        public string device_id { get; set; }
        [DisplayName("Data Name")]
        public string data_name { get; set; }
        [DisplayName("Data Index")]
        public double lcl { get; set; }
        [DisplayName("lcl")]
        public double ucl { get; set; }
        [DisplayName("ucl")]
        public double lspec { get; set; }
        [DisplayName("Lspec")]
        public double uspec { get; set; }
        [DisplayName("Uspec")]
        public int data_index { get; set; }
        [DisplayName("上報時間")]
        public DateTime clm_date_time { get; set; }
        [DisplayName("上報者")]
        public string clm_user { get; set; }
    }
}

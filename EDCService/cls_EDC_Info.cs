using System;
using System.Xml.Serialization;
using System.Xml;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;


namespace EDCService
{

    [Serializable]
    [XmlRoot("EDC")]
    public class EDC_Report
    {
        [XmlElement("glass_id")]
        public string Glass_ID { get; set; }
        [XmlElement("group_id")]
        public string GROUP_ID { get; set; }
        [XmlElement("lot_id")]
        public string LOT_ID { get; set; }
        [XmlElement("product_id")]
        public string PRODUCT_ID { get; set; }
        [XmlElement("pfcd")]
        public string PFCD { get; set; }
        [XmlElement("eqp_id")]
        public string EQP_ID { get; set; }
        [XmlElement("sub_eqp_id")]
        public string SUB_EQP_ID { get; set; }
        [XmlElement("ec_code")]
        public string EC_CODE { get; set; }
        [XmlElement("route_no")]
        public string ROUTE_NO { get; set; }
        [XmlElement("route_version")]
        public string ROUTE_VERSION { get; set; }
        [XmlElement("owner")]
        public string OWNER { get; set; }
        [XmlElement("recipe_id")]
        public string RECIPE_ID { get; set; }
        [XmlElement("operation")]
        public string OPERATION { get; set; }
        [XmlElement("ope_id")]
        public string OPE_ID { get; set; }
        [XmlElement("ope_no")]
        public string OPE_NO { get; set; }
        [XmlElement("proc_id")]
        public string PROC_ID { get; set; }
        [XmlElement("rtc_flag")]
        public string RTC_FLAG { get; set; }
        [XmlElement("pnp")]
        public string PNP { get; set; }
        [XmlElement("chamber")]
        public string CHAMBER { get; set; }
        [XmlElement("cassette_id")]
        public string CASSETTE_ID { get; set; }
        [XmlElement("line_batch_id")]
        public string LINE_BATCH_ID { get; set; }
        [XmlElement("split_id")]
        public string SPLIT_ID { get; set; }
        [XmlElement("cldate")]
        public string CLDATE { get; set; }
        [XmlElement("cltime")]
        public string CLTIME { get; set; }
        [XmlElement("mes_link_key")]
        public string MES_LINK_KEY { get; set; }
        [XmlElement("rework_count")]
        public string REWORK_COUNT { get; set; }
        [XmlElement("operator")]
        public string OPERATOR { get; set; }

        [XmlArray("datas")]
        [XmlArrayItem("iary")]
        public List<IARYClass> IARY { get; set; }

        public IARYClass CreateIARYClass()
        {
            IARYClass createobj = new IARYClass();
            IARY.Add(createobj);
            return createobj;
        }

        public class IARYClass
        {
            [XmlElement("item_name")]
            public string ITEM_NAME { get; set; }
            [XmlElement("item_type")]
            public string ITEM_TYPE { get; set; }
            [XmlElement("item_value")]
            public string ITEM_VALUE { get; set; }

            public IARYClass()
            {
                ITEM_NAME = string.Empty;
                ITEM_TYPE = string.Empty;
                ITEM_VALUE = string.Empty;
            }
        }

        //---- 建構子維持 空白 ----
        public EDC_Report()
        {
            Glass_ID = string.Empty;
            GROUP_ID = string.Empty;
            LOT_ID = string.Empty;
            PRODUCT_ID = string.Empty;
            PFCD = string.Empty;
            EQP_ID = string.Empty;
            SUB_EQP_ID = string.Empty;
            EC_CODE = string.Empty;
            ROUTE_NO = string.Empty;
            ROUTE_VERSION = string.Empty;
            OWNER = string.Empty;
            RECIPE_ID = string.Empty;
            OPERATION = string.Empty;
            OPE_ID = string.Empty;
            OPE_NO = string.Empty;
            PROC_ID = string.Empty;
            RTC_FLAG = string.Empty;
            PNP = string.Empty;
            CHAMBER = string.Empty;
            CASSETTE_ID = string.Empty;
            LINE_BATCH_ID = string.Empty;
            SPLIT_ID = string.Empty;
            CLDATE = string.Empty;
            CLTIME = string.Empty;
            MES_LINK_KEY = string.Empty;
            REWORK_COUNT = string.Empty;
            OPERATOR = string.Empty;
            IARY = new List<IARYClass>();
        }

        public object Clone()
        {

            EDC_Report rtn = (EDC_Report)this.MemberwiseClone();
            rtn.IARY = new List<IARYClass>();
            if (this.IARY != null)
            {
                foreach (IARYClass iary in this.IARY)
                {
                    IARYClass ary = new IARYClass();
                    ary.ITEM_NAME = iary.ITEM_NAME;
                    ary.ITEM_TYPE = iary.ITEM_TYPE;
                    ary.ITEM_VALUE = iary.ITEM_VALUE;
                    rtn.IARY.Add(ary);
                }
            }
            return rtn;

        }

        public class stringwriterUTF8 : StringWriter
        {
            public override Encoding Encoding
            {
                get { return Encoding.UTF8; }
            }


        }

        public T EDC_Deserialize<T>(string s)
        {
            XmlDocument xdoc = new XmlDocument();
            try
            {
                xdoc.LoadXml(s);
                XmlNodeReader reader = new XmlNodeReader(xdoc.DocumentElement);
                XmlSerializer ser = new XmlSerializer(typeof(T));
                object obj = ser.Deserialize(reader);
                return (T)obj;
            }
            catch
            {
                return default(T);
            }

        }

        public void Write_EDC(string save_file_path)
        {
            // -----------  Define xml header -------------
            stringwriterUTF8 sw = new stringwriterUTF8();
            System.Xml.XmlWriterSettings settings = new System.Xml.XmlWriterSettings();
            settings.Indent = true;
            settings.NewLineChars = "\r\n";
            settings.Encoding = Encoding.UTF8;
            settings.IndentChars = "  ";  // next line indent 2 space
            settings.OmitXmlDeclaration = false;  // write header  xml version and encode 
            XmlSerializerNamespaces names = new XmlSerializerNamespaces();
            names.Add(string.Empty, string.Empty);

            System.Xml.XmlWriter writer = System.Xml.XmlWriter.Create(sw, settings);
            XmlSerializer EDC_serialize = new XmlSerializer(this.GetType());  // typeof(EDC_Report));
            EDC_serialize.Serialize(writer, this, names);
            writer.Close();

            if (!Directory.Exists(Path.GetDirectoryName(save_file_path)))
            {
                Directory.CreateDirectory(Path.GetDirectoryName(save_file_path));
            }

            try
            {
                string temp_name = save_file_path + ".tmp";
                using (FileStream fs = System.IO.File.Open(temp_name, FileMode.Create))
                {

                    using (StreamWriter EDCWriter = new StreamWriter(fs, Encoding.UTF8))
                    {
                        EDCWriter.Write(sw.ToString());
                        EDCWriter.Flush();
                        fs.Flush();

                    }
                }

                if (System.IO.File.Exists(save_file_path))
                    System.IO.File.Delete(save_file_path);
                while (System.IO.File.Exists(save_file_path))
                    Thread.Sleep(1);
                System.IO.File.Move(temp_name, save_file_path);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }


        }

        public string EDC_String_Serialize()
        {
            // -----------  Define xml header -------------
            stringwriterUTF8 sw = new stringwriterUTF8();
            System.Xml.XmlWriterSettings settings = new System.Xml.XmlWriterSettings();
            settings.Indent = true;
            settings.NewLineChars = "\r\n";
            settings.Encoding = Encoding.UTF8;
            settings.IndentChars = "  ";  // next line indent 2 space
            settings.OmitXmlDeclaration = false;  // write header  xml version and encode 
            XmlSerializerNamespaces names = new XmlSerializerNamespaces();
            names.Add(string.Empty, string.Empty);

            System.Xml.XmlWriter writer = System.Xml.XmlWriter.Create(sw, settings);
            XmlSerializer EDC_serialize = new XmlSerializer(this.GetType());  // typeof(EDC_Report));
            EDC_serialize.Serialize(writer, this, names);
            writer.Close();
            return sw.ToString();

        }

    }
}

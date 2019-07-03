using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using MySql.Data.EntityFrameworkCore;
using System.Data.Common;


namespace DBService.DBContext
{
    public class IOT_DbContext : DbContext
    {

        static DbContextOptions CreateDbConnection(string providerName, string connectionString)
        {
            //"server= localhost;database=IoTDB;user=root;password=qQ123456"
            DbContextOptionsBuilder optionsBuilder = new DbContextOptionsBuilder();
            switch (providerName)
            {
                case "MS SQL":
                    optionsBuilder.UseSqlServer(connectionString);
                    break;

                case "My SQL":
                    optionsBuilder.UseMySQL(connectionString);
                    break;

                default:
                    break;
            }
            return optionsBuilder.Options;
        }

        // Constructor 
        public IOT_DbContext(string provider, string connectstring) : base(CreateDbConnection(provider, connectstring))
        {  
        }

        public DbSet<IOT_DEVICE> IOT_DEVICE { get; set; }
        public DbSet<IOT_DEVICE_EDC_LABEL> IOT_DEVICE_EDC_LABEL { get; set; }
        public DbSet<IOT_DEVICE_EDC_001> IOT_DEVICE_EDC_001 { get; set; }
        public DbSet<IOT_DEVICE_EDC_002> IOT_DEVICE_EDC_002 { get; set; }
        public DbSet<IOT_DEVICE_EDC_003> IOT_DEVICE_EDC_003 { get; set; }
        public DbSet<IOT_DEVICE_EDC_004> IOT_DEVICE_EDC_004 { get; set; }
        public DbSet<IOT_DEVICE_EDC_005> IOT_DEVICE_EDC_005 { get; set; }
        public DbSet<IOT_DEVICE_EDC_006> IOT_DEVICE_EDC_006 { get; set; }
        public DbSet<IOT_DEVICE_EDC_007> IOT_DEVICE_EDC_007 { get; set; }
        public DbSet<IOT_DEVICE_EDC_008> IOT_DEVICE_EDC_008 { get; set; }
        public DbSet<IOT_DEVICE_EDC_009> IOT_DEVICE_EDC_009 { get; set; }
        public DbSet<IOT_DEVICE_EDC_010> IOT_DEVICE_EDC_010 { get; set; }
        public DbSet<IOT_DEVICE_EDC_011> IOT_DEVICE_EDC_011 { get; set; }
        public DbSet<IOT_DEVICE_EDC_012> IOT_DEVICE_EDC_012 { get; set; }
        public DbSet<IOT_DEVICE_EDC_013> IOT_DEVICE_EDC_013 { get; set; }
        public DbSet<IOT_DEVICE_EDC_014> IOT_DEVICE_EDC_014 { get; set; }
        public DbSet<IOT_DEVICE_EDC_015> IOT_DEVICE_EDC_015 { get; set; }
        public DbSet<IOT_DEVICE_EDC_016> IOT_DEVICE_EDC_016 { get; set; }
        public DbSet<IOT_DEVICE_EDC_017> IOT_DEVICE_EDC_017 { get; set; }
        public DbSet<IOT_DEVICE_EDC_018> IOT_DEVICE_EDC_018 { get; set; }
        public DbSet<IOT_DEVICE_EDC_019> IOT_DEVICE_EDC_019 { get; set; }
        public DbSet<IOT_DEVICE_EDC_020> IOT_DEVICE_EDC_020 { get; set; }
        public DbSet<IOT_DEVICE_EDC_021> IOT_DEVICE_EDC_021 { get; set; }
        public DbSet<IOT_DEVICE_EDC_022> IOT_DEVICE_EDC_022 { get; set; }
        public DbSet<IOT_DEVICE_EDC_023> IOT_DEVICE_EDC_023 { get; set; }
        public DbSet<IOT_DEVICE_EDC_024> IOT_DEVICE_EDC_024 { get; set; }
        public DbSet<IOT_DEVICE_EDC_025> IOT_DEVICE_EDC_025 { get; set; }
        public DbSet<IOT_DEVICE_EDC_026> IOT_DEVICE_EDC_026 { get; set; }
        public DbSet<IOT_DEVICE_EDC_027> IOT_DEVICE_EDC_027 { get; set; }
        public DbSet<IOT_DEVICE_EDC_028> IOT_DEVICE_EDC_028 { get; set; }
        public DbSet<IOT_DEVICE_EDC_029> IOT_DEVICE_EDC_029 { get; set; }
        public DbSet<IOT_DEVICE_EDC_030> IOT_DEVICE_EDC_030 { get; set; }
        public DbSet<IOT_DEVICE_EDC_031> IOT_DEVICE_EDC_031 { get; set; }
        public DbSet<IOT_DEVICE_EDC_032> IOT_DEVICE_EDC_032 { get; set; }
        public DbSet<IOT_DEVICE_EDC_033> IOT_DEVICE_EDC_033 { get; set; }
        public DbSet<IOT_DEVICE_EDC_034> IOT_DEVICE_EDC_034 { get; set; }
        public DbSet<IOT_DEVICE_EDC_035> IOT_DEVICE_EDC_035 { get; set; }
        public DbSet<IOT_DEVICE_EDC_036> IOT_DEVICE_EDC_036 { get; set; }
        public DbSet<IOT_DEVICE_EDC_037> IOT_DEVICE_EDC_037 { get; set; }
        public DbSet<IOT_DEVICE_EDC_038> IOT_DEVICE_EDC_038 { get; set; }
        public DbSet<IOT_DEVICE_EDC_039> IOT_DEVICE_EDC_039 { get; set; }
        public DbSet<IOT_DEVICE_EDC_040> IOT_DEVICE_EDC_040 { get; set; }
        public DbSet<IOT_DEVICE_EDC_041> IOT_DEVICE_EDC_041 { get; set; }
        public DbSet<IOT_DEVICE_EDC_042> IOT_DEVICE_EDC_042 { get; set; }
        public DbSet<IOT_DEVICE_EDC_043> IOT_DEVICE_EDC_043 { get; set; }
        public DbSet<IOT_DEVICE_EDC_044> IOT_DEVICE_EDC_044 { get; set; }
        public DbSet<IOT_DEVICE_EDC_045> IOT_DEVICE_EDC_045 { get; set; }
        public DbSet<IOT_DEVICE_EDC_046> IOT_DEVICE_EDC_046 { get; set; }
        public DbSet<IOT_DEVICE_EDC_047> IOT_DEVICE_EDC_047 { get; set; }
        public DbSet<IOT_DEVICE_EDC_048> IOT_DEVICE_EDC_048 { get; set; }
        public DbSet<IOT_DEVICE_EDC_049> IOT_DEVICE_EDC_049 { get; set; }
        public DbSet<IOT_DEVICE_EDC_050> IOT_DEVICE_EDC_050 { get; set; }
        public DbSet<IOT_DEVICE_EDC_051> IOT_DEVICE_EDC_051 { get; set; }
        public DbSet<IOT_DEVICE_EDC_052> IOT_DEVICE_EDC_052 { get; set; }
        public DbSet<IOT_DEVICE_EDC_053> IOT_DEVICE_EDC_053 { get; set; }
        public DbSet<IOT_DEVICE_EDC_054> IOT_DEVICE_EDC_054 { get; set; }
        public DbSet<IOT_DEVICE_EDC_055> IOT_DEVICE_EDC_055 { get; set; }
        public DbSet<IOT_DEVICE_EDC_056> IOT_DEVICE_EDC_056 { get; set; }
        public DbSet<IOT_DEVICE_EDC_057> IOT_DEVICE_EDC_057 { get; set; }
        public DbSet<IOT_DEVICE_EDC_058> IOT_DEVICE_EDC_058 { get; set; }
        public DbSet<IOT_DEVICE_EDC_059> IOT_DEVICE_EDC_059 { get; set; }
        public DbSet<IOT_DEVICE_EDC_060> IOT_DEVICE_EDC_060 { get; set; }
        public DbSet<IOT_DEVICE_EDC_061> IOT_DEVICE_EDC_061 { get; set; }
        public DbSet<IOT_DEVICE_EDC_062> IOT_DEVICE_EDC_062 { get; set; }
        public DbSet<IOT_DEVICE_EDC_063> IOT_DEVICE_EDC_063 { get; set; }
        public DbSet<IOT_DEVICE_EDC_064> IOT_DEVICE_EDC_064 { get; set; }
        public DbSet<IOT_DEVICE_EDC_065> IOT_DEVICE_EDC_065 { get; set; }
        public DbSet<IOT_DEVICE_EDC_066> IOT_DEVICE_EDC_066 { get; set; }
        public DbSet<IOT_DEVICE_EDC_067> IOT_DEVICE_EDC_067 { get; set; }
        public DbSet<IOT_DEVICE_EDC_068> IOT_DEVICE_EDC_068 { get; set; }
        public DbSet<IOT_DEVICE_EDC_069> IOT_DEVICE_EDC_069 { get; set; }
        public DbSet<IOT_DEVICE_EDC_070> IOT_DEVICE_EDC_070 { get; set; }
        public DbSet<IOT_DEVICE_EDC_071> IOT_DEVICE_EDC_071 { get; set; }
        public DbSet<IOT_DEVICE_EDC_072> IOT_DEVICE_EDC_072 { get; set; }
        public DbSet<IOT_DEVICE_EDC_073> IOT_DEVICE_EDC_073 { get; set; }
        public DbSet<IOT_DEVICE_EDC_074> IOT_DEVICE_EDC_074 { get; set; }
        public DbSet<IOT_DEVICE_EDC_075> IOT_DEVICE_EDC_075 { get; set; }
        public DbSet<IOT_DEVICE_EDC_076> IOT_DEVICE_EDC_076 { get; set; }
        public DbSet<IOT_DEVICE_EDC_077> IOT_DEVICE_EDC_077 { get; set; }
        public DbSet<IOT_DEVICE_EDC_078> IOT_DEVICE_EDC_078 { get; set; }
        public DbSet<IOT_DEVICE_EDC_079> IOT_DEVICE_EDC_079 { get; set; }
        public DbSet<IOT_DEVICE_EDC_080> IOT_DEVICE_EDC_080 { get; set; }
        public DbSet<IOT_DEVICE_EDC_081> IOT_DEVICE_EDC_081 { get; set; }
        public DbSet<IOT_DEVICE_EDC_082> IOT_DEVICE_EDC_082 { get; set; }
        public DbSet<IOT_DEVICE_EDC_083> IOT_DEVICE_EDC_083 { get; set; }
        public DbSet<IOT_DEVICE_EDC_084> IOT_DEVICE_EDC_084 { get; set; }
        public DbSet<IOT_DEVICE_EDC_085> IOT_DEVICE_EDC_085 { get; set; }
        public DbSet<IOT_DEVICE_EDC_086> IOT_DEVICE_EDC_086 { get; set; }
        public DbSet<IOT_DEVICE_EDC_087> IOT_DEVICE_EDC_087 { get; set; }
        public DbSet<IOT_DEVICE_EDC_088> IOT_DEVICE_EDC_088 { get; set; }
        public DbSet<IOT_DEVICE_EDC_089> IOT_DEVICE_EDC_089 { get; set; }
        public DbSet<IOT_DEVICE_EDC_090> IOT_DEVICE_EDC_090 { get; set; }
        public DbSet<IOT_DEVICE_EDC_091> IOT_DEVICE_EDC_091 { get; set; }
        public DbSet<IOT_DEVICE_EDC_092> IOT_DEVICE_EDC_092 { get; set; }
        public DbSet<IOT_DEVICE_EDC_093> IOT_DEVICE_EDC_093 { get; set; }
        public DbSet<IOT_DEVICE_EDC_094> IOT_DEVICE_EDC_094 { get; set; }
        public DbSet<IOT_DEVICE_EDC_095> IOT_DEVICE_EDC_095 { get; set; }
        public DbSet<IOT_DEVICE_EDC_096> IOT_DEVICE_EDC_096 { get; set; }
        public DbSet<IOT_DEVICE_EDC_097> IOT_DEVICE_EDC_097 { get; set; }
        public DbSet<IOT_DEVICE_EDC_098> IOT_DEVICE_EDC_098 { get; set; }
        public DbSet<IOT_DEVICE_EDC_099> IOT_DEVICE_EDC_099 { get; set; }
        public DbSet<IOT_DEVICE_EDC_100> IOT_DEVICE_EDC_100 { get; set; }
        public DbSet<IOT_DEVICE_EDC_101> IOT_DEVICE_EDC_101 { get; set; }
        public DbSet<IOT_DEVICE_EDC_102> IOT_DEVICE_EDC_102 { get; set; }
        public DbSet<IOT_DEVICE_EDC_103> IOT_DEVICE_EDC_103 { get; set; }
        public DbSet<IOT_DEVICE_EDC_104> IOT_DEVICE_EDC_104 { get; set; }
        public DbSet<IOT_DEVICE_EDC_105> IOT_DEVICE_EDC_105 { get; set; }
        public DbSet<IOT_DEVICE_EDC_106> IOT_DEVICE_EDC_106 { get; set; }
        public DbSet<IOT_DEVICE_EDC_107> IOT_DEVICE_EDC_107 { get; set; }
        public DbSet<IOT_DEVICE_EDC_108> IOT_DEVICE_EDC_108 { get; set; }
        public DbSet<IOT_DEVICE_EDC_109> IOT_DEVICE_EDC_109 { get; set; }
        public DbSet<IOT_DEVICE_EDC_110> IOT_DEVICE_EDC_110 { get; set; }
        public DbSet<IOT_DEVICE_EDC_111> IOT_DEVICE_EDC_111 { get; set; }
        public DbSet<IOT_DEVICE_EDC_112> IOT_DEVICE_EDC_112 { get; set; }
        public DbSet<IOT_DEVICE_EDC_113> IOT_DEVICE_EDC_113 { get; set; }
        public DbSet<IOT_DEVICE_EDC_114> IOT_DEVICE_EDC_114 { get; set; }
        public DbSet<IOT_DEVICE_EDC_115> IOT_DEVICE_EDC_115 { get; set; }
        public DbSet<IOT_DEVICE_EDC_116> IOT_DEVICE_EDC_116 { get; set; }
        public DbSet<IOT_DEVICE_EDC_117> IOT_DEVICE_EDC_117 { get; set; }
        public DbSet<IOT_DEVICE_EDC_118> IOT_DEVICE_EDC_118 { get; set; }
        public DbSet<IOT_DEVICE_EDC_119> IOT_DEVICE_EDC_119 { get; set; }
        public DbSet<IOT_DEVICE_EDC_120> IOT_DEVICE_EDC_120 { get; set; }
        public DbSet<IOT_DEVICE_EDC_121> IOT_DEVICE_EDC_121 { get; set; }
        public DbSet<IOT_DEVICE_EDC_122> IOT_DEVICE_EDC_122 { get; set; }
        public DbSet<IOT_DEVICE_EDC_123> IOT_DEVICE_EDC_123 { get; set; }
        public DbSet<IOT_DEVICE_EDC_124> IOT_DEVICE_EDC_124 { get; set; }
        public DbSet<IOT_DEVICE_EDC_125> IOT_DEVICE_EDC_125 { get; set; }
        public DbSet<IOT_DEVICE_EDC_126> IOT_DEVICE_EDC_126 { get; set; }
        public DbSet<IOT_DEVICE_EDC_127> IOT_DEVICE_EDC_127 { get; set; }
        public DbSet<IOT_DEVICE_EDC_128> IOT_DEVICE_EDC_128 { get; set; }


    }


 
}

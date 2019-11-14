using System;
using System.Collections.Generic;
using System.Text;

namespace NEL_FutureDao_BT.lib
{
    class TimeHelper
    {
        public static long GetTimeStamp()
        {
            TimeSpan ts = DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0, 0);
            return Convert.ToInt64(ts.TotalSeconds);
        }

        public static DateTime transDateTime(long tm)
        {
            var date = new DateTime(1970, 1, 1, 0, 0, 0, 0);
            date = date.AddSeconds(tm);
            return date;
        }
        public static long transTimeStamp(DateTime tm)
        {
            TimeSpan ts = tm - new DateTime(1970, 1, 1, 0, 0, 0, 0);
            return Convert.ToInt64(ts.TotalSeconds);
        }
    }
}

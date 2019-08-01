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
    }
}

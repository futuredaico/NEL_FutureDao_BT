using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Text;

namespace NEL_FutureDao_BT.lib
{
    static class MongoDecimalHelper
    {
        public static BsonDecimal128 format(this decimal value)
        {
            return BsonDecimal128.Create(value);
        }

        public static decimal format(this BsonDecimal128 value)
        {
            return value.AsDecimal;
        }

        public static decimal formatPrecision(this decimal value, int precision=4)
        {
            if (precision < 0) return value;
            if (precision == 0) return decimal.Parse(value.ToString().Split(".")[0]);
            StringBuilder sb = new StringBuilder("#0.");
            for(int i=0;i<precision; ++i)
            {
                sb.Append("0");
            }
            return decimal.Parse(value.ToString(sb.ToString()));
        }

       
    }
}

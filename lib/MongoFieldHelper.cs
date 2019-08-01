using Newtonsoft.Json.Linq;
using System.Linq;

namespace NEL_FutureDao_BT.lib
{
    public static class MongoFieldHelper
    {
        public static JObject toFilter(this long[] ls, string field, string logicalOperator = "$or")
        {
            if (ls.Count() == 1)
            {
                return new JObject() { { field, ls[0] } };
            }
            return new JObject() { { logicalOperator, new JArray() { ls.Select(item => new JObject() { { field, item } }).ToArray() } } };
        }
        public static JObject toFilter(this string[] ss, string field, string logicalOperator = "$or")
        {
            if (ss.Count() == 1)
            {
                return new JObject() { { field, ss[0] } };
            }
            return new JObject() { { logicalOperator, new JArray() { ss.Select(item => new JObject() { { field, item } }).ToArray() } } };
        }
        public static JObject toReturn(this string[] fieldArr)
        {
            JObject obj = new JObject();
            foreach (var field in fieldArr)
            {
                obj.Add(field, 1);
            }
            return obj;
        }
        public static JObject toSort(this string[] fieldArr, bool order = false)
        {
            int flag = order ? 1 : -1;
            JObject obj = new JObject();
            foreach (var field in fieldArr)
            {
                obj.Add(field, flag);
            }
            return obj;
        }
    }
}

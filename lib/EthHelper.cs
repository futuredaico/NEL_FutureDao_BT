using System;
using System.Text;
using Newtonsoft.Json.Linq;
using System.Numerics;
using System.Linq;
using System.Net;

namespace NEL_FutureDao_BT.lib
{
    class EthHelper
    {
        //static string apiUrl = "https://node3.web3api.com";
        static string apiUrl = "https://ropsten.infura.io/v3/638c755c81fe495e85debe581520b373";
        static int preLen = 64*13 + 2;
        static string ethSplit(string ss)
        {
            if (ss.Length == 0) return "";

            var r1 = ss.Substring(preLen).HexString2Bytes();
            var k = r1.Length - 1;
            for (; k >= 0; --k)
            {
                if (r1[k] != 0) break;
            }
            var len = k + 1;
            var r2 = new byte[len];
            Array.Copy(r1, 0, r2, 0, len);
            var rr = Encoding.UTF8.GetString(r2);
            /*
            var r1 = ss.Substring(preLen).HexString2Bytes();
            var r2 = new BigInteger(r1.Reverse().ToArray()).ToByteArray();
            var rr = Encoding.UTF8.GetString(r2.Reverse().ToArray());
            */
            return rr;
        }
        static string fillIndex(string proposalIndex)
        {
            return "0x3b214a74" + long.Parse(proposalIndex).ToString("x64");

        }
        public static string ethCall(string contractHash, string proposalIndex)
        {
            proposalIndex = fillIndex(proposalIndex);
            var postJo = new JObject {
                { "jsonrpc", "2.0"},
                { "method", "eth_call"},
                { "params", new JArray{
                    new JObject{{"to", contractHash},{ "data", proposalIndex } }, "latest" }
                },
                { "id", 2 },
            };
            var res = HttpHelper.HttpPost(apiUrl, Encoding.UTF8.GetBytes(postJo.ToString()));
            Console.WriteLine(res);
            //
            var resJo = JObject.Parse(res);
            var resStr = resJo["result"].ToString();
            return ethSplit(resStr);
        }
        
    }
}

using System;
using System.Text;
using Newtonsoft.Json.Linq;
using System.Numerics;
using System.Linq;
using System.Net;
using System.Collections.Generic;

namespace NEL_FutureDao_BT.lib
{
    class EthHelper
    {
        static string apiUrl_mainnet = "https://node3.web3api.com";
        //static string apiUrl_testnet = "https://ropsten.infura.io/v3/638c755c81fe495e85debe581520b373";
        static string apiUrl_testnet = "https://ropsten.infura.io/v3/424e7ec089ce460b97500d350cf9235c";
        static int preLenProposal = 64 * 13 + 2;
        static int preLenSymbol = 64 * 2 + 2;
        static string ethSplit(string ss, int preLen)
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
        static string ethSplitLong(string ss)
        {
            return new BigInteger(ss.Substring(2).HexString2Bytes().Reverse().ToArray()).ToString();
        }
        static string fillIndex(string proposalIndex)
        {
            return "0x3b214a74" + long.Parse(proposalIndex).ToString("x64");

        }
        public static string ethCall(string contractHash, string proposalIndex, string type = "mainnet")
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
            var apiUrl = type == "mainnet" ? apiUrl_mainnet : apiUrl_testnet;
            var res = HttpHelper.HttpPost(apiUrl, Encoding.UTF8.GetBytes(postJo.ToString()));
            Console.WriteLine(res);
            //
            var resJo = JObject.Parse(res);
            var resStr = resJo["result"].ToString();
            return ethSplit(resStr, preLenProposal);
        }



        class MoloVersion{
            public const string V1 = "1.0";
            public const string V2 = "2.0";
        }
        class ProposalParam
        {
            public string method { get; set; }
            public string methodHash { get; set; }
            public int resSubLen { get; set; }
        }
        class ProposalParamInfo
        {
            public static ProposalParam proposalV1 = new ProposalParam
            {
                method = "proposalQueue",
                methodHash = "0x3b214a74",
                resSubLen = 64 * 13 + 2
            };
            public static ProposalParam proposalV2 = new ProposalParam
            {
                method = "proposals",
                methodHash = "0x013cf08b",
                resSubLen = 64 * 15 + 2
            };
        }

        static string getProposalMethodHash(string version, out int resSubLen)
        {
            resSubLen = 0;

            var param = ProposalParamInfo.proposalV1;
            if (version == MoloVersion.V2)
            {
                param = ProposalParamInfo.proposalV2;
            }
            if (version == MoloVersion.V1)
            {
                param = ProposalParamInfo.proposalV1;
            }
            resSubLen = param.resSubLen;
            return param.methodHash;
        }
        static string fillIndex(string proposalIndex, string methodHash)
        {
            return methodHash + long.Parse(proposalIndex).ToString("x64");
        }
        public static string getProposalInfo(string contractHash, string proposalIndex, string version, string type)
        {
            var methodHash = getProposalMethodHash(version, out int resSubLen);
            var fullIndex = fillIndex(proposalIndex, methodHash);
            var resStr = eth_call_(contractHash, fullIndex, type);
            var res = ethSplit(resStr, resSubLen);
            return res;
        }
        static string eth_call_(string contractHash, string proposalIndex, string type)
        {
            var postJo = new JObject {
                { "jsonrpc", "2.0"},
                { "method", "eth_call"},
                { "params", new JArray{
                    new JObject{{"to", contractHash},{ "data", proposalIndex } }, "latest" }
                },
                { "id", 2 },
            };
            var apiUrl = type == "mainnet" ? apiUrl_mainnet : apiUrl_testnet;
            var res = HttpHelper.HttpPost(apiUrl, Encoding.UTF8.GetBytes(postJo.ToString()));
            Console.WriteLine(res);
            //
            var resJo = JObject.Parse(res);
            var resStr = resJo["result"].ToString();
            return resStr;
        }


        static Dictionary<string, string> tokenMethodDict = new Dictionary<string, string>
        {
            { "symbol", "0x95d89b41"},
            { "decimals", "0x313ce567"},
        };
        static string ethCallTokenInfo(string hash, string method, string type = "mainnet")
        {
            var postJo = new JObject {
                { "jsonrpc", "2.0"},
                { "method", "eth_call"},
                { "params", new JArray{
                    new JObject{ {"to", hash},{ "data", method } }, "latest" }
                },
                { "id", 2},
            };
            var apiUrl = type == "mainnet" ? apiUrl_mainnet : apiUrl_testnet;
            var res = HttpHelper.HttpPost(apiUrl, Encoding.UTF8.GetBytes(postJo.ToString()));
            Console.WriteLine(res);
            //
            var resJo = JObject.Parse(res);
            var resStr = resJo["result"].ToString();
            return resStr;
        }
        static string getTokenInfo(string hash, string method, string type)
        {
            var methodFmt = tokenMethodDict.GetValueOrDefault(method.ToLower());
            var res = ethCallTokenInfo(hash, methodFmt, type);
            return res;
        }
        public static string getTokenSymbol(string hash, string type)
        {
            var method = "symbol";
            var resStr = getTokenInfo(hash, method, type);
            return ethSplit(resStr, preLenSymbol);
        }
        public static long getTokenDecimals(string hash, string type)
        {
            var method = "decimals";
            var resStr = getTokenInfo(hash, method, type);
            var decimalsStr = ethSplitLong(resStr);
            var decimals = long.Parse(decimalsStr);
            return decimals;
        }

    }
}

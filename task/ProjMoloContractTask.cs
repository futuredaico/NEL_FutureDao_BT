using NEL.NNS.lib;
using NEL_FutureDao_BT.core;
using NEL_FutureDao_BT.lib;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;

namespace NEL_FutureDao_BT.task
{
    class ProjMoloContractTask : AbsTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo rConn;
        private DbConnInfo lConn;
        private string eventLogsCounter = "counters";
        private string eventLogsCol = "logs";
        private string notifyCounter = "molonotifycounters";
        private string notifyCol = "molonotifyinfos";
        private string projHashCol = "moloprojhashinfos";
        //
        public ProjMoloContractTask(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            rConn = Config.remoteDbConnInfo;
            lConn = Config.localDbConnInfo;

            initIndex();
        }
        private void initIndex()
        {
            mh.setIndex(lConn.connStr, lConn.connDB, notifyCol, "{'blockNumber':1,'event':1}", "i_blockNumber_event");
            mh.setIndex(lConn.connStr, lConn.connDB, notifyCol, "{'transactionHash':1,'event':1}", "i_transactionHash_event");
        }

        private string[] nilArr = new string[0];
        public override void process()
        {
            Sleep(2000);

            var rh = GetR("transaction",out long rt);
            var lh = GetL();
            if (lh >= rh)
            {
                Log(lh, rh);
                return;
            }
            //foreach(var hh in cc.OrderBy(p => p).ToArray())
            //{
            //    var index = hh;
            for (var index = lh + 1; index <= rh; ++index)
            {
                var findStr = new JObject { { "blockNumber", index } }.ToString();
                var queryRes = mh.GetData(rConn.connStr, rConn.connDB, eventLogsCol, findStr);
                if (queryRes.Count == 0)
                {
                    UpdateL(index, 0);
                    UpdateL(index, 0, nilArr);
                    Log(index, rh);
                    continue;
                }
                var blockTime = long.Parse(queryRes[0]["timestamp"].ToString());

                var res = queryRes.Select(p => {
                    p["contractHash"] = p["contractHash"].ToString().ToLower();
                    return p;
                });
                var hashArr = res.Select(p => p["contractHash"].ToString()).Distinct().ToArray();
                var hashDict = getProjId(hashArr, out Dictionary<string, string> hashTypeDict, out Dictionary<string, long> hashDecimalsDict);
                if (hashDict.Count == 0) continue;
                if (hashTypeDict.Count > 0)
                {
                    processNewContract(index, hashTypeDict, hashDecimalsDict);
                }
                res = res.Where(p => hashDict.ContainsKey(p["contractHash"].ToString())).ToArray();
                if (res.Count() == 0) continue;
                
                res = res.Select(p => dataFormat(p, hashDecimalsDict.GetValueOrDefault(p["contractHash"].ToString()))).ToArray();
                foreach (var item in res)
                {
                    if (item == null) continue;
                    item["contractHash"] = item["contractHash"].ToString();
                    findStr = new JObject { { "transactionHash", item["transactionHash"] }, { "event", item["event"] } }.ToString();
                    if (mh.GetDataCount(lConn.connStr, lConn.connDB, notifyCol, findStr) == 0)
                    {
                        item["blockTime"] = blockTime;
                        item["projId"] = hashDict.GetValueOrDefault(item["contractHash"].ToString());
                        mh.PutData(lConn.connStr, lConn.connDB, notifyCol, item.ToString());
                    }
                }
                UpdateL(index, blockTime);
                UpdateL(index, blockTime, hashDict.Values.Distinct().ToArray());
                Log(index, rh);
            }
            //
            updateCounter(rh, rt);
        }

        private void updateCounter(long index, long time)
        {
            if (index <= 0 || time <= 0) return;
            var findStr = "{}";
            var updateStr = new JObject { { "$set", new JObject { { "lastBlockIndex", index }, { "lastBlockTime", time } } } }.ToString();
            mh.UpdateDataMany(lConn.connStr, lConn.connDB, notifyCounter, updateStr, findStr);
        }

        private void UpdateL(long index, long time, string[] hashArr)
        {
            if (time == 0) return;
            var findStr = "{}";
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, notifyCounter, findStr);
            //
            string[] hasHashArr = null;
            string[] notHashArr = null;
            if (queryRes.Count == 0)
            {
                notHashArr = hashArr;
            } else
            {
                var counterArr = queryRes.Select(p => p["counter"].ToString());
                hasHashArr = counterArr.ToArray();
                if(hashArr != null && hashArr.Count() > 0)
                {
                    hasHashArr = hashArr.Where(p => counterArr.Contains(p)).ToArray();
                    notHashArr = hashArr.Where(p => !counterArr.Contains(p)).ToArray();
                }
            }
            if(hasHashArr != null && hasHashArr.Count() > 0)
            {
                var updateStr = new JObject { { "$set", new JObject { { "lastBlockIndex", index }, { "lastBlockTime", time } } } }.ToString();
                foreach (var item in hasHashArr)
                {
                    findStr = new JObject { { "counter", item } }.ToString();
                    mh.UpdateData(lConn.connStr, lConn.connDB, notifyCounter, updateStr, findStr);
                }
            }
            if (notHashArr != null && notHashArr.Count() > 0)
            {
                var joArr = notHashArr.Select(p => new JObject { { "counter", p }, { "lastBlockIndex", index }, { "lastBlockTime", time } }).ToArray();
                mh.PutData(lConn.connStr, lConn.connDB, notifyCounter, new JArray { joArr });
            }
        }
        long[] cc = new long[] {
                9055683,
    8983158,
    8983036,
    8979413,
    8976848,
    8974625,
    8972591,
    8884789,
    8884765,
    8884755,
    8884724,
    8756471,
    8756465,
    8719087,
    8688915,
    8688912,
    8655222,
    8652319,
    8652162,
    8649694,
    8649160,
    8647384,
    8641989,
    8641705,
    8637521,
    8621015,
    8612217,
    8602592,
    8602587,
    8594628,
    8594620,
    8594603,
    8594588,
    8594579,
    8581851,
    8574375,
    8568910,
    8561530,
    8557400,
    8556414,
    8554059,
    8552217,
    8552113,
    8552066,
    8551905,
    8551679,
    8551658,
    8544779,
    8543610,
    8542447,
    8520087,
    8518432,
    8506486,
    8503339,
    8500367,
    8497346,
    8491205,
    8491075,
    8490913,
    8490903,
    8490760,
    8490673,
    8486875,
    8485672,
    8484388,
    8479516,
    8479464,
    8455725,
    8455717,
    8448335,
    8447732,
    8442425,
    8441653,
    8441465,
    8435085,
    8432983,
    8432194,
    8432132,
    8428108,
    8422725,
    8422718,
    8422709,
    8412818,
    8407422,
    8406354,
    8405288,
    8404659,
    8404390,
    8403784,
    8385133,
    8384784,
    8381852,
    8372225,
    8372212,
    8372208,
    8372198,
    8372185,
    8371260,
    8367050,
    8367028,
    8364602,
    8357528,
    8353822,
    8350001,
    8345895,
    8345770,
    8345098,
    8345058,
    8344417,
    8343913,
    8337896,
    8337891,
    8333120,
    8333113,
    8330112,
    8330106,
    8328144,
    8326850,
    8320757,
    8317931,
    8317788,
    8317167,
    8317149,
    8317144,
    8317139,
    8315417,
    8314567,
    8314558,
    8313440,
    8313085,
    8312600,
    8312542,
    8299895,
    8299767,
    8299708,
    8297740,
    8297709,
    8292758,
    8292533,
    8280141,
    8263304,
    8257196,
    8257171,
    8253363,
    8247851,
    8246700,
    8246685,
    8238551,
    8224383,
    8223290,
    8223281,
    8221149,
    8198769,
    8177869,
    8177810,
    9128670,
    9126937,
    9122977,
    9112521,
    9109833,
    9097900,
    9096748,
    9062485,
    9041547,
    9041538,
    9041515,
    9036019,
    9033930,
    9024922,
    9017659,
    9017237,
    9015541,
    8958016,
    8936788,
    8930842,
    8891367,
    8875180,
    8846310,
    8842197,
    8842185,
    8838704,
    8828419,
    8827569,
    8825678,
    8810514,
    8810113,
    8809531,
    8804825,
    8804721,
    8803117,
    8802819,
    8802801,
    8788832,
    8785276,
    8731547,
    8731533,
    8731516,
    8685347,
    8666778,
    8644771,
    8619479,
    8619281,
    8618793,
    8618792,
    8618791,
    8618789,
    8618787,
    8613167,
    8613059,
    8612397,
    8600409,
    8596560,
    8588896,
    8582093,
    8576173,
    8567445,
    8563206,
    8560129,
    8555238,
    8554597,
    8548939,
    8544989,
    8542704,
    8542312,
    8542288,
    8535559,
    8534511,
    8533917,
    8533904,
    8533860,
    8524245,
    8524243,
    8522694,
    8517748,
    8517621,
    8516307,
    8515318,
    8514595,
    8511391,
    8509604,
    8504505,
    8504260,
    8496951,
    8492155,
    8491460,
    8484308,
    8480525,
    8478990,
    8472840,
    8472337,
    8471389,
    8470951,
    8470081,
    8460663,
    8459374,
    8458113,
    8456774,
    8455500,
    8454212,
    8453798,
    8452936,
    8442615,
    8441322,
    8440024,
    8417791,
    8414140,
    8410209,
    8408863,
    8408770,
    8402374,
    8401005,
    8395303,
    8395301,
    8395300,
    8395295,
    8395294,
    8395293,
    8395292,
    8389803,
    8388557,
    8383364,
    8383338,
    8383296,
    8383293,
    8383286,
    8377321,
    8377316,
    8377311,
    8370610,
    8370609,
    8370605,
    8364022,
    8362771,
    8362594,
    8362584,
    8362437,
    8362408,
    8362374,
    8362243,
    8362231,
    8362094,
    8361733,
    8361657,
    8361585,
    8361553,
    8361550,
    8359050,
    8350394,
    8348860,
    8347695,
    8347690,
    8347686,
    8318417,
    8307588,
    8288162,
    8277368,
    8273730,
    8273148,
    8272492,
    8272102,
    8271035,
    8268772,
    8267452,
    8266349,
    8266203,
    8263662,
    8262528,
    8261091,
    8244230,
    8239979,
    8234569,
    8234551,
    8230193,
    8224527,
    8224486,
    8224467,
    8221117,
    8220593,
    8220313,
    8216469,
    8216431,
    8215321,
    8215237,
    8209836,
    8209835,
    8209834,
    8208707,
    8208699,
    8208689,
    8208686,
    8208675,
    8208674,
    8208671,
    8208670,
    8208668,
    8208665,
    8208662,
    8208660,
    8208659,
    8208558,
    8208465,
    8208459,
    8207745,
    8207714,
    8207702,
    8207612,
    8203420,
    8203417,
    8203416,
    8203415,
    8202565,
    8201958,
    8201957,
    8201907,
    8201905,
    8201903,
    8201883,
    8201882,
    8201880,
    8201873,
    8201558,
    8201392,
    8201387,
    8201385,
    8201383,
    8201290,
    8195406,
    8194886,
    8194882,
    8194879,
    8194877,
    8194875,
    8194873,
    8194870,
    8194053,
    8194046,
    8192168,
    8189920,
    8189672,
    8189020,
    8189018,
    8189014,
    8189008,
    8184795,
    8184730,
    8182985,
    8182975,
    8182972,
    8182967,
    8182963,
    8182959,
    8182778,
    8182774,
    8182772,
    8182770,
    8182765,
    8182641,
    8182640,
    8182638,
    8182637,
    8182627,
    8182624,
    8182623,
    8182622,
    8177384,
    8176703,
    8176620,
    8176494,
    8176461,
    8176424,
    8176395,
    8176186,
    8176079,
    8174849,
    8173521,
    8170747,
    8170567,
    8170381,
    8170378,
    8170372,
    8170368,
    8169276,
    8156225,
    8138428,
    8125971,
    8125873,
    8124897,
    8124141,
    8119737,
    8119382,
    8119379,
    8116363,
    8116332,
    8115343,
    8115326,
    8115287,
    8114895,
    8113779,
    8113294,
    8113289,
    8113214,
    8113212,
    8113209,
    8113207,
    8094931,
    8094869,
    8092407,
    8087388,
    8081340,
    8081308,
    8081283,
    8081254,
    8038301,
    8024678,
    8018268,
    8009987,
    8002806,
    8001570,
    8000322,
    7998991,
    7997739,
    7991376,
    7982393,
    7981138,
    7979863,
    7978582,
    7978293,
    7963238,
    7961973,
    7950454,
    7946808,
    7946807,
    7938760,
    7935047,
    7933584,
    7932658,
    7932550,
    7928072,
    7921414,
    7919918,
    7915647,
    7914099,
    7914097,
    7914096,
    7914093,
    7914092,
    7914090,
    7914086,
    7914084,
    7908185,
    7908014,
    7908006,
    7907963,
    7907710,
    7907688,
    7907685,
    7907678,
    7907669,
    7907644,
    7907617,
    7906283,
    7904464,
    7903196,
    7902542,
    7902120,
    7902059,
    7902056,
    7902055,
    7902053,
    7902052,
    7902027,
    7901860,
    7900924,
    7900923,
    7900829,
    7895762,
    7891552,
    7890069,
    7890067,
    7889422,
    7889420,
    7889418,
    7888871,
    7887807,
    7887801,
    7887119,
    7886879,
    7883499,
    7883490,
    7883132,
    7881863,
    7881849,
    7880433,
    7880431,
    7880386,
    7876816,
    7876425,
    7876055,
    7876052,
    7875402,
    7875055,
    7872448,
    7872052,
    7871246,
    7871097,
    7869732,
    7869628,
    7869146,
    7868514,
    7868103,
    7862129,
    7860368,
    7858022,
    7857034,
    7851995,
    7851256,
    7850972,
    7850971,
    7850968,
    7850960,
    7850437,
    7849353,
    7849156,
    7849112,
    7849109,
    7849105,
    7848642,
    7845783,
    7842378,
    7841797,
    7840491,
    7839238,
    7838767,
    7838764,
    7837956,
    7836595,
    7835332,
    7834056,
    7832835,
    7831515,
    7830203,
    7830021,
    7828873,
    7827611,
    7826328,
    7825983,
    7825737,
    7825009,
    7823754,
    7821420,
    7821419,
    7817083,
    7817032,
    7815420,
    7815418,
    7815352,
    7813603,
    7813538,
    7813419,
    7813406,
    7813396,
    7811893,
    7807242,
    7798563,
    7798044,
    7793209,
    7791851,
    7791849,
    7786961,
    7785474,
    7775258,
    7771152,
    7771146,
    7771142,
    7768898,
    7766608,
    7766607,
    7766603,
    7766602,
    7766598,
    7766596,
    7766593,
    7766591,
    7766589,
    7766587,
    7766577,
    7766275,
    7765739,
    7765262,
    7765256,
    7765255,
    7765252,
    7765247,
    7765244,
    7765201,
    7765197,
    7761680,
    7755526,
    7755524,
    7755523,
    7754583,
    7754500,
    7754326,
    7754320,
    7754217,
    7754187,
    7754177,
    7753943,
    7753507,
    7753417,
    7753415,
    7753412,
    7753384,
    7753353,
    7753350,
    7753348,
    7753209,
    7751984,
    7751982,
    7751978,
    7751977,
    7751975,
    7751973,
    7751970,
    7751967,
    7751406,
    7751384,
    7751382,
    7751379,
    7751372,
    7751370,
    7751362,
    7751356,
    7751339,
    7751334,
    7751331,
    7746524,
    7746408,
    7746396,
    7745203,
    7745192,
    7745187,
    7745186,
    7745184,
    7745183,
    7745181,
    7745180,
    7745179,
    7745177,
    7745175,
    7743746,
    7743736,
    7743735,
    7743733,
    7743731,
    7743729,
    7740417,
    7740384,
    7736024,
    7734572,
    7734558,
    7734552,
    7734545,
    7734524,
    7734311,
    7733663,
    7733658,
    7733654,
    7733649,
    7733629,
    7733615,
    7733610,
    7733600,
    7733586,
    7733567,
    7727980,
    7727076,
    7722766,
    7722729,
    7721283,
    7715738,
    7715709,
    7715707,
    7709762,
    7709553,
    7709356,
    7709342,
    7702864,
    7685943,
    7684591,
    7684516,
    7679042,
    7679003,
    7664621,
    7664471,
    7664469,
    7659112,
    7659107,
    7653679,
    7653431,
    7653380,
    7650761,
    7602106,
    7589290,
    7586504,
    7574537,
    7574535,
    7574037,
    7573962,
    7570903,
    7570879,
    7561671,
    7560636,
    7558018,
    7557695,
    7556416,
    7556019,
    7555855,
    7555709,
    7554363,
    7551027,
    7550694,
    7550658,
    7550629,
    7549906,
    7549619,
    7548048,
    7537231,
    7532709,
    7531797,
    7530775,
    7528576,
    7526959,
    7526816,
    7523548,
    7517540,
    7511836,
    7509443,
    7499057,
    7494563,
    7494561,
    7490213,
    7489449,
    7478329,
    7478325,
    7477922,
    7477681,
    7474324,
    7467698,
    7467503,
    7465344,
    7465338,
    7465222,
    7465055,
    7465051,
    7465001,
    7464929,
    7464087,
    7460694,
    7460522,
    7457970,
    7453873,
    7453743,
    7453650,
    7446000,
    7445982,
    7435047,
    7435031,
    7435023,
    7435018,
    7435012,
    7435008,
    7435002,
    7413456,
    7408329,
    7396623,
    7396613,
    7396597,
    7389787,
    7383487,
    7368636,
    7368442,
    7364043,
    7362739,
    7362575,
    7362531,
    7357268,
    7352427,
    7352425,
    7352420,
    7352417,
    7352415,
    7351996,
    7351921,
    7351486,
    7349495,
    7340112,
    7339202,
    7337183,
    7337024,
    7336484,
    7333072,
    7333062,
    7331446,
    7331153,
    7331132,
    7331094,
    7331086,
    7331083,
    7320087,
    7319233,
    7319173,
    7312017,
    7312015,
    7298190,
    7298167,
    7295414,
    7295402,
    7291261,
    7287775,
    7287773,
    7287771,
    7287770,
    7281735,
    7276892,
    7254480,
    7254478,
    7254474,
    7250438,
    7250172,
    7250112,
    7250099,
    7241914,
    7238734,
    7238728,
    7233682,
    7233394,
    7233374,
    7225642,
    7225221,
    7225217,
    7221987,
    7221687,
    7218566
        };
        // **********************************
        private void processNewContract(long index, Dictionary<string, string> newHashDict, Dictionary<string, long> hashDecimalsDict)
        {
            var firstIndex = newHashDict.Keys.Select(p => getFirstBlockIndex(p)).Min();
            for (var startIndex = firstIndex; startIndex < index;) {
                var endIndex = startIndex + 100;
                if (endIndex > index) endIndex = index;
                var findStr = new JObject { { "blockNumber", new JObject { { "$gte", startIndex},{ "$lt", endIndex} } } }.ToString();
                var queryRes = mh.GetData(rConn.connStr, rConn.connDB, eventLogsCol, findStr);
                startIndex += 100;
                if (queryRes.Count == 0) continue;
                //
                var numberArr = queryRes.Select(p => (long)p["blockNumber"]).Distinct().ToArray();
                var res = queryRes.Select(p =>
                {
                    var hash = p["contractHash"].ToString();
                    if (!newHashDict.ContainsKey(p["contractHash"].ToString())) return null;
                    var dt = dataFormat(p, hashDecimalsDict.GetValueOrDefault(hash));
                    if (dt == null) return null;
                    dt["projId"] = newHashDict.GetValueOrDefault(hash);
                    dt["blockTime"] = long.Parse(p["timestamp"].ToString());
                    return dt;
                }).Where(p => p != null).ToArray();
                saveContractInfo(res);
            }
            transferType(newHashDict.Values.Distinct().ToArray());
        }
        private long getFirstBlockIndex(string hash)
        {
            var findStr = new JObject { { "contractHash", hash } }.ToString();
            var sortStr = "{'blockNumber':1}";
            var queryRes = mh.GetData(rConn.connStr, lConn.connDB, eventLogsCol, findStr, sortStr, 0, 1);
            if (queryRes.Count == 0) return 0;

            var item = queryRes[0];
            return long.Parse(item["blockNumber"].ToString());
        }
        private void saveContractInfo(JObject[] jtArr)
        {
            foreach(var item in jtArr)
            {
                if (item == null) continue;
                var findStr = new JObject { { "transactionHash", item["transactionHash"] }, { "event", item["event"] } }.ToString();
                if (mh.GetDataCount(lConn.connStr, lConn.connDB, notifyCol, findStr) == 0)
                {
                    mh.PutData(lConn.connStr, lConn.connDB, notifyCol, item);
                }
            }
        }
        private void transferType(string[] projIdArr)
        {
            foreach(var projId in projIdArr)
            {
                var findStr = new JObject { { "projId", projId } }.ToString();
                var updateStr = new JObject { { "$set", new JObject { { "type", MoloType.Init } } } }.ToString();
                mh.UpdateDataMany(lConn.connStr, lConn.connDB, projHashCol, updateStr, findStr);
            }
        }

        private JObject dataFormat(JToken jt, long fundDecimals)
        {
            var eventName = jt["event"].ToString();
            var types = (jt["argsTypes"] as JArray).ToArray();
            var args = (jt["args"] as JArray).ToArray();
            var names = MoloEventHelper.getEventParamNames(eventName);
            if(names == null || names.Length == 0) return null;

            int len = types.Length;
            if(len != args.Length || len != names.Length)
            {
                throw new Exception("event params not equal");
            }
            var data = new JObject ();
            for(int i=0; i<len; ++i)
            {
                var type = types[i].ToString();
                var arg = args[i].ToString();
                var name = names[i];
                data.Add(name, dataFormat(type, arg, name, fundDecimals));
            }
            data.Add("blockNumber", jt["blockNumber"]);
            data.Add("transactionHash", jt["transactionHash"]);
            data.Add("contractHash", jt["contractHash"].ToString());
            data.Add("address", jt["address"]);
            data.Add("event", eventName);
            
            return data;
        }
        private string dataFormat(string type, string val, string name="", long fundDecimals=1)
        {
            var div = BigInteger.One;
            if (fundDecimals > 0)
            {
                if (name == "tokenTribute" || name == "amount")
                {
                    div = new BigInteger(Math.Pow(10, fundDecimals));
                }
            }

            type = type.ToLower();
            if(type == "address")
            {
                return "0x" + val.Substring(26);
            }
            if(type == "uint256")
            {
                var num = new BigInteger(val.Substring(2).HexString2Bytes().Reverse().ToArray());
                var res = decimal.Parse(num.ToString())/decimal.Parse(div.ToString());
                return res.ToString();
                //return (new BigInteger(val.Substring(2).HexString2Bytes().Reverse().ToArray())/div).ToString();
            }
            if(type ==  "uint8")
            {
                return (new BigInteger(val.Substring(2).HexString2Bytes().Reverse().ToArray())/div).ToString();
            }
            if(type == "bool")
            {
                return new BigInteger(val.Substring(2).HexString2Bytes().Reverse().ToArray()).ToString();
            }
            throw new Exception("Not support type:"+type);
        }

        private Dictionary<string, string> getProjId(string[] hashArr, out Dictionary<string, string> newProjIdDict, out Dictionary<string, long> hashDecimalsDict)
        {
            var findStr = "{}"; 
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, projHashCol, findStr);
            if (queryRes.Count == 0)
            {
                newProjIdDict = new Dictionary<string, string>();
                hashDecimalsDict = new Dictionary<string, long>();
                return new Dictionary<string, string>();
            }
            //
            newProjIdDict = 
                queryRes.Where(p => p["type"].ToString() != MoloType.Init)
                .ToDictionary(k => k["contractHash"].ToString().ToLower(), v => v["projId"].ToString());
            //
            hashDecimalsDict =
                queryRes.ToDictionary(k => k["contractHash"].ToString().ToLower(), v => long.Parse(v["fundDecimals"].ToString()));
            //
            return queryRes.ToDictionary(k => k["contractHash"].ToString().ToLower(), v => v["projId"].ToString());
        }

        private void Log(long lh, long rh, string key = "logs")
        {
            Console.WriteLine("{0}.[{1}]processed: {2}/{3}", name(), key, lh, rh);
        }
        private void UpdateL(long index, long time, string key="logs")
        {
            var findStr = new JObject { { "counter", key } }.ToString();
            if(mh.GetDataCount(lConn.connStr, lConn.connDB, notifyCounter, findStr) == 0)
            {
                var newdata = new JObject { { "counter", key },{ "lastBlockIndex", index},{ "lastBlockTime", time} }.ToString();
                mh.PutData(lConn.connStr, lConn.connDB, notifyCounter, newdata);
                return;
            }
            var updateStr = new JObject { { "$set", new JObject { { "lastBlockIndex", index },{ "lastBlockTime", time} } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, notifyCounter, updateStr, findStr);

        }
        private long GetL(string key="logs")
        {
            
            var findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, notifyCounter, findStr);
            if (queryRes.Count == 0) return -1;
            return long.Parse(queryRes[0]["lastBlockIndex"].ToString());
        }
        //private long GetR(string key="transaction")
        private long GetR(string key, out long time)
        {
            time = -1;
            var findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(rConn.connStr, rConn.connDB, eventLogsCounter, findStr);
            if (queryRes.Count == 0) return -1;
            time = long.Parse(queryRes[0]["timestamp"].ToString());
            return long.Parse(queryRes[0]["lastUpdateIndex"].ToString());
        }
    }

    class MoloEventHelper
    {
        private static Dictionary<string, string[]> eventParamNames = new Dictionary<string, string[]>
        {
            /*
    event SubmitProposal(uint256 proposalIndex, address indexed delegateKey, address indexed memberAddress, address indexed applicant, uint256 tokenTribute, uint256 sharesRequested);
    event SubmitVote(uint256 indexed proposalIndex, address indexed delegateKey, address indexed memberAddress, uint8 uintVote);
    event ProcessProposal(uint256 indexed proposalIndex, address indexed applicant, address indexed memberAddress, uint256 tokenTribute, uint256 sharesRequested, bool didPass);
    event Ragequit(address indexed memberAddress, uint256 sharesToBurn);
    event Abort(uint256 indexed proposalIndex, address applicantAddress);
    event UpdateDelegateKey(address indexed memberAddress, address newDelegateKey);
    event SummonComplete(address indexed summoner, uint256 shares);
             */
            // molo.st ********************
            { "SubmitVote", new string[]{ "proposalIndex", "delegateKey", "memberAddress", "uintVote" } },
            { "SubmitProposal", new string[]{ "proposalIndex", "delegateKey", "memberAddress", "applicant", "tokenTribute", "sharesRequested"} },
            { "ProcessProposal", new string[]{ "proposalIndex", "applicant", "memberAddress", "tokenTribute", "sharesRequested", "didPass" } },
            { "Ragequit", new string[]{ "memberAddress", "sharesToBurn" } },
            { "Abort", new string[]{ "proposalIndex", "applicantAddress" } }, // applicantAddress = delegateKey
            { "UpdateDelegateKey", new string[]{ "memberAddress", "newDelegateKey" } },
            { "SummonComplete", new string[]{ "summoner", "shares" } },
            // fund
            //emit Withdrawal(receiver, amount);
            { "Withdrawal", new string[]{ "receiver", "amount" } },

            // molo.ed ********************


            // 管理合约 ************
            { "OnChangeSlope", new string[]{ "slope"} },      // 斜率   默认: 1000 * 10^9，  实际需除以1000
            { "OnChangeAlpha", new string[]{ "alpha"} },    // ratio  默认: 300， 实际需除以1000


            // 资金池合约 *********
            // 预挖矿
            { "OnPreMint", new string[]{"who", "tokenAmt", "timestamp" } },
            { "OnBuy", new string[]{ "who", "fundAmt", "tokenAmt"} },
            { "OnSell", new string[]{ "who", "fundAmt", "tokenAmt"} },
            // 项目方取钱
            { "OnSendEth", new string[]{ "who", "fundAmt"} },
            // 投资方清退
            { "OnClearing", new string[]{ "who", "ratio", "fundAmt30", "fundAmt70"} },
            // 项目方盈利
            { "OnRevenue", new string[]{ "who", "fundAmt"} },
            // 订单支付确认
            { "OnEvent", new string[]{ "orderId"} },
            

            // 自治合约 *********
            { "OnGetFdtOut", new string[]{ "who", "amt"} },
            { "OnSetFdtIn", new string[]{ "who", "amt"} },
            { "OnLock", new string[]{ "contractAddress", "lockAddress", "index", "expireDate", "lockAmount"} },
            { "OnFree", new string[]{ "contractAddress", "lockAddress", "index", "expireDate"} },

            // 代币合约 *********
            { "Transfer", new string[]{ "from", "to", "amt"} },
            { "Approval", new string[]{ "owner", "spender", "amt"} },
            //
            { "OnApplyProposal", new string[]{ "index", "proposalName", "proposaler", "startTime", "recipient", "fundAmt","timeConsuming","proposalDetail"} },
            { "OnVote", new string[]{ "who", "index", "voteResult", "shares"} },
            { "OnProcess", new string[]{ "index", "pass"} },
            { "OnAbort", new string[]{ "index"} },
            { "OnOneTicketRefuse", new string[]{ "index"} },
            //
            { "OnApplyClearingProposal", new string[]{ "index", "proposaler", "address", "startTime"} },
            { "OnVoteClearingProposal", new string[]{ "who", "index", "fdtAmt"} },
            { "OnProcessClearingProposal", new string[]{ "index", "pass"} }
        };

        public static string[] getEventParamNames(string eventName)
        {
            return eventParamNames.GetValueOrDefault(eventName);
        }
    }

    class MoloType
    {
        public const string Init = "0";
        public const string Add = "1";
    }
}

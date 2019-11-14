using MongoDB.Bson;
using NEL.NNS.lib;
using NEL_FutureDao_BT.core;
using NEL_FutureDao_BT.lib;
using NEL_FutureDao_BT.task.dao;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace NEL_FutureDao_BT.task
{
    /// <summary>
    /// 
    /// 计算:
    /// 代币名称          : 
    /// 代币已发行数量    : OnPreMint + OnBuy.token - OnSell.token  ----> Transfer.from(nil) - Transfer.to(nil)
    /// 预留代币总额      : OnPreMint
    /// 预留代币(未解锁)  : 定时计算
    /// 预留代币(已解锁)  : 定时计算
    /// 资产名称          : 
    /// 资金池总额        : 治理池 + 储备池
    /// 治理池金额        : OnBuy.fund * 0.7 - OnSendEth.fund(项目方取钱) - OnClearing.fund1(投资方清退)
    /// 储备池金额        : OnBuy.fund * 0.3 - OnSell.fund - OnClearing.fund2(投资方清退) + OnRevenue.fund(项目方盈利)
    /// 储备比例          : 
    /// 价格增速          : 
    /// 
    /// 
    /// 个人代币余额 = onBuy - onSell
    /// 个人可用代币 = 预留已解锁的 + onBuy - onSell - onVote(投票) + onVote(正常结束/取消/终止)
    /// 个人锁定代币 = 预留未解锁的 + onVote(投票)
    /// 
    /// </summary>
    class ProjFinanceTask : AbsTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo daoConn;
        private string notifyCounter = "daonotifycounters";
        private string notifyCol = "daonotifyinfos";
        private string daoFinanceCol = "daoprojfinanceinfos";
        private string daoFinanceHashCol = "daoprojfinancehashinfos";
        private string daoFinanceRewardCol = "daoprojfinancerewardinfos";
        //
        private string daoFinanceFundPoolCol = "daoprojfinancefundpoolinfos";
        private string daoFinanceReserveTokenHistCol = "daoprojfinancereservetokenhistinfos";
        private string daoFinanceParamSetHistCol = "daoprojfinanceparamsethistinfos";
        private string daoFinancePriceHistCol = "daoprojfinancepricehistinfos";
        private long OneHourSeconds = 3600;
        private string NilAddr = "0x0000000000000000000000000000000000000000";

        public ProjFinanceTask(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            daoConn = Config.localDbConnInfo;
            //
            initIndex();
        }

        private void initIndex()
        {
            mh.setIndex(daoConn.connStr, daoConn.connDB, daoFinanceCol, "{'projId':1}", "i_projId");
            mh.setIndex(daoConn.connStr, daoConn.connDB, daoFinanceHashCol, "{'contractHash':1}", "i_contractHash");
            mh.setIndex(daoConn.connStr, daoConn.connDB, daoFinanceRewardCol, "{'projId':1,'activeState':1}", "i_projId_activeState");
            mh.setIndex(daoConn.connStr, daoConn.connDB, daoFinanceRewardCol, "{'projId':1,'rewardId':1}", "i_projId_rewardId");
            mh.setIndex(daoConn.connStr, daoConn.connDB, daoFinanceFundPoolCol, "{'projId':1}", "i_projId");
            mh.setIndex(daoConn.connStr, daoConn.connDB, daoFinanceFundPoolCol, "{'tokenUnlockFinishFlag':1,'tokenLastUpdateTime':1}", "i_tokenUnlockFinishFlag_tokenLastUpdateTime");
            mh.setIndex(daoConn.connStr, daoConn.connDB, daoFinanceReserveTokenHistCol, "{'transactionHash':1}", "i_transactionHash");
            mh.setIndex(daoConn.connStr, daoConn.connDB, daoFinanceParamSetHistCol, "{'transactionHash':1,'event':1}", "i_transactionHash_event");
            mh.setIndex(daoConn.connStr, daoConn.connDB, daoFinancePriceHistCol, "{'projId':1,'recordType':1}", "i_projId_recordType");
        }

        public override void process()
        {
            Sleep(2000);
            processSubTask(processReserveToken);
            processSubTask(processReserveFundRatio);
            processSubTask(processFundPool);
            processSubTask(processUnLockToken);
        }
        private void processSubTask(Action action)
        {
            try
            {
                action();
            } catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        // 预留代币历史和总额
        private void processReserveToken()
        {
            string key = "ReserveToken";
            long rh = getR();
            long lh = getL(key);
            if (lh >= rh)
            {
                log(key, lh, rh);
                return;
            }
            for (long index = lh + 1; index <= rh; ++index)
            {
                string findStr = new JObject { { "blockNumber", index }, { "event", "OnPreMint" } }.ToString();
                var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, notifyCol, findStr);
                if (queryRes.Count == 0)
                {
                    updateL(key, index);
                    log(key, index, rh);
                    continue;
                }
                // 
                foreach (var item in queryRes)
                {
                    // 预留代币历史
                    findStr = new JObject { { "transactionHash", item["transactionHash"] } }.ToString();
                    if (mh.GetDataCount(daoConn.connStr, daoConn.connDB, daoFinanceReserveTokenHistCol, findStr) == 0)
                    {
                        item["timestamp"] = long.Parse(item["timestamp"].ToString());
                        mh.PutData(daoConn.connStr, daoConn.connDB, daoFinanceReserveTokenHistCol, item.ToString());
                    }
                }

                // 统计锁仓总量
                queryRes.Select(p => p["projId"].ToString()).Distinct().ToList().ForEach(p => handleProjReserveTokenTotal(p));

                updateL(key, index);
                log(key, index, rh);
            }
        }
        private void handleProjReserveTokenTotal(string projId)
        {
            //
            var whereStr = new JObject { { "projId", projId } }.ToString();
            if (mh.GetDataCount(daoConn.connStr, daoConn.connDB, daoFinanceFundPoolCol, whereStr) == 0)
            {
                var totalFlag = UnlockFinishFlag.Not;
                if (getProjReserveInfo(projId, out decimal total, out string address, out string fundName, out string tokenName))
                {
                    //totalFlag = UnlockFinishFlag.Yes;
                }
                var dbZERO = decimal.Zero.format();
                var now = TimeHelper.GetTimeStamp();
                var newdata = new FundPoolInfo
                {
                    projId = projId,
                    tokenName = tokenName,
                    tokenLockTotal = total.format(),
                    tokenLockTotalAddress = address,
                    tokenUnlockNotAmount = total.format(),
                    tokenUnlockYesAmount = dbZERO,
                    tokenUnlockFinishFlag = totalFlag,
                    tokenLastUpdateTime = TimeHelper.GetTimeStamp(),
                    fundName = fundName,
                    fundPoolTotal = dbZERO,
                    fundPoolTotalTp = dbZERO,
                    fundManagePoolTotal = dbZERO,
                    fundManagePoolTotalTp = dbZERO,
                    fundReservePoolTotal = dbZERO,
                    fundReservePoolTotalTp = dbZERO,
                    //
                    fundReserveRatio = dbZERO,
                    priceRaiseSpeed = dbZERO,
                    //
                    hasOnBuyFundTotal = dbZERO,
                    hasOnBuyFundTotalTp = dbZERO,
                    hasIssueTokenTotal = dbZERO,
                    hasIssueTokenTotalTp = dbZERO,
                    time = now,
                    lastUpdateTime = now,
                };
                mh.PutData<FundPoolInfo>(daoConn.connStr, daoConn.connDB, daoFinanceFundPoolCol, newdata);
            }
        }
        private bool getProjReserveInfo(string projId, out decimal total, out string address, out string fundName, out string tokenName)
        {
            //
            total = 0;
            address = "";
            fundName = "";
            tokenName = "";
            var findStr = new JObject { { "projId", projId } }.ToString();
            var fieldStr = new JObject { { "reserveTokenFlag", 1 },{ "reserveTokenInfo", 1 }, { "fundName", 1 }, { "tokenSymbol", 1 } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, daoFinanceCol, findStr, fieldStr);
            if (queryRes.Count == 0) return false;
            //
            var item = queryRes[0];
            if(item["reserveTokenFlag"].ToString() == "1")
            {
                var rInfo = item["reserveTokenInfo"] as JArray;
                if(rInfo.Count > 0)
                {
                    var info = rInfo.SelectMany(p => p["info"] as JArray).ToList();
                    if(info.Count > 0)
                    {
                        total = info.Sum(p => decimal.Parse(p["amt"].ToString()));
                    }
                    address = rInfo[0]["address"].ToString();
                }
                //var info = item["reserveTokenInfo"]["info"] as JArray;
                //total = info.Sum(p => decimal.Parse(p["amt"].ToString()));
                //address = item["reserveTokenInfo"]["address"].ToString();
            }
            fundName = item["fundName"].ToString();
            tokenName = item["tokenSymbol"].ToString();
            return true;
        }



        // 参数设置(斜率slope和比例alpha)
        private void processReserveFundRatio()
        {
            string key = "param";
            long rh = getR();
            long lh = getL(key);
            if (lh >= rh)
            {
                log(key, lh, rh);
                return;
            }

            for (var index=lh+1; index<=rh; ++index)
            {
                string findStr = new JObject { { "blockNumber", index }, { "$or", new JArray { new JObject { { "event", "OnChangeSlope" } }, new JObject { { "event", "OnChangeAlpha" } } } } }.ToString();
                var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, notifyCol, findStr);
                if(queryRes.Count == 0)
                {
                    updateL(key, index);
                    log(key, index, rh);
                    continue;
                }
                foreach (var item in queryRes)
                {
                    //
                    var projId = item["projId"].ToString();
                    handleProjReserveTokenTotal(projId);
                    // 参数设置历史
                    findStr = new JObject { { "transactionHash", item["transactionHash"]},{ "event", item["event"]} }.ToString();
                    if(mh.GetDataCount(daoConn.connStr, daoConn.connDB, daoFinanceParamSetHistCol, findStr) ==0)
                    {
                        mh.PutData(daoConn.connStr, daoConn.connDB, daoFinanceParamSetHistCol, item.ToString());
                    }

                    // 参数设置汇总
                    findStr = new JObject { { "projId", projId} }.ToString();
                    BsonDocument updateBson = null;
                    string eventName = item["event"].ToString();
                    if(eventName == "OnChangeSlope")
                    {
                        // 斜率
                        var val = decimal.Parse(item["slope"].ToString());  // 除以1000
                        updateBson = new BsonDocument { { "$set", new BsonDocument
                        {
                            { "priceRaiseSpeed", (val/1000).format()}
                        } } };
                        mh.UpdateDecimal(daoConn.connStr, daoConn.connDB, daoFinanceFundPoolCol, updateBson, findStr);
                    } else
                    {
                        var val = decimal.Parse(item["alpha"].ToString());  // 除以1000
                        updateBson = new BsonDocument { { "$set", new BsonDocument
                        {
                            { "fundReserveRatio", (val/1000).format()}
                        } } };
                        mh.UpdateDecimal(daoConn.connStr, daoConn.connDB, daoFinanceFundPoolCol, updateBson, findStr);
                    }
                }

                updateL(key, index);
                log(key, index, rh);
            }
        }


        // 资金池金额(资金池(已售出)/治理池/储备池/已发行)
        private void processFundPool()
        {
            string key = "fundPool";
            long rh = getR();
            long lh = getL(key);
            if (lh >= rh)
            {
                log(key, lh, rh);
                return;
            }
            long now = TimeHelper.GetTimeStamp();
            var dbZERO = decimal.Zero.format();
            for (var index = lh + 1; index <= rh; ++index)
            {
                // 获取不同项目的资金池数据信息
                var findStr = new JObject { { "blockNumber", index} }.ToString();
                var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, notifyCol, findStr);
                if (queryRes.Count == 0)
                {
                    updateL(key, index);
                    log(key, index, rh);
                    continue;
                }
                //
                var res = 
                queryRes.GroupBy(p => p["projId"], (k, g) =>
                {
                    /// (eth)资金池总额        : 治理池 + 储备池
                    /// (eth)治理池金额        : OnBuy.fund * 0.7 - OnSendEth.fund(项目方取钱) - OnClearing.fund1(投资方清退)
                    /// (eth)储备池金额        : OnBuy.fund * 0.3 - OnSell.fund - OnClearing.fund2(投资方清退) + OnRevenue.fund(项目方盈利)
                    /// (eth)已售出总额        : OnBuy.fund
                    /// 
                    /// (dai)代币已发行数量    : OnPreMint + OnBuy.token - OnSell.token  ----> Transfer.from(nil) - Transfer.to(nil)

                    var pt = decimal.Zero;
                    var pt1 = decimal.Zero;
                    var pt2 = decimal.Zero;
                    var hasOnBuyFundTotal = decimal.Zero;
                    var hasIssueTokenTotal = decimal.Zero;

                    foreach (var it in g)
                    {
                        // OnPreMint + OnBuy + OnSell + OnSendEth(取钱) + OnClearing(清退) + OnRevenue + OnEvent
                        var eventName = it["event"].ToString();
                        if(eventName == "OnPreMint")
                        {
                            continue;
                        }
                        if (eventName == "OnBuy")
                        {
                            var fundAmt = decimal.Parse(it["fundAmt"].ToString());
                            var tokenAmt = decimal.Parse(it["tokenAmt"].ToString());
                            pt1 += fundAmt * decimal.Parse("0.7");
                            pt2 += fundAmt * decimal.Parse("0.3");
                            hasOnBuyFundTotal += fundAmt;
                            continue;
                        }
                        if (eventName == "OnSell")
                        {
                            var fundAmt = decimal.Parse(it["fundAmt"].ToString());
                            var tokenAmt = decimal.Parse(it["tokenAmt"].ToString());
                            pt2 -= fundAmt;
                            continue;
                        }
                        if (eventName == "OnSendEth")
                        {
                            var fundAmt = decimal.Parse(it["fundAmt"].ToString());
                            pt1 -= fundAmt;
                            continue;
                        }
                        if (eventName == "OnClearing")
                        {
                            var fundAmt1 = decimal.Parse(it["fundAmt1"].ToString());
                            var fundAmt2 = decimal.Parse(it["fundAmt2"].ToString());
                            pt1 -= fundAmt1;
                            pt2 -= fundAmt2;
                            continue;
                        }
                        if (eventName == "OnRevenue")
                        {
                            var fundAmt = decimal.Parse(it["fundAmt"].ToString());
                            pt2 += fundAmt;
                            continue;
                        }
                        if (eventName == "Transfer")
                        {
                            var from = it["from"].ToString();
                            var to = it["to"].ToString();
                            var amt = decimal.Parse(it["amt"].ToString());
                            if(from == NilAddr && to != NilAddr)
                            {
                                hasIssueTokenTotal += amt;
                            }
                            if(from != NilAddr && to == NilAddr)
                            {
                                hasIssueTokenTotal -= amt;
                            }
                            continue;
                        }
                    }
                    pt = pt1 + pt2;
                    return new
                    {
                        projId = k.ToString(),
                        fundPool = pt,
                        managePool = pt1,
                        reservePool = pt2,
                        hasOnBuyFundTotal = hasOnBuyFundTotal,
                        hasIssueTokenTotal = hasIssueTokenTotal
                    };
                }).ToArray();
                //
                foreach (var item in res)
                {
                    if(item.fundPool == 0 
                        && item.managePool == 0 
                        && item.reservePool == 0 
                        && item.hasOnBuyFundTotal == 0
                        && item.hasIssueTokenTotal == 0)
                    {
                        continue;
                    }
                    var projId = item.projId;
                    handleProjReserveTokenTotal(projId);
                    //
                    var subfindStr = new JObject { { "projId", projId} }.ToString();
                    var subres = mh.GetData<FundPoolInfo>(daoConn.connStr, daoConn.connDB, daoFinanceFundPoolCol, subfindStr);
                    var olddata = subres[0];
                    var fundPool = olddata.fundPoolTotal.format();
                    var fundPoolTp = olddata.fundPoolTotalTp.format();
                    var managePool = olddata.fundManagePoolTotal.format();
                    var managePoolTp = olddata.fundManagePoolTotalTp.format();
                    var reservePool = olddata.fundReservePoolTotal.format();
                    var reservePoolTp = olddata.fundReservePoolTotalTp.format();
                    var hasOnBuyFundTotal = olddata.hasOnBuyFundTotal.format();
                    var hasOnBuyFundTotalTp = olddata.hasOnBuyFundTotalTp.format();
                    var hasIssueTokenTotal = olddata.hasIssueTokenTotal.format();
                    var hasIssueTokenTotalTp = olddata.hasIssueTokenTotalTp.format();
                    
                    if (fundPoolTempNotClearAllFlag)
                    {
                        fundPoolTp = decimal.Zero;
                        managePoolTp = decimal.Zero;
                        reservePoolTp = decimal.Zero;
                        hasOnBuyFundTotalTp = decimal.Zero;
                        hasIssueTokenTotalTp = decimal.Zero;
                    }
                    fundPool += item.fundPool - fundPoolTp;
                    fundPoolTp = item.fundPool;
                    managePool += item.managePool - managePoolTp;
                    managePoolTp = item.managePool;
                    reservePool += item.reservePool - reservePoolTp;
                    reservePoolTp = item.reservePool;
                    hasOnBuyFundTotal += item.hasOnBuyFundTotal - hasOnBuyFundTotalTp;
                    hasOnBuyFundTotalTp = item.hasOnBuyFundTotal;
                    hasIssueTokenTotal += item.hasIssueTokenTotal - hasIssueTokenTotalTp;
                    hasIssueTokenTotalTp = item.hasIssueTokenTotal;

                    var updateBson = new BsonDocument { { "$set", new BsonDocument
                    {
                        { "fundPoolTotal", fundPool.format()},
                        { "fundPoolTotalTp", fundPoolTp.format()},
                        { "fundManagePoolTotal", managePool.format()},
                        { "fundManagePoolTotalTp", managePoolTp.format()},
                        { "fundReservePoolTotal", reservePool.format()},
                        { "fundReservePoolTotalTp", reservePoolTp.format()},
                        { "hasOnBuyFundTotal", hasOnBuyFundTotal.format()},
                        { "hasOnBuyFundTotalTp", hasOnBuyFundTotalTp.format()},
                        { "hasIssueTokenTotal", hasIssueTokenTotal.format()},
                        { "hasIssueTokenTotalTp", hasIssueTokenTotalTp.format()},
                    } } };
                    mh.UpdateDecimal(daoConn.connStr, daoConn.connDB, daoFinanceFundPoolCol, updateBson, subfindStr);
                }
                //
                updateL(key, index);
                log(key, index, rh);

                // 清除临时字段数据
                fundPoolTempNotClearAllFlag = false;
                try
                {
                    foreach (var item in res)
                    {
                        var projId = item.projId;
                        string subfindStr = new JObject { { "projId", projId } }.ToString();
                        var updateBson = new BsonDocument { { "$set", new BsonDocument{
                            {"fundPoolTotalTp", dbZERO},
                            {"fundManagePoolTotalTp", dbZERO},
                            {"fundReservePoolTotalTp", dbZERO},
                            {"hasOnBuyFundTotalTp", dbZERO},
                            {"hasIssueTokenTotalTp", dbZERO},
                        }} };
                        mh.UpdateDecimal(daoConn.connStr, daoConn.connDB, daoFinanceFundPoolCol, updateBson, subfindStr);
                    }
                } catch(Exception ex)
                {
                    // 
                    Console.WriteLine(ex);
                    fundPoolTempNotClearAllFlag = true;
                }
            }
        }
        private bool fundPoolTempNotClearAllFlag = false;


        // 预留代币已解锁和未解锁(定时循环)
        private void processUnLockToken()
        {
            long now = TimeHelper.GetTimeStamp();
            Console.WriteLine("UnlockToken timer is running");

            var findStr = new JObject { { "tokenUnlockFinishFlag", UnlockFinishFlag.Not }, { "tokenLastUpdateTime", new JObject { { "$lt", now - OneHourSeconds } } } }.ToString();
            var queryRes = mh.GetData<FundPoolInfo>(daoConn.connStr, daoConn.connDB, daoFinanceFundPoolCol, findStr);
            if (queryRes.Count == 0) return;
            //
            queryRes.ForEach(item => handleUnLockToken(item, now));
        }
        private void handleUnLockToken(FundPoolInfo item, long now)
        {
            var projId = item.projId;
            var findStr = new JObject { { "projId", projId } }.ToString();
            var updateStr = "";
            if (item.tokenLockTotal.format() == item.tokenUnlockYesAmount.format())
            {
                updateStr = new JObject { { "$set", new JObject { { "tokenUnlockFinishFlag", UnlockFinishFlag.Yes } } } }.ToString();
                mh.UpdateData(daoConn.connStr, daoConn.connDB, daoFinanceFundPoolCol, updateStr, findStr);
                return;
            }
            var fieldStr = new JObject { { "tokenAmt", 1 }, { "timestamp", 1 } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, daoFinanceReserveTokenHistCol, findStr, fieldStr);
            if (queryRes.Count == 0) return;

            var sum = queryRes.Sum(p => decimal.Parse(p["tokenAmt"].ToString()));
            var unlockY = queryRes.Sum(p =>
            {
                if (long.Parse(p["timestamp"].ToString()) < now) return 0;
                return decimal.Parse(p["tokenAmt"].ToString());
            });
            var unlockN = queryRes.Sum(p =>
            {
                if (long.Parse(p["timestamp"].ToString()) >= now) return 0;
                return decimal.Parse(p["tokenAmt"].ToString());
            });
            bool flag = false;
            var updateJo = new BsonDocument { };
            if (sum != item.tokenLockTotal)
            {
                updateJo.Add("tokenLockTotal", sum.format());
                flag = true;
            }
            if (unlockY != item.tokenUnlockYesAmount)
            {
                updateJo.Add("tokenUnlockYesAmount", unlockY.format());
                flag = true;
            }
            if (unlockN != item.tokenUnlockNotAmount)
            {
                updateJo.Add("tokenUnlockNotAmount", unlockN.format());
                if (unlockN == decimal.Zero)
                {
                    updateJo.Add("tokenUnlockFinishFlag", UnlockFinishFlag.Yes);
                }
                flag = true;
            }

            if (flag)
            {
                updateJo.Add("tokenLastUpdateTime", now);
                var updateBson = new BsonDocument { { "$set", updateJo } };
                mh.UpdateDecimal(daoConn.connStr, daoConn.connDB, daoFinanceFundPoolCol, updateBson, findStr);
            }
        }


        // #######################
        private void log(string key, long lh, long rh)
        {
            Console.WriteLine("{0}.[{1}]processed: {2}/{3}", name(), key, lh, rh);
        }
        private void updateL(string key, long hh)
        {
            string findStr = new JObject { { "counter", key } }.ToString();
            if(mh.GetDataCount(daoConn.connStr, daoConn.connDB, notifyCounter, findStr) == 0)
            {
                string newdata = new JObject { { "counter", key},{ "lastBlockIndex", hh} }.ToString();
                mh.PutData(daoConn.connStr, daoConn.connDB, notifyCounter, newdata);
                return;
            }
            string updateStr = new JObject { { "$set", new JObject { { "lastBlockIndex", hh } } } }.ToString();
            mh.UpdateData(daoConn.connStr, daoConn.connDB, notifyCounter, updateStr, findStr);
        }
        private long getL(string key)
        {
            string findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, notifyCounter, findStr);
            if (queryRes.Count == 0) return -1;
            return long.Parse(queryRes[0]["lastBlockIndex"].ToString());
        }
        private long getR(string key = "logs")
        {
            string findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, notifyCounter, findStr);
            if (queryRes.Count == 0) return 0;
            return long.Parse(queryRes[0]["lastBlockIndex"].ToString());
        }
    }
    class UnlockFinishFlag
    {
        public const string Not = "0";
        public const string Yes = "1";
    }
    class SkOp
    {
        public const string NotOp = "3";        // 未操作 
        public const string HandlingOp = "4";   // 处理中
        public const string FinishOp = "5";     // 已完成
    }
}

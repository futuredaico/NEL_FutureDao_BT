using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace NEL_FutureDao_BT.task.dao
{
    class Dao
    {
        public static BsonDecimal128 ZERO = BsonDecimal128.Create(0);
    }
    class ProposalType
    {
        public const string ApplyFund = "AppFund";
        public const string ApplyClear = "AppClear";
    }
    class ProposalState
    {
        public const string Voting = "voting";          // 投票中
        public const string Bullettining = "bullettining";   // 公示中
        public const string Executing = "executing";    // 执行中
        public const string Finished = "finished";      // 正常结束
        public const string NoPass = "nopass";          // 投票未通过
        public const string Stop = "stop";              // 已终止
        public const string Cancel = "cancel";          // 已取消
    }
    [BsonIgnoreExtraElements]
    class ProposalInfo
    {
        public ObjectId _id;
        public string projId { get; set; }
        public string index { get; set; }
        public string proposalName { get; set; }
        public string proposalType { get; set; }
        public string proposalerAddress { get; set; }
        public BsonDecimal128 proposalFund { get; set; }
        public string recipientAdress { get; set; }
        public string timeConsuming { get; set; } // 发放方式: 0表示立即发放; > 0表示按天发放
        public string proposalDetail { get; set; }
        public string proposalState { get; set; }
        public long startTime { get; set; } // 提案开始时间
        public long bulletinTime { get; set; } // 公示开始时间
        public long voteProCount { get; set; }
        public long voteConCount { get; set; }
    }
    [BsonIgnoreExtraElements]
    class TokenBalanceInfo
    {
        public ObjectId _id { get; set; }
        public string projId { get; set; }
        public string fundName { get; set; }
        public string tokenName { get; set; }
        public string address { get; set; }

        public BsonDecimal128 onPreMint { get; set; } = Dao.ZERO;           // 预留总额     
        public BsonDecimal128 onPreMintTp { get; set; } = Dao.ZERO;               
        public BsonDecimal128 onPreMintAtEnd { get; set; } = Dao.ZERO;      // 预留可用的(定时计算)
        public bool onPreMintAtEndFinishFlag { get; set; } = true;      // 预留可用的(定时计算), true表示已完成
        public long onPreMintAtEndTime { get; set; } = 0;      // 预留可用的(定时计算)
        public BsonDecimal128 onBuy { get; set; } = Dao.ZERO;
        public BsonDecimal128 onBuyTp { get; set; } = Dao.ZERO;
        public BsonDecimal128 onSell { get; set; } = Dao.ZERO;
        public BsonDecimal128 onSellTp { get; set; } = Dao.ZERO;
        public BsonDecimal128 transferFrom { get; set; } = Dao.ZERO;
        public BsonDecimal128 transferFromTp { get; set; } = Dao.ZERO;
        public BsonDecimal128 transferTo { get; set; } = Dao.ZERO;
        public BsonDecimal128 transferToTp { get; set; } = Dao.ZERO;
        public BsonDecimal128 onSetFdtIn { get; set; } = Dao.ZERO;
        public BsonDecimal128 onSetFdtInTp { get; set; } = Dao.ZERO;
        public BsonDecimal128 onGetFdtOut { get; set; } = Dao.ZERO;
        public BsonDecimal128 onGetFdtOutTp { get; set; } = Dao.ZERO;
        public BsonDecimal128 onVote { get; set; } = Dao.ZERO;            // 投票总额                 
        public BsonDecimal128 onVoteTp { get; set; } = Dao.ZERO;                         
        public BsonDecimal128 onVoteAtEnd { get; set; } = Dao.ZERO;       // 投票结束释放总额(定时计算)
        public BsonDecimal128 onProcess { get; set; } = Dao.ZERO;
        public BsonDecimal128 onProcessTp { get; set; } = Dao.ZERO;

        public BsonDecimal128 balance { get; set; } = Dao.ZERO;
        public BsonDecimal128 availableAmt { get; set; } = Dao.ZERO;
        public BsonDecimal128 tokenAmt { get; set; } = Dao.ZERO;
        public BsonDecimal128 shareAmt { get; set; } = Dao.ZERO;
        public BsonDecimal128 lockAmt { get; set; } = Dao.ZERO;
        public long lastUpdateTime { get; set; } = 0;
    }
    [BsonIgnoreExtraElements]
    class TokenPriceInfo
    {
        public ObjectId _id { get; set; }
        public string ob_address { get; set; }
        public BsonDecimal128 ob_fundAmt { get; set; } = BsonDecimal128.Create("0");
        public BsonDecimal128 ob_tokenAmt { get; set; } = BsonDecimal128.Create("0");
        public BsonDecimal128 ob_price { get; set; } = BsonDecimal128.Create("0");
        public string ob_txid { get; set; }
        public long ob_blockTime { get; set; }
        public string os_address { get; set; }
        public BsonDecimal128 os_fundAmt { get; set; } = BsonDecimal128.Create("0");
        public BsonDecimal128 os_tokenAmt { get; set; } = BsonDecimal128.Create("0");
        public BsonDecimal128 os_price { get; set; } = BsonDecimal128.Create("0");
        public string os_txid { get; set; }
        public long os_blockTime { get; set; }
        public string contractHash { get; set; }
        public string projId { get; set; }
        public long recordTime { get; set; }
        public int recordType { get; set; }

    }
    [BsonIgnoreExtraElements]
    class FundPoolInfo
    {
        public ObjectId _id { get; set; }
        public string projId { get; set; }
        public string tokenName { get; set; }
        //
        public string tokenLockTotalAddress { get; set; }
        public BsonDecimal128 tokenLockTotal { get; set; }
        public BsonDecimal128 tokenUnlockNotAmount { get; set; }
        public BsonDecimal128 tokenUnlockYesAmount { get; set; }
        public string tokenUnlockFinishFlag { get; set; }
        public long tokenLastUpdateTime { get; set; } = 0;
        public string fundName { get; set; }
        public BsonDecimal128 fundPoolTotal { get; set; }
        public BsonDecimal128 fundPoolTotalTp { get; set; }
        public BsonDecimal128 fundManagePoolTotal { get; set; }
        public BsonDecimal128 fundManagePoolTotalTp { get; set; }
        public BsonDecimal128 fundReservePoolTotal { get; set; }
        public BsonDecimal128 fundReservePoolTotalTp { get; set; }
        public BsonDecimal128 fundReserveRatio{ get; set; }
        public BsonDecimal128 priceRaiseSpeed{ get; set; }
        //
        public BsonDecimal128 hasIssueTokenTotal { get; set; }
        public BsonDecimal128 hasIssueTokenTotalTp { get; set; }
        public BsonDecimal128 hasOnBuyFundTotal { get; set; }
        public BsonDecimal128 hasOnBuyFundTotalTp { get; set; }
        public long hasSupport { get; set; } = 0;
        public long time { get; set; }
        public long lastUpdateTime { get; set; }
    }
}

using Newtonsoft.Json.Linq;

namespace NEL_FutureDao_BT.core
{
    /// <summary>
    /// 任务接口
    /// 
    /// </summary>    
    interface ITask
    {
        /// <summary>
        /// 
        /// 任务名称
        /// 
        /// </summary>
        /// <returns></returns>
        string name();

        /// <summary>
        /// 初始化配置
        /// 
        /// </summary>
        /// <param name="config"></param>
        void Init(JObject config);

        /// <summary>
        /// 启动任务
        /// 
        /// </summary>
        void Start();

        /// <summary>
        /// 
        /// 启动成功个数据
        /// </summary>
        /// <returns></returns>
        bool isRuning();
    }
}

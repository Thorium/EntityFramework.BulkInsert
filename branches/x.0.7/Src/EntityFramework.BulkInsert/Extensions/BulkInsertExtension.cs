using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Data.SqlClient;

namespace EntityFramework.BulkInsert.Extensions
{
    public static class BulkInsertExtension
    {
        internal const int DefaultBatchSize = 5000;

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="entities"></param>
        /// <param name="batchSize"></param>
        public static void BulkInsert<T>(this DbContext context, IEnumerable<T> entities, int batchSize = DefaultBatchSize)
        {
            var bulkInsert = ProviderFactory.Get(context);
            bulkInsert.Run(entities, SqlBulkCopyOptions.Default, DefaultBatchSize);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="entities"></param>
        /// <param name="options"></param>
        /// <param name="batchSize"></param>
        public static void BulkInsert<T>(this DbContext context, IEnumerable<T> entities, SqlBulkCopyOptions options, int batchSize = DefaultBatchSize)
        {
            var bulkInsert = ProviderFactory.Get(context);
            bulkInsert.Run(entities, options, DefaultBatchSize);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="entities"></param>
        /// <param name="transaction"></param>
        /// <param name="options"></param>
        /// <param name="batchSize"></param>
        public static void BulkInsert<T>(this DbContext context, IEnumerable<T> entities, IDbTransaction transaction, SqlBulkCopyOptions options = SqlBulkCopyOptions.Default, int batchSize = DefaultBatchSize)
        {
            var bulkInsert = ProviderFactory.Get(context);
            bulkInsert.Run(entities, transaction, options, DefaultBatchSize);
        }

        /*
        public static void BulkInsert<T>(this DbContext context, IEnumerable<T> entities,
            Func<BulkInsertOptions, BulkInsertOptions> options)
        {
            var bulkInsert = ProviderFactory.Get(context);
            bulkInsert.Run(entities, options(new BulkInsertOptions()));
        }
         * */
    }

    internal class BulkInsertOptions
    {
        public int BatchSizeValue { get; private set; }
        public SqlBulkCopyOptions SqlBulkCopyOptionsValue { get; private set; }
        public int TimeOutValue { get; private set; }
        public SqlRowsCopiedEventHandler CallbackMethod { get; private set; }
        public int NotifyAfterValue { get; private set; }

#if !NET40
        public bool EnableStreamingValue { get; private set; }
#endif

        internal BulkInsertOptions()
        {
            BatchSizeValue = BulkInsertExtension.DefaultBatchSize;
            TimeOutValue = 30;
        }

        /// <summary>
        /// Sets batch size
        /// </summary>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        public BulkInsertOptions BatchSize(int batchSize)
        {
            BatchSizeValue = batchSize;
            return this;
        }

        /// <summary>
        /// Sets sql bulk copy timeout in seconds
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public BulkInsertOptions TimeOut(int timeout)
        {
            TimeOutValue = timeout;
            return this;
        }

        /// <summary>
        /// Sets SqlBulkCopy options
        /// </summary>
        /// <param name="sqlBulkCopyOptions"></param>
        /// <returns></returns>
        public BulkInsertOptions SqlBulkCopyOptions(SqlBulkCopyOptions sqlBulkCopyOptions)
        {
            SqlBulkCopyOptionsValue = sqlBulkCopyOptions;
            return this;
        }

        /// <summary>
        /// Sets callback method for sql bulk insert
        /// </summary>
        /// <param name="callback"></param>
        /// <param name="interval">Numbers of rows processed before callback is invoked</param>
        /// <returns></returns>
        public BulkInsertOptions Callback(SqlRowsCopiedEventHandler callback, int interval)
        {
            CallbackMethod = callback;
            NotifyAfterValue = interval;

            return this;
        }

#if !NET40
        /// <summary>
        /// Sets batch size
        /// </summary>
        /// <param name="enableStreaming"></param>
        /// <returns></returns>
        public BulkInsertOptions EnableStreaming(bool enableStreaming)
        {
            EnableStreamingValue = enableStreaming;
            return this;
        }
#endif

    }
}

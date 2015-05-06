using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Data.SqlClient;
using EntityFramework.BulkInsert.Extensions;

namespace EntityFramework.BulkInsert.Providers
{
    public abstract class ProviderBase<TConnection, TTransaction> : IEfBulkInsertProvider 
        where TConnection : IDbConnection
        where TTransaction : IDbTransaction
    {
        protected DbContext Context;
        protected abstract string ConnectionString { get; }


        public void Run<T>(IEnumerable<T> entities, IDbTransaction transaction, SqlBulkCopyOptions options, int batchSize)
        {
            Run(entities, (TTransaction) transaction, options, batchSize);
        }

        public IEfBulkInsertProvider SetContext(DbContext context)
        {
            Context = context;

            //var cs = ConfigurationManager.ConnectionStrings[context.GetType().Name];
            //ConnectionString = cs.ConnectionString;

            return this;
        }

        //public abstract void Run<T>(IEnumerable<T> entities, BulkInsertOptions options);

        public IDbConnection GetConnection()
        {
            return CreateConnection();
        }

        public void Run<T>(IEnumerable<T> entities, SqlBulkCopyOptions options, int batchSize)
        {
            using (var dbConnection = GetConnection())
            {
                dbConnection.Open();

                using (var transaction = dbConnection.BeginTransaction())
                {
                    try
                    {
                        Run(entities, transaction, options, batchSize);
                        transaction.Commit();
                    }
                    catch (Exception)
                    {
                        if (transaction.Connection != null)
                        {
                            transaction.Rollback();
                        }
                        throw;
                    }
                }
            }
        }

        //public abstract string ProviderName { get; }
        public abstract void Run<T>(IEnumerable<T> entities, TTransaction transaction, SqlBulkCopyOptions options, int batchSize);
        protected abstract TConnection CreateConnection();
    }
}
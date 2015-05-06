using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace EntityFramework.BulkInsert.Providers
{
    /*
    public class EfSqlCeBulkiInsertProvider : ProviderBase<SqlCeConnection, SqlCeTransaction>
    {
        public override string ProviderName { get { return "System.Data.SqlServerCe.4.0"; } }

        public override void Run<T>(IEnumerable<T> entities, SqlCeTransaction transaction, SqlBulkCopyOptions options, int batchSize)
        {
            using (var cmd = new SqlCeCommand("", (SqlCeConnection) transaction.Connection, transaction))
            {
                
            }
        }

        protected override SqlCeConnection CreateConnection()
        {
            return new SqlCeConnection(ConnectionString);
        }
    }*/
}

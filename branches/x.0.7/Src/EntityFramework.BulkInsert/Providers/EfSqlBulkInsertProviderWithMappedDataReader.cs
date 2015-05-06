using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using EntityFramework.BulkInsert.Extensions;
using EntityFramework.BulkInsert.Helpers;
using EntityFramework.MappingAPI;
using EntityFramework.MappingAPI.Extensions;

namespace EntityFramework.BulkInsert.Providers
{
    public class EfSqlBulkInsertProviderWithMappedDataReader : ProviderBase<SqlConnection, SqlTransaction>
    {
        protected override string ConnectionString
        {
            get
            {
                return (string)Context.Database.Connection.GetPrivateFieldValue("_connectionString");
            }
        }
        /*
        public override void Run<T>(IEnumerable<T> entities, BulkInsertOptions options)
        {
            throw new System.NotImplementedException();
        }
        */
        public override void Run<T>(IEnumerable<T> entities, SqlTransaction transaction, SqlBulkCopyOptions options, int batchSize)
        {
            var baseType = typeof(T);
            var allTypes = baseType.GetDerivedTypes(true);

            var neededMappings = allTypes.ToDictionary(x => x, x => Context.Db(x));

            var keepIdentity = (SqlBulkCopyOptions.KeepIdentity & options) > 0;
            using (var reader = new MappedDataReader<T>(entities, neededMappings, keepIdentity))
            {
                using (var sqlBulkCopy = new SqlBulkCopy(transaction.Connection, options, transaction))
                {
                    sqlBulkCopy.BatchSize = batchSize;
                    sqlBulkCopy.DestinationTableName = string.Format("[{0}].[{1}]", reader.SchemaName, reader.TableName);
                    //sqlBulkCopy.DestinationTableName = reader.TableName;
#if !NET40
                    //sqlBulkCopy.EnableStreaming = true;
#endif

                    foreach (var kvp in reader.Mappings)
                    {
                        sqlBulkCopy.ColumnMappings.Add(kvp.Key, kvp.Value);
                    }

                    sqlBulkCopy.WriteToServer(reader);
                }
            }
        }

        protected override SqlConnection CreateConnection()
        {
            return new SqlConnection(ConnectionString);
        }
    }
}
// Usage:
// 1) Reference Devart.Data.dll and Devart.Data.Oracle.dll
// 2) Register in EF-context constructor:
//    this.RegisterDevartBulkInsertProvider();
// 3) Ensure that your entities have [Column("...")] -attributes to map the entity properties to database columns.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Devart.Data.Oracle;
using EntityFramework.BulkInsert.Helpers;
using System.ComponentModel.DataAnnotations.Schema;

namespace EntityFramework.BulkInsert.Providers
{
    /// <summary>
    /// Devart provider for EntityFramework Bulk Insert, Oracle.
    /// </summary>
    public class DevartProvider : ProviderBase<OracleConnection, OracleTransaction>
    {
        public DevartProvider() : base() { }

        //public override object GetSqlGeography(string wkt, int srid) { throw new NotImplementedException(); }

        protected override OracleConnection CreateConnection()
        {
            return (OracleConnection)base.DbConnection ?? new OracleConnection(base.ConnectionString);
        }

        //public override void Run<T>(IEnumerable<T> entities, BulkInsertOptions options)
        //{
        //    Run(entities, null, options);
        //}

        public override void Run<T>(IEnumerable<T> entities)
        {
            Run(entities, null);
        }

        public override void Run<T>(IEnumerable<T> entities, OracleTransaction transaction /*, BulkInsertOptions options*/)
        {
            if (!entities.Any()) return;
            var conn = transaction != null ? transaction.Connection : (OracleConnection)base.DbConnection;
            var openedConnection = false;

            if (conn.State == System.Data.ConnectionState.Closed)
            {
                conn.Open();
                openedConnection = true;
            }

            var list = entities.ToArray();
            var props = list[0].GetType().GetProperties();
            var dbnames = props.Select(p => Tuple.Create(
                p, //Corresponding table [Column(name)] from POCO-object
                (ColumnAttribute) p.GetCustomAttributes(typeof(ColumnAttribute), true).FirstOrDefault()
                )).Where(a => a.Item2 != null).ToArray();

            var oracleParams2 = HandleParameters<T>(list, dbnames);

            try
            {
                string tablename;
                var table = typeof(T).GetCustomAttributes(typeof(TableAttribute), true).FirstOrDefault() as TableAttribute;
                if (table != null)
                {
                    tablename = table.Name;
                }
                else
                {
                    using (var reader = new MappedDataReader<T>(list, this))
                    {
                        tablename = reader.TableName;
                    }
                }

                var sql = "INSERT INTO " + tablename + " ('" + String.Join("','", dbnames.Select(c => c.Item2.Name)) + "')" +
                          " VALUES (" + String.Join(",", dbnames.Select((c, i) => ":COL" + (i + 1))) + ")";

                //if(openedConnection)conn.AutoCommit = true;
                var command = conn.CreateCommand(sql.Replace('\'', '"'), System.Data.CommandType.Text);
                command.Parameters.AddRange(oracleParams2.ToArray());
                command.ExecuteArray(list.Length);
            }
            finally
            {
                if (openedConnection)
                {
                    conn.Close();
                }
            }
        }

        private static IEnumerable<OracleParameter> HandleParameters<T>(T[] list, Tuple<System.Reflection.PropertyInfo, ColumnAttribute>[] dbnames)
        {
            int idx2 = 1;
            foreach (var col in dbnames)
            {
                OracleParameter orpar;

                if (col.Item1.PropertyType.Name == typeof(Guid).Name)
                {
                    var guidValues = list.Select(l => new Guid((l.GetType().GetProperty(col.Item1.Name).GetValue(l)).ToString())).ToArray();

                    orpar = new OracleParameter
                            (
                                ":COL" + idx2.ToString(),
                                OracleDbType.Raw,
                                guidValues,
                                System.Data.ParameterDirection.Input
                            );

                    orpar.ArrayLength = guidValues.Length;
                }
                else
                {
                    var values = list.Select(l => l.GetType().GetProperty(col.Item1.Name).GetValue(l)).ToArray();

                    orpar = new OracleParameter
                            (
                                ":COL" + idx2.ToString(),
                                col.Item1.PropertyType.Name == typeof(String).Name ? OracleDbType.NVarChar :
                                col.Item1.PropertyType.Name == typeof(DateTime).Name ? OracleDbType.Date :
                                OracleDbType.Number,
                                values,
                                System.Data.ParameterDirection.Input
                            );

                    orpar.ArrayLength = values.Length;
                }

                yield return orpar;
                idx2++;
            }
        }
    }
}
namespace System.Data.Entity
{
    public static class EntityFrameworkContextExtensions
    {
        public static void RegisterDevartBulkInsertProvider(this DbContext context)
        {
            if (System.Configuration.ConfigurationManager.AppSettings.Get("InnerDbProviderName") != "Devart.Data.Oracle") return;
            try
            {
                EntityFramework.BulkInsert.ProviderFactory.Get(context);
            }
            catch (EntityFramework.BulkInsert.Exceptions.BulkInsertProviderNotFoundException)
            {
                EntityFramework.BulkInsert.ProviderFactory.Register<EntityFramework.BulkInsert.Providers.DevartProvider>("Devart.Data.Oracle.OracleConnection");
            }
            catch (AggregateException ae)
            {
                if (ae.InnerException is EntityFramework.BulkInsert.Exceptions.BulkInsertProviderNotFoundException)
                    EntityFramework.BulkInsert.ProviderFactory.Register<EntityFramework.BulkInsert.Providers.DevartProvider>("Devart.Data.Oracle.OracleConnection");
                else
                    throw;
            }
        }
    }
}
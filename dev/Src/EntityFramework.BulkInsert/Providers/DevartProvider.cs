// Usage:
// 1) Reference Devart.Data.dll and Devart.Data.Oracle.dll
// 2) Register in EF-context constructor:
//    this.RegisterDevartBulkInsertProvider();
// 3) Ensure that your entities have [Column("...")] -attributes to map the entity properties to database columns.
// 
// Note that you have to have your entries in container.Entry(item).State = EntityState.Unchanged
// Call your context await .SaveChangesAsync() before and after the BulkInsert()-command to keep the EF-context in sync.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Devart.Data.Oracle;
using EntityFramework.BulkInsert.Helpers;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Entity.Core.Mapping;
using System.Data.Entity.Core.Metadata.Edm;

namespace EntityFramework.BulkInsert.Providers
{
    /// <summary>
    /// Devart provider for EntityFramework Bulk Insert, Oracle.
    /// </summary>
    public class DevartProvider : ProviderBase<OracleConnection, OracleTransaction>
    {
        public DevartProvider() : base() { }

        //public override object GetSqlGeography(string wkt, int srid) { throw new NotImplementedException(); }
        //public override object GetSqlGeometry(string wkt, int srid) { throw new NotImplementedException(); }

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
            var dbnames = props.Where(p => !p.GetGetMethod().IsVirtual).Select(p => Tuple.Create(
                p, //Corresponding table [Column(name)] from POCO-object
                (ColumnAttribute) p.GetCustomAttributes(typeof(ColumnAttribute), true).FirstOrDefault()
                )).Where(a => a.Item2 != null).ToArray();

            string tablenameTmp = "";
            IEnumerable<OracleParameter> oracleParams2;
            IEnumerable<string> colnames;
            IEnumerable<string> colValues;
            if (dbnames.Length > 0)
            {
                oracleParams2 = HandleParameters<T>(list, dbnames);
                colnames = dbnames.Select(c => c.Item2.Name);
                colValues = dbnames.Select((c, i) => ":COL" + (i + 1));
            }
            else
            {
                var objContext = ((System.Data.Entity.Infrastructure.IObjectContextAdapter)base.Context).ObjectContext;
                var workspace = objContext.MetadataWorkspace;

                var entityMapping = workspace.GetItems<EntityType>(System.Data.Entity.Core.Metadata.Edm.DataSpace.CSpace);
                var entityProperties = (
                    from item in entityMapping
                    from property in item.Properties
                    where item.Name == typeof (T).Name
                    select new
                    {
                        PropertyName = property.Name,
                        PropertyType = property.TypeName
                    }).Select((item, index) => new { Item = item, Index = index});

                var dbMapping = workspace.GetItems<EntityContainerMapping>(System.Data.Entity.Core.Metadata.Edm.DataSpace.CSSpace);
                var dbColumns= (
                    from item in dbMapping
                    from entitySet in item.StoreEntityContainer.EntitySets
                    from column in entitySet.ElementType.DeclaredMembers
                    where entitySet.Name == typeof(T).Name
                    select new
                    {
                        TableName = entitySet.Table,
                        ColumnName = column.Name
                    }).Select((item, index) => new { Item = item, Index = index });

                var objects = (from property in entityProperties
                    join column in dbColumns on property.Index equals column.Index
                    select new
                    {
                        Property = property.Item.PropertyName,
                        Type = property.Item.PropertyType,
                        Table = column.Item.TableName,
                        Column = column.Item.ColumnName
                    }).ToArray();

                colnames = objects.Select(c => c.Column);
                colValues = objects.Select((c, i) => ":COL" + (i + 1));
                tablenameTmp = objects[0].Table;
                oracleParams2 = HandleParametersTn(list, objects.Select(i => Tuple.Create(i.Property, i.Type)).ToArray());
            }

            try
            {
                var table =
                    typeof (T).GetCustomAttributes(typeof (TableAttribute), true).FirstOrDefault() as TableAttribute;

                var tablename = table != null ? table.Name : tablenameTmp;

                var sql = "INSERT INTO " + tablename + " ('" + String.Join("','", colnames) +
                          "') VALUES (" + String.Join(",", colValues) + ")";


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
            return HandleParametersTn(list, dbnames.Select(i => Tuple.Create(i.Item1.Name, i.Item1.PropertyType.Name)).ToArray());
        }

        private static IEnumerable<OracleParameter> HandleParametersTn<T>(T[] list, Tuple<string, string>[] nameAndType)
        {
            int idx2 = 1;
            foreach (var col in nameAndType)
            {
                OracleParameter orpar;

                if (col.Item2 == typeof(Guid).Name)
                {
                    var guidValues = list.Select(l => {
                        var x = new Nullable<Guid>();

                        if (l.GetType().GetProperty(col.Item1).GetValue(l) == null)
                        {
                            x = null;
                        }
                        else
                        {
                            x = new Guid((l.GetType().GetProperty(col.Item1).GetValue(l)).ToString());
                        }
                        return x;
                        }).ToArray();

                    orpar = new OracleParameter
                            (
                                ":COL" + idx2,
                                OracleDbType.Raw,
                                guidValues,
                                System.Data.ParameterDirection.Input
                            );

                    orpar.ArrayLength = guidValues.Length;
                }
                else
                {
                    var values = list.Select(l => l.GetType().GetProperty(col.Item1).GetValue(l)).ToArray();

                    orpar = new OracleParameter
                            (
                                ":COL" + idx2.ToString(),
                                col.Item2 == typeof(String).Name ? OracleDbType.NVarChar :
                                col.Item2 == typeof(DateTime).Name ? OracleDbType.Date :
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
        /// <summary>
        /// Call this in your EF-context constructor!
        /// </summary>
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
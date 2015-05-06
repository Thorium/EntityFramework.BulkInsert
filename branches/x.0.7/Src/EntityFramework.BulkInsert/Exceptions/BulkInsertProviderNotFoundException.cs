using System;

namespace EntityFramework.BulkInsert.Exceptions
{
    public class BulkInsertProviderNotFoundException : Exception
    {
        private readonly string _connectionType;
        public BulkInsertProviderNotFoundException(string connectionType)
        {
            _connectionType = connectionType;
        }

        public override string Message
        {
            get
            {
                return
                    string.Format(
                        "BulkInsertProvider not found for '{0}.\nTo register new provider user EntityFramework.BulkInsert.Providers.ProviderFactory.Register() method'",
                        _connectionType);
            }
        }
    }
}

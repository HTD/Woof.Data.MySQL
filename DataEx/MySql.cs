using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace Woof.DataEx {

    /// <summary>
    /// Minimalistic MySQL backend.
    /// </summary>
    public class MySql : IDbSource {

        #region Configuration

        /// <summary>
        /// Connection string for MySQL database access.
        /// </summary>
        private readonly string ConnectionString;

        public MySql(string connectionString) => ConnectionString = connectionString;

        /// <summary>
        /// Creates new MySQL input parameter.
        /// </summary>
        /// <param name="name">Parameter name.</param>
        /// <param name="value">Parameter value.</param>
        /// <returns><see cref="MySqlParameter"/>.</returns>
        public DbParameter I(string name, object value) => new MySqlParameter(name, value);

        /// <summary>
        /// Creates new MySQL input / output parameter.
        /// </summary>
        /// <param name="name">Parameter name.</param>
        /// <param name="value">Parameter value.</param>
        /// <returns><see cref="MySqlParameter"/>.</returns>
        public DbParameter IO(string name, object value) => new MySqlParameter(name, value) { Direction = ParameterDirection.InputOutput };

        /// <summary>
        /// Creates new MySQL output parameter.
        /// </summary>
        /// <param name="name">Parameter name.</param>
        /// <returns><see cref="MySqlParameter"/>.</returns>
        public DbParameter O(string name) => new MySqlParameter(name, null) { Direction = ParameterDirection.Output };

        #endregion

        #region Command builder

        /// <summary>
        /// Creates new <see cref="MySqlCommand"/> from stored procedure name and optional parameters and opens a connection if needed.
        /// </summary>
        /// <param name="connection">Disposable connection.</param>
        /// <param name="procedure">Stored procedure name.</param>
        /// <param name="parameters">Optional MySQL parameters.</param>
        /// <returns>Command.</returns>
        private async Task<MySqlCommand> GetCommandAsync(MySqlConnection connection, string procedure, params DbParameter[] parameters) {
            var command = new MySqlCommand(procedure, connection) { CommandType = CommandType.StoredProcedure };
            if (parameters.Length > 0) foreach (var parameter in parameters) command.Parameters.Add(parameter);
            if (connection.State != ConnectionState.Open) await connection.OpenAsync();
            return command;
        }

        #endregion

        #region Data operations

        /// <summary>
        /// Executes a stored procedure. Returns number of rows affected.
        /// </summary>
        /// <param name="procedure">Stored procedure name.</param>
        /// <param name="parameters">Optional MySQL parameters.</param>
        /// <returns>The number of rows affected.</returns>
        public async Task<int> ExecuteAsync(string procedure, params DbParameter[] parameters) {
            using (var connection = new MySqlConnection(ConnectionString))
            using (var command = await GetCommandAsync(connection, procedure, parameters)) return await command.ExecuteNonQueryAsync();
        }

        /// <summary>
        /// Executes a stored procedure and returns a scalar.
        /// </summary>
        /// <param name="procedure">Stored procedure name.</param>
        /// <param name="parameters">Optional MySQL parameters.</param>
        /// <returns>Scalar object.</returns>
        public async Task<T> GetScalarAsync<T>(string procedure, params DbParameter[] parameters) {
            object value = null;
            using (var connection = new MySqlConnection(ConnectionString))
            using (var command = await GetCommandAsync(connection, procedure, parameters)) value = await command.ExecuteScalarAsync();
            if (value == null || value is DBNull) return default(T);
            else return (T)value;
        }

        /// <summary>
        /// Executes a stored procedure and returns a table.
        /// </summary>
        /// <param name="procedure">Stored procedure name.</param>
        /// <param name="parameters">Optional MySQL parameters.</param>
        /// <returns>Table data.</returns>
        public async Task<object[][]> GetTableAsync(string procedure, params DbParameter[] parameters) {
            var table = new List<object[]>();
            using (var connection = new MySqlConnection(ConnectionString))
            using (var command = await GetCommandAsync(connection, procedure, parameters)) {
                using (var reader = await command.ExecuteReaderAsync()) {
                    while (await reader.ReadAsync()) {
                        object[] values = new object[reader.FieldCount];
                        reader.GetValues(values);
                        table.Add(values);
                    }
                }
            }
            return table.ToArray();
        }

        /// <summary>
        /// Executes a stored procedure and returns a table.
        /// </summary>
        /// <typeparam name="T">Record type.</typeparam>
        /// <param name="procedure">Stored procedure name.</param>
        /// <param name="parameters">Optional MySQL parameters.</param>
        /// <returns>Table data.</returns>
        public async Task<T[]> GetTableAsync<T>(string procedure, params DbParameter[] parameters) where T : new()
            => (await GetTableAsync(procedure, parameters)).AsArrayOf<T>();

        /// <summary>
        /// Executes a stored procedure and returns a record.
        /// </summary>
        /// <typeparam name="T">Record type.</typeparam>
        /// <param name="procedure">Stored procedure name.</param>
        /// <param name="parameters">Optional MySQL parameters.</param>
        /// <returns>Table data.</returns>
        public async Task<T> GetRecordAsync<T>(string procedure, params DbParameter[] parameters) where T : new() {
            var raw = await GetTableAsync(procedure, parameters);
            var row = raw?.FirstOrDefault();
            return row.As<T>();
        }

        /// <summary>
        /// Executes a stored procedure and returns multiple tables.
        /// </summary>
        /// <param name="procedure">Stored procedure name.</param>
        /// <param name="parameters">Optional MySQL parameters.</param>
        /// <returns>An array of table data.</returns>
        public async Task<object[][][]> GetDataAsync(string procedure, params DbParameter[] parameters) {
            var data = new List<object[][]>();
            var table = new List<object[]>();
            using (var connection = new MySqlConnection(ConnectionString))
            using (var command = await GetCommandAsync(connection, procedure, parameters)) {
                using (var reader = await command.ExecuteReaderAsync()) {
                    do {
                        while (await reader.ReadAsync()) {
                            object[] values = new object[reader.FieldCount];
                            reader.GetValues(values);
                            table.Add(values);
                        }
                        data.Add(table.ToArray());
                        table.Clear();
                    } while (await reader.NextResultAsync());
                }
            }
            return data.ToArray();
        }        

        #endregion

    }

}
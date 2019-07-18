using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using EntityFrameworkCore.PostgreSQL.Copy.Statements;
using Microsoft.EntityFrameworkCore;

namespace EntityFrameworkCore.PostgreSQL.Copy
{
    public sealed class MergeOperation<TEntity>
    {
        private readonly DbContext _context;
        private Func<IEnumerable<TEntity>> _getDataFunc;
        private Expression<Func<TEntity, object>> _conflictExpression;
        private Expression<Func<TEntity, TEntity, TEntity>> _updateExpression;

        public MergeOperation(DbContext context)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }

        public void WithData(Func<IEnumerable<TEntity>> getDataFunc)
        {
            _getDataFunc = getDataFunc;
        }

        public void OnConflictDoUpdate(Expression<Func<TEntity, object>> conflictExpression, Expression<Func<TEntity, TEntity, TEntity>> updateExpression)
        {
            _conflictExpression = conflictExpression;
            _updateExpression = updateExpression;
        }

        public async Task ExecuteAsync()
        {
//            _context.Database.OpenConnection();

            var tempTableName = Guid.NewGuid().ToString("N");

            var temporaryTableStatementBuilder = new CreateTemporaryTableStatementBuilder(_context);
            var temporaryTableStatement = temporaryTableStatementBuilder.GetStatement(typeof(TEntity), tempTableName);

            Console.WriteLine(temporaryTableStatement);
            Console.WriteLine("----------------------------------");

            await _context.Database.ExecuteSqlCommandAsync(temporaryTableStatement);

            var copyStatementBuilder = new CopyStatementBuilder(_context);
            var copyStatement = copyStatementBuilder.GetStatement(typeof(TEntity), tempTableName);

            Console.WriteLine(copyStatement);
            Console.WriteLine("----------------------------------");


            var entityType = _context.Model.FindEntityType(typeof(TEntity));
            var dbConnection = (Npgsql.NpgsqlConnection) _context.Database.GetDbConnection();

            using (var writer = dbConnection.BeginBinaryImport(copyStatement))
            {
                foreach (var entity in _getDataFunc())
                {
                    writer.StartRow();

                    // TODO build & compile expression 
                    foreach (var property in entityType.GetProperties())
                    {
                        var propertyAnnotations = property.Relational();

                        writer.Write(property.PropertyInfo.GetValue(entity), propertyAnnotations.ColumnType); // NpgsqlDbType.Integer
                    }
                }

                writer.Complete();
            }

            var insertIntoStatementBuilder = new InsertIntoStatementBuilder(_context);
            var insertStatement = insertIntoStatementBuilder.GetStatement(typeof(TEntity), tempTableName, _conflictExpression, _updateExpression);

            Console.WriteLine(insertStatement);
            Console.WriteLine("----------------------------------");

            await _context.Database.ExecuteSqlCommandAsync(insertStatement, insertIntoStatementBuilder.GetDbParameters());

//                    RawSqlCommand rawSqlCommand = databaseFacade.GetRelationalService<IRawSqlCommandBuilder>().Build(sql.Format, parameters);
//                    return rawSqlCommand.RelationalCommand.ExecuteNonQuery(databaseFacade.GetRelationalService<IRelationalConnection>(), rawSqlCommand.ParameterValues);
        }
    }
}
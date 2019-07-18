using System;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.PostgreSQL.Copy.Statements
{
    public sealed class CreateTemporaryTableStatementBuilder
    {
        private readonly DbContext _context;

        public CreateTemporaryTableStatementBuilder(DbContext context)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }

        public string GetStatement(Type type, string tempTableName)
        {
            var entityType = _context.Model.FindEntityType(type);


            //            var relationalCommandBuilder = context.GetService<IRelationalCommandBuilder>();
            var relationalCommandBuilder = _context.GetService<ISqlGenerationHelper>();

            var statementBuilder = new StringBuilder();

            statementBuilder.Append("CREATE TEMPORARY TABLE ");
            relationalCommandBuilder.DelimitIdentifier(statementBuilder, tempTableName);
            statementBuilder.Append(" (");
            statementBuilder.AppendLine();

            var firstProperty = true;
            foreach (var property in entityType.GetProperties())
            {
                if (!firstProperty)
                {
                    statementBuilder.Append(",");
                    statementBuilder.AppendLine();
                }

                var propertyAnnotations = property.Relational();

                statementBuilder.Append("    ");
                relationalCommandBuilder.DelimitIdentifier(statementBuilder, propertyAnnotations.ColumnName);
                statementBuilder.Append(" ");
                statementBuilder.Append(propertyAnnotations.ColumnType);
                statementBuilder.Append(" ");

                statementBuilder.Append(property.IsColumnNullable() ? "NULL" : "NOT NULL");
                firstProperty = false;
            }

            statementBuilder.AppendLine();
            statementBuilder.Append(")");
            statementBuilder.Append(" ON COMMIT DROP");
            statementBuilder.Append(relationalCommandBuilder.StatementTerminator);

            return statementBuilder.ToString();
//            entityType.GetProperties().First().Npgsql()
        }
    }
}
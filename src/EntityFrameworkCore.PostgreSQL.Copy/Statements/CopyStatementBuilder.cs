using System;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.PostgreSQL.Copy.Statements
{
    public sealed class CopyStatementBuilder
    {
        private readonly DbContext _context;

        public CopyStatementBuilder(DbContext context)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }

        public string GetStatement(Type type, string tempTableName)
        {
            var entityType = _context.Model.FindEntityType(type);
            var relationalCommandBuilder = _context.GetService<ISqlGenerationHelper>();

            var statementBuilder = new StringBuilder();

            statementBuilder.Append("COPY ");
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

                firstProperty = false;
            }

            statementBuilder.AppendLine();
            statementBuilder.Append(")");
            statementBuilder.Append(" FROM STDIN (FORMAT BINARY)");
            statementBuilder.Append(relationalCommandBuilder.StatementTerminator);

            return statementBuilder.ToString();
        }
    }
}
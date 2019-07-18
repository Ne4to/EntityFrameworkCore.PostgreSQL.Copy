using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq.Expressions;
using EntityFrameworkCore.PostgreSQL.Copy.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.PostgreSQL.Copy.Statements
{
    public sealed class InsertIntoStatementBuilder
    {
        private readonly DbContext _context;
        private SqlStatementBuilder _statementBuilder;

        public InsertIntoStatementBuilder(DbContext context)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }

        public IEnumerable<DbParameter> GetDbParameters() => _statementBuilder.GetDbParameters();

        public string GetStatement(Type type, string tempTableName, Expression conflictIndexColumnExpression, Expression conflictUpdateSetter)
        {
            var entityType = _context.Model.FindEntityType(type);
            var relationalCommandBuilder = _context.GetService<ISqlGenerationHelper>();

            _statementBuilder = new SqlStatementBuilder(relationalCommandBuilder);

            //
            _statementBuilder.Append("INSERT INTO ");
            _statementBuilder.DelimitIdentifier(entityType.Relational().TableName);
            _statementBuilder.AppendLine();
            _statementBuilder.Append("(");
            _statementBuilder.AppendLine();

            var firstProperty = true;
            foreach (var property in entityType.GetProperties())
            {
                if (!firstProperty)
                {
                    _statementBuilder.Append(",");
                    _statementBuilder.AppendLine();
                }

                var propertyAnnotations = property.Relational();

                _statementBuilder.Append("    ");
                _statementBuilder.DelimitIdentifier(propertyAnnotations.ColumnName);

                firstProperty = false;
            }

            _statementBuilder.AppendLine();
            _statementBuilder.Append(")");
            _statementBuilder.AppendLine();

            _statementBuilder.Append("SELECT ");
            _statementBuilder.AppendLine();

            firstProperty = true;
            foreach (var property in entityType.GetProperties())
            {
                if (!firstProperty)
                {
                    _statementBuilder.Append(",");
                    _statementBuilder.AppendLine();
                }

                var propertyAnnotations = property.Relational();

                _statementBuilder.Append("    ");
                _statementBuilder.DelimitIdentifier(propertyAnnotations.ColumnName);

                firstProperty = false;
            }

            _statementBuilder.AppendLine();

            _statementBuilder.Append("FROM ");
            _statementBuilder.DelimitIdentifier(tempTableName);

            // TODO make parameters
            if (conflictUpdateSetter != null)
            {
                _statementBuilder.AppendLine();
                _statementBuilder.Append("ON CONFLICT ");

                _statementBuilder.Append("(");
                var conflictIndexColumnVisitor = new ConflictIndexColumnVisitor(conflictIndexColumnExpression, entityType, _statementBuilder);
                conflictIndexColumnVisitor.AppendStatement();
                _statementBuilder.AppendLine(")");

                _statementBuilder.AppendLine("DO UPDATE SET ");

                var conflictUpdateExpressionVisitor = new ConflictUpdateExpressionVisitor(conflictUpdateSetter, entityType, _statementBuilder);
                conflictUpdateExpressionVisitor.AppendStatement();
            }

            _statementBuilder.Append(relationalCommandBuilder.StatementTerminator);
            return _statementBuilder.Build();
        }
    }
}
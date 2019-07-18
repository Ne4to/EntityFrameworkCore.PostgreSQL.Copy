using System;
using System.Linq.Expressions;
using EntityFrameworkCore.PostgreSQL.Copy.Statements;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EntityFrameworkCore.PostgreSQL.Copy.Expressions
{
    public sealed class ConflictIndexColumnVisitor : ExpressionVisitor
    {
        private readonly Expression _conflictIndexColumnExpression;
        private readonly IEntityType _entityType;
        private readonly SqlStatementBuilder _statementBuilder;

        public ConflictIndexColumnVisitor(Expression conflictIndexColumnExpression, IEntityType entityType, SqlStatementBuilder statementBuilder)
        {
            _conflictIndexColumnExpression = conflictIndexColumnExpression;
            _entityType = entityType;
            _statementBuilder = statementBuilder;
        }

        public void AppendStatement()
        {
            if (_conflictIndexColumnExpression.NodeType != ExpressionType.Lambda)
            {
                throw new NotSupportedException();
            }

            var lambdaExpression = (LambdaExpression) _conflictIndexColumnExpression;
            Visit(lambdaExpression.Body);
        }

        protected override Expression VisitNew(NewExpression node)
        {
            for (var argumentIndex = 0; argumentIndex < node.Arguments.Count; argumentIndex++)
            {
                var nodeArgument = node.Arguments[argumentIndex];

                if (argumentIndex != 0)
                {
                    _statementBuilder.Append(",");
                }

                Visit(nodeArgument);
            }

            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            var lambdaExpression = (LambdaExpression) _conflictIndexColumnExpression;
            var parameterExpression = (ParameterExpression) node.Expression;

            if (lambdaExpression.Parameters[0].Equals(parameterExpression))
            {
                _statementBuilder.DelimitIdentifier(_entityType.FindProperty(node.Member.Name).Relational().ColumnName);
                return node;
            }

            throw new NotSupportedException();
        }
    }
}
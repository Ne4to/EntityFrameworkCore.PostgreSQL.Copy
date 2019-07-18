using System;
using System.Linq.Expressions;
using EntityFrameworkCore.PostgreSQL.Copy.Statements;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EntityFrameworkCore.PostgreSQL.Copy.Expressions
{
    public sealed class ConflictUpdateExpressionVisitor : ExpressionVisitor
    {
        private readonly Expression _conflictUpdateSetter;
        private readonly IEntityType _entityType;
        private readonly SqlStatementBuilder _statementBuilder;

        private string _currentAssignmentColumnType;

        public ConflictUpdateExpressionVisitor(Expression conflictUpdateSetter, IEntityType entityType, SqlStatementBuilder statementBuilder)

        {
            _conflictUpdateSetter = conflictUpdateSetter ?? throw new ArgumentNullException(nameof(conflictUpdateSetter));
            _entityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
            _statementBuilder = statementBuilder ?? throw new ArgumentNullException(nameof(statementBuilder));
        }

        public void AppendStatement()
        {
            if (_conflictUpdateSetter.NodeType != ExpressionType.Lambda)
            {
                throw new NotSupportedException();
            }

            var lambdaExpression = (LambdaExpression) _conflictUpdateSetter;
            var initExpression = (MemberInitExpression) lambdaExpression.Body;

            VisitMemberInit(initExpression);
        }

        protected override Expression VisitMemberInit(MemberInitExpression node)
        {
            for (var bindingIndex = 0; bindingIndex < node.Bindings.Count; bindingIndex++)
            {
                var initExpressionBinding = node.Bindings[bindingIndex];
                if (initExpressionBinding.BindingType != MemberBindingType.Assignment)
                {
                    throw new NotSupportedException();
                }

                var memberAssignment = (MemberAssignment) initExpressionBinding;

                if (bindingIndex != 0)
                {
                    _statementBuilder.Append(",");
                    _statementBuilder.AppendLine();
                }

                var relationalPropertyAnnotations = _entityType.FindProperty(memberAssignment.Member.Name).Relational();

                var columnName = relationalPropertyAnnotations.ColumnName;
                _currentAssignmentColumnType = relationalPropertyAnnotations.ColumnType;

                _statementBuilder.Append("    ");
                _statementBuilder.DelimitIdentifier(columnName);
                _statementBuilder.Append(" = ");

                Visit(memberAssignment.Expression);
            }

            return node;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            // TODO add other supported types
            if (node.Type == typeof(int) || node.Type == typeof(string))
            {
//                var typeName = $"{_sqlGenerationHelper.DelimitIdentifier(_entityType.Relational().TableName)}.{_sqlGenerationHelper.DelimitIdentifier(_currentAssignmentColumnName)}%TYPE";
                var typeName = _currentAssignmentColumnType;
                var parameterName = _statementBuilder.AddParameter(node.Value, typeName);

//                _statementBuilder.Append(parameterName);
                _statementBuilder.Append($"@{parameterName}");
                //_statementBuilder.DelimitIdentifier(parameterName);
                return node;
            }

            throw new NotSupportedException();
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            var lambdaExpression = (LambdaExpression) _conflictUpdateSetter;
            var parameterExpression = (ParameterExpression) node.Expression;

            var existingRowParameter = lambdaExpression.Parameters[0];
            var excludedRowParameter = lambdaExpression.Parameters[1];

            if (excludedRowParameter.Equals(parameterExpression))
            {
                _statementBuilder.Append("EXCLUDED.");
                _statementBuilder.DelimitIdentifier(_entityType.FindProperty(node.Member.Name).Relational().ColumnName);
                return node;
            }

            if (existingRowParameter.Equals(parameterExpression))
            {
                _statementBuilder.DelimitIdentifier(_entityType.FindProperty(node.Member.Name).Relational().ColumnName);
                return node;
            }

            throw new NotSupportedException();
        }
    }
}
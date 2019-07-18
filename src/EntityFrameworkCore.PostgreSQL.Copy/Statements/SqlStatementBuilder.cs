using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;

namespace EntityFrameworkCore.PostgreSQL.Copy.Statements
{
    public sealed class SqlStatementBuilder
    {
        private readonly ISqlGenerationHelper _helper;
        private readonly StringBuilder _bodyBuilder = new StringBuilder();
        private readonly List<Parameter> _parameters = new List<Parameter>();

        public SqlStatementBuilder(ISqlGenerationHelper helper)
        {
            _helper = helper;
        }

        public void Append(string value)
        {
            _bodyBuilder.Append(value);
        }

        public void AppendLine(string value)
        {
            _bodyBuilder.AppendLine(value);
        }

        public void AppendLine()
        {
            _bodyBuilder.AppendLine();
        }

        public void DelimitIdentifier(string identifier)
        {
            _helper.DelimitIdentifier(_bodyBuilder, identifier);
        }

        public string Build()
        {
            return _bodyBuilder.ToString();

            //if (_parameters.Count == 0)
            //{
            //    return _bodyBuilder.ToString();
            //}

            //var declarationBuilder = new StringBuilder();

            //declarationBuilder.AppendLine("DO $$");
            //declarationBuilder.AppendLine("DECLARE");

            //for (var paramIndex = 0; paramIndex < _parameters.Count; paramIndex++)
            //{
            //    var parameter = _parameters[paramIndex];

            //    if (paramIndex != 0)
            //    {
            //        declarationBuilder.Append(",");
            //        declarationBuilder.AppendLine();
            //    }

            //    declarationBuilder.Append("    ");
            //    declarationBuilder.Append($"{parameter.Name} {parameter.TypeName}");
            //}

            //declarationBuilder.AppendLine(_helper.StatementTerminator);

            //declarationBuilder.AppendLine("BEGIN");

            //declarationBuilder.AppendLine(_bodyBuilder.ToString());

            //declarationBuilder.Append("END $$");
            //declarationBuilder.AppendLine(_helper.StatementTerminator);

            //return declarationBuilder.ToString();
        }

        public string AddParameter(object value, string typeName)
        {
            var parameterName = $"p{_parameters.Count}";

            _parameters.Add(new Parameter()
            {
                Name = parameterName,
                TypeName = typeName,
                Value = value
            });

            return parameterName;
        }

        public IEnumerable<DbParameter> GetDbParameters()
        {
            foreach (var parameter in _parameters)
            {
                yield return new NpgsqlParameter
                {
                    ParameterName = parameter.Name,
                    DataTypeName = parameter.TypeName,
                    Value = parameter.Value
                };
            }
        }

        private class Parameter
        {
            public string Name { get; set; }
            public string TypeName { get; set; }
            public object Value { get; set; }
        }
    }
}
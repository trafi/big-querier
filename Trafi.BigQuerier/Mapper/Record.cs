﻿using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Trafi.BigQuerier.Mapper
{
    public static class Record
    {
        public static TableSchema GetSchema(Type type)
        {
            if (!IsContractType(type))
            {
                throw new NotImplementedException($"Type {type.FullName} is not supported by BigQuerier Contract (maybe QuerierContract attribute is missing?)");
            }

            var builder = new TableSchemaBuilder();
            foreach (var property in FilterValidTypeProperties(type))
            {
                Property.AppendRecordFieldSchema(builder, property.Name, property.PropertyType);
            }

            return builder.Build();
        }

        public static Func<object, BigQueryInsertRow> GetValueToBigQueryFunction(Type type)
        {
            if (!IsContractType(type))
            {
                throw new NotImplementedException($"Type {type.FullName} is not supported by BigQuerier Contract (maybe QuerierContract attribute is missing?)");
            }

            var propertyMappers = FilterValidTypeProperties(type)
                .Select(p => Property.GetBigQueryRecordFieldAppendFunction(p))
                .ToArray();

            return value =>
            {
                if (value == null)
                {
                    return null;
                }

                var row = new BigQueryInsertRow();
                foreach (var mapper in propertyMappers)
                {
                    mapper(value, row);
                }
                return row;
            };
        }

        public static Func<BigQueryRow, object> GetValueFromBigQueryFunction(Type type)
        {
            if (!IsContractType(type))
            {
                throw new NotImplementedException($"Type {type.FullName} is not supported by BigQuerier Contract (maybe QuerierContract attribute is missing?)");
            }

            var propertyMappers = FilterValidTypeProperties(type)
                .Select(p => new { Mapper = Property.GetPropertySetFromObjectFunction(p), Name = p.Name })
                .ToArray();

            var constructor = type.GetConstructor(new Type[] { });
            
            return row =>
            {
                var obj = constructor.Invoke(new object[] { });
                foreach (var item in propertyMappers)
                {
                    object rowField;
                    try
                    {
                        rowField = row[item.Name];
                    }
                    catch
                    {
                        continue;
                    }

                    item.Mapper(rowField, obj);
                }
                return obj;
            };
        }

        public static Func<Dictionary<string, object>, object> GetValueFromBigQueryDictionaryFunction(Type type)
        {
            if (!IsContractType(type))
            {
                throw new NotImplementedException($"Type {type.FullName} is not supported by BigQuerier Contract (maybe QuerierContract attribute is missing?)");
            }

            var propertyMappers = FilterValidTypeProperties(type)
                .Select(p => new { Mapper = Property.GetPropertySetFromObjectFunction(p), Name = p.Name })
                .ToArray();
            
            return row =>
            {
                if (row == null)
                {
                    return null;
                }

                var obj = Activator.CreateInstance(type);
                foreach (var item in propertyMappers)
                {
                    object rowField;
                    if (row.TryGetValue(item.Name, out rowField))
                    {
                        item.Mapper(rowField, obj);
                    }
                }
                return obj;
            };
        }

        private static IEnumerable<PropertyInfo> FilterValidTypeProperties(Type type)
        {
            return type.GetProperties()
                    .Where(p => p.MemberType == MemberTypes.Property && p.CanRead && p.CanWrite);
        }

        public static bool IsContractType(Type type)
        {
            return type.GetTypeInfo().GetCustomAttribute<QuerierContract>() != null;
        }
    }
}

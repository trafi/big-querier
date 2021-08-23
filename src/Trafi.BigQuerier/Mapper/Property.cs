// Copyright 2021 TRAFI
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace Trafi.BigQuerier.Mapper
{
    public static class Property
    {
        public static void AppendRecordFieldSchema(TableSchemaBuilder tableBuilder, string name, Type type)
        {
            var simpleFieldOptions = Value.MaybeSimpleFieldOptionsFromType(type);
            if (simpleFieldOptions != null)
            {
                tableBuilder.Add(name, simpleFieldOptions.type, simpleFieldOptions.mode);
                return;
            }

            if (type.IsArray)
            {
                var elementType = type.GetElementType();
                var simpleArrayElementFieldOptions = Value.MaybeSimpleFieldOptionsFromType(elementType);
                if (null != simpleArrayElementFieldOptions)
                {
                    tableBuilder.Add(name, simpleArrayElementFieldOptions.type, BigQueryFieldMode.Repeated);
                    return;
                }

                if (Record.IsContractType(elementType))
                {
                    tableBuilder.Add(name, Record.GetSchema(elementType), BigQueryFieldMode.Repeated);
                    return;
                }
            }

            if (Record.IsContractType(type))
            {
                tableBuilder.Add(name, Record.GetSchema(type), BigQueryFieldMode.Nullable);
                return;
            }

            throw new NotImplementedException($"Type {type.FullName} is not a valid BigQuerier Contract field");
        }

        public static Action<object, BigQueryInsertRow> GetBigQueryRecordFieldAppendFunction(PropertyInfo p)
        {
            var propertyName = p.Name;
            var objectMapFunction = Value.MaybeFieldToBigQueryFunction(p.PropertyType);
            if (null != objectMapFunction)
            {
                return (instance, row) =>
                {
                    row.Add(propertyName, objectMapFunction(p.GetValue(instance)));
                };
            }

            var repeatedObjectMapFunction = Value.MaybeRepeatedFieldToBigQueryFunction(p.PropertyType);
            if (null != repeatedObjectMapFunction)
            {
                return (instance, row) =>
                {
                    row.Add(propertyName, repeatedObjectMapFunction(p.GetValue(instance)));
                };
            }

            var recordMapFunction = Record.GetValueToBigQueryFunction(p.PropertyType);
            return (instance, row) =>
            {
                row.Add(propertyName, recordMapFunction(p.GetValue(instance)));
            };
        }

        public static Action<object, object> GetPropertySetFromObjectFunction(PropertyInfo p)
        {
            var objectMapFunction = Value.MaybeFieldFromBigQueryFunction(p.PropertyType);
            if (null != objectMapFunction)
            {
                return (fromObj, instanceObjext) =>
                {
                    var mapResult = objectMapFunction(fromObj);
                    if (!mapResult.Skip) {
                        p.SetValue(instanceObjext, mapResult.Value);
                    }
                };
            }

            var repeatedObjectMapFunction = Value.MaybeRepeatedFieldFromBigQueryFunction(p.PropertyType);
            if (null != repeatedObjectMapFunction)
            {
                return (fromObj, instanceObjext) =>
                {
                    var result = repeatedObjectMapFunction(fromObj);
                    if (!result.Skip) {
                        p.SetValue(instanceObjext, result.Value);
                    }
                };
            }

            var elementMapFunction = Record.GetValueFromBigQueryDictionaryFunction(p.PropertyType);
            return (fromObj, instanceObject) =>
            {
                p.SetValue(instanceObject, elementMapFunction((Dictionary<string, object>)fromObj));
            };
        }
    }
}

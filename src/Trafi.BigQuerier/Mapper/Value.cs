// Copyright 2021 TRAFI
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Trafi.BigQuerier.Mapper
{
    public static class Value
    {
        public class FieldOptions
        {
            public BigQueryDbType type;
            public BigQueryFieldMode mode;
        }

        public static FieldOptions MaybeSimpleFieldOptionsFromType(Type type)
        {
            if (type == typeof(string))
            {
                return new FieldOptions
                {
                    type = BigQueryDbType.String,
                    mode = BigQueryFieldMode.Nullable,
                };
            }
            else if (type == typeof(long))
            {
                return new FieldOptions
                {
                    type = BigQueryDbType.Int64,
                    mode = BigQueryFieldMode.Nullable,
                };
            }
            else if (type == typeof(long?))
            {
                return new FieldOptions
                {
                    type = BigQueryDbType.Int64,
                    mode = BigQueryFieldMode.Nullable,
                };
            }
            else if (type == typeof(int))
            {
                return new FieldOptions
                {
                    type = BigQueryDbType.Int64,
                    mode = BigQueryFieldMode.Nullable,
                };
            }
            else if (type == typeof(int?))
            {
                return new FieldOptions
                {
                    type = BigQueryDbType.Int64,
                    mode = BigQueryFieldMode.Nullable,
                };
            }
            else if (type == typeof(double))
            {
                return new FieldOptions
                {
                    type = BigQueryDbType.Float64,
                    mode = BigQueryFieldMode.Nullable,
                };
            }
            else if (type == typeof(double?))
            {
                return new FieldOptions
                {
                    type = BigQueryDbType.Float64,
                    mode = BigQueryFieldMode.Nullable,
                };
            }
            else if (type == typeof(bool))
            {
                return new FieldOptions
                {
                    type = BigQueryDbType.Bool,
                    mode = BigQueryFieldMode.Nullable,
                };
            }
            else if (type == typeof(bool?))
            {
                return new FieldOptions
                {
                    type = BigQueryDbType.Bool,
                    mode = BigQueryFieldMode.Nullable,
                };
            }
            else if (type == typeof(DateTime))
            {
                return new FieldOptions
                {
                    type = BigQueryDbType.Timestamp,
                    mode = BigQueryFieldMode.Nullable,
                };
            }
            else if (type == typeof(DateTime?))
            {
                return new FieldOptions
                {
                    type = BigQueryDbType.Timestamp,
                    mode = BigQueryFieldMode.Nullable,
                };
            }

            return null;
        }

        public static Func<object, object> MaybeFieldToBigQueryFunction(Type type)
        {
            if (type == typeof(string))
            {
                return v => v;
            }
            else if (type == typeof(long))
            {
                return v => v;
            }
            else if (type == typeof(long?))
            {
                return v => v;
            }
            else if (type == typeof(int))
            {
                return v => (long)(int)v;
            }
            else if (type == typeof(int?))
            {
                return v => (long?)(int?)v;
            }
            else if (type == typeof(double))
            {
                return v => v;
            }
            else if (type == typeof(double?))
            {
                return v => v;
            }
            else if (type == typeof(bool))
            {
                return v => v;
            }
            else if (type == typeof(bool?))
            {
                return v => v;
            }
            else if (type == typeof(DateTime))
            {
                return v => v;
            }
            else if (type == typeof(DateTime?))
            {
                return v => v;
            }

            return null;
        }

        public struct MapResult
        {
            public object Value;
            public bool Skip;
        }

        private static Func<object, MapResult> TryCastLongToIntFunction()
        {
            return o => o is long l && l <= int.MaxValue 
                    ? new MapResult { Value = (int)l }
                    : new MapResult { Skip = true };
        }

        private static Func<object, MapResult> TryCastLongToIntNullableFunction()
        {
            return o => o is long l && l <= int.MaxValue
                ? new MapResult { Value = (int?)l }
                : o == null 
                  ? new MapResult { Value = null }
                  : new MapResult { Skip = true };
        }

        private static Func<object, MapResult> DirectMapIfTypeFunction<T>()
        {
            return o => o is T
                ? new MapResult { Value = (T)o }
                : new MapResult { Skip = true };
        }

        public static Func<object, MapResult> MaybeFieldFromBigQueryFunction(Type type)
        {
            if (type == typeof(string))
            {
                return DirectMapIfTypeFunction<string>();
            }
            else if (type == typeof(long))
            {
                return DirectMapIfTypeFunction<long>();
            }
            else if (type == typeof(long?))
            {
                return DirectMapIfTypeFunction<long?>();
            }
            else if (type == typeof(int))
            {
                return TryCastLongToIntFunction();
            }
            else if (type == typeof(int?))
            {
                return TryCastLongToIntNullableFunction();
            }
            else if (type == typeof(double))
            {
                return o => o is double
                    ? new MapResult { Value = o as double? }
                    : new MapResult { Skip = true };
            }
            else if (type == typeof(double?))
            {
                return o => o is double
                    ? new MapResult { Value = o as double? }
                    : new MapResult { Skip = true };
            }
            else if (type == typeof(bool))
            {
                return o => o is bool
                    ? new MapResult { Value = o as bool? }
                    : new MapResult { Skip = true };
            }
            else if (type == typeof(bool?))
            {
                return o => o is bool
                    ? new MapResult { Value = o as bool? }
                    : new MapResult { Skip = true };
            }
            else if (type == typeof(DateTime))
            {
                return o => o is DateTime
                    ? new MapResult { Value = o as DateTime? }
                    : new MapResult { Skip = true };
            }
            else if (type == typeof(DateTime?))
            {
                return o => o is DateTime
                    ? new MapResult { Value = o as DateTime? }
                    : new MapResult { Skip = true };
            }

            return null;
        }

        public static Func<object, object> MaybeRepeatedFieldToBigQueryFunction(Type type)
        {
            if (type.IsArray)
            {
                var elementType = type.GetElementType();
                if (Record.IsContractType(elementType))
                {
                    var elementMapFunction = Record.GetValueToBigQueryFunction(elementType);
                    return value => ArrayValueToBigQuery<BigQueryInsertRow>(value, elementMapFunction);
                }
                else
                {
                    var elementMapFunction = MaybeFieldToBigQueryFunction(elementType);

                    if (elementType == typeof(string))
                    {
                        return value => ArrayValueToBigQuery<string>(value, elementMapFunction);
                    }
                    else if (elementType == typeof(long))
                    {
                        return value => ArrayValueToBigQuery<long>(value, elementMapFunction);
                    }
                    else if (elementType == typeof(long?))
                    {
                        return value => ArrayValueToBigQuery<long?>(value, elementMapFunction);
                    }
                    else if (elementType == typeof(int))
                    {
                        return value => ArrayValueToBigQuery<long>(value, elementMapFunction);
                    }
                    else if (elementType == typeof(int?))
                    {
                        return value => ArrayValueToBigQuery<long?>(value, elementMapFunction);
                    }
                    else if (elementType == typeof(double))
                    {
                        return value => ArrayValueToBigQuery<double>(value, elementMapFunction);
                    }
                    else if (elementType == typeof(double?))
                    {
                        return value => ArrayValueToBigQuery<double?>(value, elementMapFunction);
                    }
                    else if (elementType == typeof(bool))
                    {
                        return value => ArrayValueToBigQuery<bool>(value, elementMapFunction);
                    }
                    else if (elementType == typeof(bool?))
                    {
                        return value => ArrayValueToBigQuery<bool?>(value, elementMapFunction);
                    }
                    else if (elementType == typeof(DateTime))
                    {
                        return value => ArrayValueToBigQuery<DateTime>(value, elementMapFunction);
                    }
                    else if (elementType == typeof(DateTime?))
                    {
                        return value => ArrayValueToBigQuery<DateTime?>(value, elementMapFunction);
                    }
                }
            }

            return null;
        }

        public static Func<object, MapResult> MaybeRepeatedFieldFromBigQueryFunction(Type type)
        {
            if (type.IsArray)
            {
                var elementType = type.GetElementType();
                if (Record.IsContractType(elementType))
                {
                    var elementMapFunction = Record.GetValueFromBigQueryDictionaryFunction(elementType);
                    return value => {
                        if (value == null)
                        {
                            return new MapResult { Value = Array.CreateInstance(elementType, 0), Skip = false };
                        }

                        var list = (Dictionary<string, object>[])value;
                        var mapped = list
                            .Select(elementMapFunction)
                            .ToArray();
                        var array = Array.CreateInstance(elementType, mapped.Length);
                        Array.Copy(mapped, array, mapped.Length);

                        return new MapResult { Value = array, Skip = false };
                    };
                }
                else
                {
                    var elementMapFunction = MaybeFieldFromBigQueryFunction(elementType);
                    if (elementType == typeof(string))
                    {
                        return value => NullableClassArrayValueFromBigQuery<string>(value, elementMapFunction);
                    }
                    else if (elementType == typeof(long))
                    {
                        return value => PrimitiveArrayValueFromBigQuery<long>(value, elementMapFunction);
                    }
                    else if (elementType == typeof(long?))
                    {
                        return value => NullablePrimitiveArrayValueFromBigQuery<long>(value, elementMapFunction);
                    }
                    else if (elementType == typeof(int))
                    {
                        return value => IntegerArrayValueFromBigQuery(value, elementMapFunction);
                    }
                    else if (elementType == typeof(int?))
                    {
                        return value => NullableIntegerArrayValueFromBigQuery(value, elementMapFunction);
                    }
                    else if (elementType == typeof(double))
                    {
                        return value => PrimitiveArrayValueFromBigQuery<double>(value, elementMapFunction);
                    }
                    else if (elementType == typeof(double?))
                    {
                        return value => NullablePrimitiveArrayValueFromBigQuery<double>(value, elementMapFunction);
                    }
                    else if (elementType == typeof(bool))
                    {
                        return value => PrimitiveArrayValueFromBigQuery<bool>(value, elementMapFunction);
                    }
                    else if (elementType == typeof(bool?))
                    {
                        return value => NullablePrimitiveArrayValueFromBigQuery<bool>(value, elementMapFunction);
                    }
                    else if (elementType == typeof(DateTime))
                    {
                        return value => PrimitiveArrayValueFromBigQuery<DateTime>(value, elementMapFunction);
                    }
                    else if (elementType == typeof(DateTime?))
                    {
                        return value => NullablePrimitiveArrayValueFromBigQuery<DateTime>(value, elementMapFunction);
                    }
                }
            }

            return null;
        }

        private static T[] ArrayValueToBigQuery<T>(object value, Func<object, object> valueToBigQueryFunction)
        {
            if (value == null) return new T[0];

            var array = (Array)value;
            var rows = new List<T>(array.Length);
            foreach (var element in array)
            {
                rows.Add((T)valueToBigQueryFunction(element));
            }
            return rows.ToArray();
        }

        private static MapResult PrimitiveArrayValueFromBigQuery<T>(object value, Func<object, MapResult> elementMapFunction) where T: struct
        {
            if (!(value is T[])) return new MapResult { Skip = true };
            var array = value as T[];
            if (array == null) return new MapResult { Value = new T[0] };

            return new MapResult
            {
                Value = array
                    .Select(i => elementMapFunction(i))
                    .Where(i => !i.Skip)
                    .Select(i => (T)i.Value)
                    .ToArray()
            };
        }

        private static MapResult IntegerArrayValueFromBigQuery(object value, Func<object, MapResult> elementMapFunction)
        {
            if (!(value is long[])) return new MapResult { Skip = true };
            var array = value as long[];
            if (array == null) return new MapResult { Value = new long[0] };

            return new MapResult
            {
                Value = array
                    .Select(i => elementMapFunction(i))
                    .Where(i => !i.Skip)
                    .Select(i => (int)i.Value)
                    .ToArray()
            };
        }

        private static MapResult NullablePrimitiveArrayValueFromBigQuery<T>(object value, Func<object, MapResult> elementMapFunction) where T: struct
        {
            if (!(value is T?[])) return new MapResult { Skip = true };
            var array = value as T?[];
            if (array == null) return new MapResult { Value = new T?[0] };

            return new MapResult
            {
                Value = array
                    .Select(i => elementMapFunction(i))
                    .Select(i => i.Skip ? null : (T?)i.Value)
                    .ToArray()
            };
        }

        private static MapResult NullableIntegerArrayValueFromBigQuery(object value, Func<object, MapResult> elementMapFunction)
        {
            if (!(value is long?[])) return new MapResult { Skip = true };
            var array = value as long?[];
            if (array == null) return new MapResult { Value = new long?[0] };

            return new MapResult
            {
                Value = array
                    .Select(i => elementMapFunction(i))
                    .Select(i => i.Skip ? null : (int?)i.Value)
                    .ToArray()
            };
        }

        private static MapResult NullableClassArrayValueFromBigQuery<T>(object value, Func<object, MapResult> elementMapFunction) where T : class
        {
            if (!(value is T[])) return new MapResult { Skip = true };
            var array = value as T[];
            if (array == null) return new MapResult { Value = new T[0] };

            return new MapResult
            {
                Value = array
                    .Select(i => elementMapFunction(i))
                    .Select(i => i.Skip ? null : (T)i.Value)
                    .ToArray()
            };
        }
    }
}

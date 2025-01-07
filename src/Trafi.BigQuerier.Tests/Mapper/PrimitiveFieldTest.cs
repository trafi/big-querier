using FluentAssertions;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using Trafi.BigQuerier.Mapper;

namespace Trafi.BigQuerier.Tests.Mapper;

[TestFixture]
public class PrimitiveFieldTest
{
    [QuerierContract]
    private class With<T>
    {
        public T Prop { get; set; }
    }

    [QuerierContract]
    private class WithArray<T>
    {
        public T[] Prop { get; set; }
    }

    #region Simple Field

    #region Simple Field Schema

    private static TestCaseData MakeSchemaFieldTestCase<T>(string bigQueryType)
    {
        return new TestCaseData(bigQueryType, Contract<With<T>>.Create().Cache)
        {
            TestName = $"{typeof(T)} fields should be {bigQueryType} in schema"
        };
    }

    private static IEnumerable<TestCaseData> AddSchemaFieldTestCases()
    {
        yield return MakeSchemaFieldTestCase<string>("STRING");
        yield return MakeSchemaFieldTestCase<long>("INTEGER");
        yield return MakeSchemaFieldTestCase<long?>("INTEGER");
        yield return MakeSchemaFieldTestCase<int>("INTEGER");
        yield return MakeSchemaFieldTestCase<int?>("INTEGER");
        yield return MakeSchemaFieldTestCase<double>("FLOAT");
        yield return MakeSchemaFieldTestCase<double?>("FLOAT");
        yield return MakeSchemaFieldTestCase<DateTime>("TIMESTAMP");
        yield return MakeSchemaFieldTestCase<DateTime?>("TIMESTAMP");
        yield return MakeSchemaFieldTestCase<bool>("BOOLEAN");
        yield return MakeSchemaFieldTestCase<bool?>("BOOLEAN");
    }

    [Test, TestCaseSource(nameof(AddSchemaFieldTestCases))]
    public void SchemaFieldTypeShouldBeCorrect(string bigQueryType, ContractCache contract)
    {
        contract.Schema.Fields.Should().HaveCount(1);
        contract.Schema.Fields[0].Name.Should().Be("Prop");
        contract.Schema.Fields[0].Mode.Should().Be("NULLABLE");
        contract.Schema.Fields[0].Type.Should().Be(bigQueryType);
    }

    #endregion

    #region Simple Field Mapping

    private static TestCaseData MakeFieldMappingTestCase<TApp, TBigQuery>(TApp appValue, TBigQuery bigQueryValue,
        bool fetchOnly, bool shouldSkip)
    {
        var type = typeof(TApp);
        var caseMessage = fetchOnly ? "work for select" : "work for insert and select";
        return new TestCaseData(type, appValue, bigQueryValue,
            new Value.MapResult {Value = appValue, Skip = shouldSkip}, fetchOnly)
        {
            TestName = $"{typeof(TApp)} field mapping should {caseMessage}"
        };
    }

    private static IEnumerable<TestCaseData> AddFieldMappingTestCases()
    {
        yield return MakeFieldMappingTestCase<string, string>
            (appValue: "Hello", bigQueryValue: "Hello", fetchOnly: false, shouldSkip: false);
        yield return MakeFieldMappingTestCase<string, int>
            (appValue: "Hello", bigQueryValue: 123, fetchOnly: true, shouldSkip: true);

        yield return MakeFieldMappingTestCase<long, long>
            (appValue: 123, bigQueryValue: 123, fetchOnly: false, shouldSkip: false);
        yield return MakeFieldMappingTestCase<long, string>
            (appValue: 123, bigQueryValue: "garbage", fetchOnly: true, shouldSkip: true);
        yield return MakeFieldMappingTestCase<long?, long?>
            (appValue: 123, bigQueryValue: 123, fetchOnly: false, shouldSkip: false);
        yield return MakeFieldMappingTestCase<long?, string>
            (appValue: 123, bigQueryValue: "garbage", fetchOnly: true, shouldSkip: true);
        yield return MakeFieldMappingTestCase<long?, long>
            (appValue: 123, bigQueryValue: 123, fetchOnly: false, shouldSkip: false);
        yield return MakeFieldMappingTestCase<long, long?>
            (appValue: 123, bigQueryValue: 123, fetchOnly: false, shouldSkip: false);


        yield return MakeFieldMappingTestCase<int, string>
            (appValue: 123, bigQueryValue: "garbage", fetchOnly: true, shouldSkip: true);
        yield return MakeFieldMappingTestCase<int?, string>
            (appValue: 123, bigQueryValue: "garbage", fetchOnly: true, shouldSkip: true);
        yield return MakeFieldMappingTestCase<int, long>
            (appValue: 123, bigQueryValue: 123, fetchOnly: true, shouldSkip: false);
        yield return MakeFieldMappingTestCase<int, long>
            (appValue: 123, bigQueryValue: long.MaxValue, fetchOnly: true, shouldSkip: true);
        yield return MakeFieldMappingTestCase<int?, long>
            (appValue: 123, bigQueryValue: 123, fetchOnly: false, shouldSkip: false);
        yield return MakeFieldMappingTestCase<int?, long?>
            (appValue: 123, bigQueryValue: 123, fetchOnly: false, shouldSkip: false);
        yield return MakeFieldMappingTestCase<int, long?>
            (appValue: 123, bigQueryValue: 123, fetchOnly: false, shouldSkip: false);

        yield return MakeFieldMappingTestCase<double, double>
            (appValue: 123, bigQueryValue: 123, fetchOnly: false, shouldSkip: false);
        yield return MakeFieldMappingTestCase<double, string>
            (appValue: 123, bigQueryValue: "garbage", fetchOnly: true, shouldSkip: true);

        yield return MakeFieldMappingTestCase<DateTime, DateTime>
        (appValue: DateTime.Parse("2018-03-04 18:23:45"),
            bigQueryValue: DateTime.Parse("2018-03-04 18:23:45"), fetchOnly: false, shouldSkip: false);
        yield return MakeFieldMappingTestCase<DateTime, string>
        (appValue: DateTime.Parse("2018-03-04 18:23:45"),
            bigQueryValue: "garbage", fetchOnly: true, shouldSkip: true);

        yield return MakeFieldMappingTestCase<bool, bool>
            (appValue: true, bigQueryValue: true, fetchOnly: false, shouldSkip: false);
        yield return MakeFieldMappingTestCase<bool, string>
            (appValue: true, bigQueryValue: "garbage", fetchOnly: true, shouldSkip: true);
    }

    [Test, TestCaseSource(nameof(AddFieldMappingTestCases))]
    public void FieldMappedValueShouldBeCorrect(Type type, object value, object expectedBigQueryValue,
        Value.MapResult expectedValueAgain, bool fetchOnly)
    {
        var fieldToBigQuery = Value.MaybeFieldToBigQueryFunction(type);
        var fieldFromBigQuery = Value.MaybeFieldFromBigQueryFunction(type);

        object bigQueryValue;
        if (!fetchOnly)
        {
            bigQueryValue = fieldToBigQuery(value);
            bigQueryValue
                .Should().BeEquivalentTo(expectedBigQueryValue,
                    $"{type} value {value} should be {expectedBigQueryValue} when converted to big query value");
        }
        else
        {
            bigQueryValue = expectedBigQueryValue;
        }

        var valueAgain = fieldFromBigQuery(bigQueryValue);
        if (expectedValueAgain.Skip)
        {
            valueAgain.Skip.Should()
                .BeTrue($"Should skip setting {type} value {bigQueryValue} when converting from big query");
        }
        else
        {
            valueAgain.Value.Should().BeEquivalentTo(expectedValueAgain.Value,
                $"Should set {type} value {bigQueryValue} to {expectedValueAgain.Value} when converting from big query");
        }
    }

    #endregion

    #endregion

    #region Simple field with Array values

    #region Simple field with Array values schema

    private static TestCaseData MakeSchemaArrayFieldTestCase<T>(string bigQueryType)
    {
        return new TestCaseData(bigQueryType, Contract<WithArray<T>>.Create().Cache)
        {
            TestName = $"{typeof(T)} array fields should be REPEATED in schema and have {bigQueryType} type"
        };
    }

    private static IEnumerable<TestCaseData> AddSchemaArrayFieldTestCases()
    {
        yield return MakeSchemaArrayFieldTestCase<string>("STRING");
        yield return MakeSchemaArrayFieldTestCase<long>("INTEGER");
        yield return MakeSchemaArrayFieldTestCase<long?>("INTEGER");
        yield return MakeSchemaArrayFieldTestCase<int>("INTEGER");
        yield return MakeSchemaArrayFieldTestCase<int?>("INTEGER");
        yield return MakeSchemaArrayFieldTestCase<double>("FLOAT");
        yield return MakeSchemaArrayFieldTestCase<double?>("FLOAT");
        yield return MakeSchemaArrayFieldTestCase<DateTime>("TIMESTAMP");
        yield return MakeSchemaArrayFieldTestCase<DateTime?>("TIMESTAMP");
        yield return MakeSchemaArrayFieldTestCase<bool>("BOOLEAN");
        yield return MakeSchemaArrayFieldTestCase<bool?>("BOOLEAN");
    }

    [Test, TestCaseSource(nameof(AddSchemaArrayFieldTestCases))]
    public void SchemaArrayFieldTypeShouldBeCorrect(string bigQueryType, ContractCache contract)
    {
        contract.Schema.Fields.Should().HaveCount(1);
        contract.Schema.Fields[0].Name.Should().Be("Prop");
        contract.Schema.Fields[0].Mode.Should().Be("REPEATED");
        contract.Schema.Fields[0].Type.Should().Be(bigQueryType);
    }

    #endregion

    #region Simple Field Mapping

    private static TestCaseData MakeArrayFieldMappingTestCase<TApp, TBigQuery>(TApp[] appValue,
        TBigQuery[] bigQueryValue, bool fetchOnly, bool shouldSkip)
    {
        var type = typeof(TApp);
        return new TestCaseData(type, appValue, bigQueryValue,
            new Value.MapResult {Value = appValue, Skip = shouldSkip}, fetchOnly)
        {
            TestName = $"{type} array field mapping should work for insert and select"
        };
    }

    private static IEnumerable<TestCaseData> AddArrayFieldMappingTestCases()
    {
        yield return MakeArrayFieldMappingTestCase<string, string>
            (appValue: new[] {"Hello"}, bigQueryValue: new[] {"Hello"}, fetchOnly: false, shouldSkip: false);
        yield return MakeArrayFieldMappingTestCase<string, long>
            (appValue: new string[] { }, bigQueryValue: new long[] {123}, fetchOnly: true, shouldSkip: true);

        yield return MakeArrayFieldMappingTestCase<long, long>
        (appValue: new long[] {42, 123}, bigQueryValue: new long[] {42, 123}, fetchOnly: false,
            shouldSkip: false);
        yield return MakeArrayFieldMappingTestCase<long, bool>
            (appValue: new long[] {42, 123}, bigQueryValue: new bool[] {true}, fetchOnly: true, shouldSkip: true);
        yield return MakeArrayFieldMappingTestCase<long?, long?>
        (appValue: new long?[] {42, null, 123}, bigQueryValue: new long?[] {42, null, 123}, fetchOnly: false,
            shouldSkip: false);
        yield return MakeArrayFieldMappingTestCase<long?, bool?>
            (appValue: new long?[] {42, 123}, bigQueryValue: new bool?[] {true}, fetchOnly: true, shouldSkip: true);
        yield return MakeArrayFieldMappingTestCase<long, long?>
        (appValue: new long[] {42, 123}, bigQueryValue: new long?[] {42, 123}, fetchOnly: false,
            shouldSkip: false);
        yield return MakeArrayFieldMappingTestCase<long?, long>
        (appValue: new long?[] {42, 123}, bigQueryValue: new long[] {42, 123}, fetchOnly: false,
            shouldSkip: false);

        yield return MakeArrayFieldMappingTestCase<int, long>
        (appValue: new int[] { 42, 123 }, bigQueryValue: new long[] { 42, 123 }, fetchOnly: false,
            shouldSkip: false);
        yield return MakeArrayFieldMappingTestCase<int, bool>
            (appValue: new int[] { 42, 123 }, bigQueryValue: new bool[] { true }, fetchOnly: true, shouldSkip: true);
        yield return MakeArrayFieldMappingTestCase<int?, long?>
        (appValue: new int?[] { 42, null, 123 }, bigQueryValue: new long?[] { 42, null, 123 }, fetchOnly: false,
            shouldSkip: false);
        yield return MakeArrayFieldMappingTestCase<int?, bool?>
            (appValue: new int?[] { 42, 123 }, bigQueryValue: new bool?[] { true }, fetchOnly: true, shouldSkip: true);
        yield return MakeArrayFieldMappingTestCase<int, long?>
        (appValue: new int[] { 42, 123 }, bigQueryValue: new long?[] { 42, 123 }, fetchOnly: false,
            shouldSkip: false);
        yield return MakeArrayFieldMappingTestCase<int?, long>
        (appValue: new int?[] { 42, 123 }, bigQueryValue: new long[] { 42, 123 }, fetchOnly: false,
            shouldSkip: false);
        yield return MakeArrayFieldMappingTestCase<int?, long>
        (appValue: new int?[] { 42, 123 }, bigQueryValue: new long[] { 42, long.MaxValue }, fetchOnly: true,
            shouldSkip: true);



        yield return MakeArrayFieldMappingTestCase<double, double>
        (appValue: new double[] {42, 123}, bigQueryValue: new double[] {42, 123}, fetchOnly: false,
            shouldSkip: false);
        yield return MakeArrayFieldMappingTestCase<double, bool>
            (appValue: new double[] {42, 123}, bigQueryValue: new bool[] {true}, fetchOnly: true, shouldSkip: true);

        yield return MakeArrayFieldMappingTestCase<DateTime, DateTime>
        (appValue: new DateTime[]
        {
            DateTime.Parse("2019-11-10 11:23:11"),
            DateTime.Parse("2019-11-10 11:23:12")
        }, bigQueryValue: new DateTime[]
        {
            DateTime.Parse("2019-11-10 11:23:11"),
            DateTime.Parse("2019-11-10 11:23:12")
        }, fetchOnly: false, shouldSkip: false);
        yield return MakeArrayFieldMappingTestCase<DateTime, bool>
        (appValue: new DateTime[]
        {
            DateTime.Parse("2019-11-10 11:23:11"),
            DateTime.Parse("2019-11-10 11:23:12")
        }, bigQueryValue: new bool[] {true}, fetchOnly: true, shouldSkip: true);

        yield return MakeArrayFieldMappingTestCase<bool, bool>
        (appValue: new bool[] {true, false}, bigQueryValue: new bool[] {true, false}, fetchOnly: false,
            shouldSkip: false);
        yield return MakeArrayFieldMappingTestCase<bool, string>
        (appValue: new bool[] {true, false}, bigQueryValue: new string[] {"garbage"}, fetchOnly: true,
            shouldSkip: true);
    }

    [Test, TestCaseSource(nameof(AddArrayFieldMappingTestCases))]
    public void ArrayFieldMappedValueShouldBeCorrect(Type type, object value, object expectedBigQueryValue,
        Value.MapResult expectedValueAgain, bool fetchOnly)
    {
        var fieldToBigQuery = Value.MaybeRepeatedFieldToBigQueryFunction(type.MakeArrayType());
        var fieldFromBigQuery = Value.MaybeRepeatedFieldFromBigQueryFunction(type.MakeArrayType());

        object bigQueryValue;
        if (!fetchOnly)
        {
            bigQueryValue = fieldToBigQuery(value);
            bigQueryValue
                .Should().BeEquivalentTo(expectedBigQueryValue);
        }
        else
        {
            bigQueryValue = expectedBigQueryValue;
        }

        if (expectedValueAgain.Skip)
        {
            fieldFromBigQuery(bigQueryValue).Skip.Should().BeTrue("Should skip");
        }
        else
        {
            fieldFromBigQuery(bigQueryValue).Value.Should().BeEquivalentTo(expectedValueAgain.Value);
        }
    }

    #endregion

    #endregion
}

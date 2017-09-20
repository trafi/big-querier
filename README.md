# Big Querier

[![Build Status](https://travis-ci.org/trafi/big-querier.svg?branch=master)](https://travis-ci.org/trafi/big-querier)

Big Queries for Simple People.

## What it is

Big Querier helps to automatically map objects to big query insert rows and vice-versa. Therefore, it can be thought of as simple ORM. However, there are 3 parts of big querier, each of them can be used separately:

- __The Client__: A Big Query wrapper that simplifies query creation and result retrieval.
- __The Contract__: A Row-to-object mapper, can convert object to row for insertion or selected row back to object.
- __The Dispatcher Service__: A backend logger helper that dispatches logs to BigQuery in background.

## Client Usage

```c#
var client = new Trafi.BigQuerier.BigQueryClient(
    "trafi-app-dev",
    "../Much/Path/To/Cert.p12",
    "very-secret",
    "very-mail@developer.gserviceaccount.com"
);
```

Has method to get or create a table:

```c#
var table = await client
    .GetTableClient("dataset_id", "table_id", yourSchema);
```

And then insert a row:

```c#
await table.InsertRows(new[] {
    new BigQueryInsertRow { ... }
});
```

Or get the rows:

```c#
var results = await client
    .Query("SELECT * FROM dataset_id.table_id");
```

This library throws `BigQuerierException` exceptions.

## Features for Simple People

Describe your contract:

```c#
[QuerierContract]
class Item
{
    public string Name { get; set; }
    public long? Count { get; set; }
    public long[] Values { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? ModifiedAt { get; set; }
}

[QuerierContract]
class MyItem
{
    public string MyKey { get; set; }
    public bool? MyValue { get; set; }
    public Item[] Items { get; set; }
    public string[] Strings { get; set; }
    public long[] Values { get; set; }
    public Item Subitem { get; set; }
}
```

Supported types are `string`, `long`, `double`, `bool`, `DateTime`,
arrays or optional values of all mentioned types, arrays of other
contracts and other contracts as properties.

Start by creating a contract. This auto-generates a mapper
that can be reused:

```c#
var contract = Contract<MyItem>.Create();
```

Contract builds schema for you, so it's easier to create tables:

```c#
var table = await client
    .GetTableClient("dataset_id", "table_id", contract.Schema);
```

Contract helps to convert your items to rows:

```c#
var row = contract.ToRow(
    new MyItem
    {
        MyKey = "0001",
        MyValue = false,
        Items = new Item[]
        {
            new Item
            {
                Name = "Nerijus",
                CreatedAt = DateTime.UtcNow,
                ModifiedAt = DateTime.UtcNow,
            }
        },
        Strings = new string[] {"A", "B", "C"},
        Values = new long[] {42, 11, 111},
        Subitem = new Item
        {
            Name = "Stuff",
            Count = 3,
            Values = new long[] {42, 11, 111},
            CreatedAt = DateTime.UtcNow,
        },
    }
);
await table.InsertRows(new[] {row});
```

Contract helps to convert rows back to items:

```c#
var results = await contract.FromRowsAsync(
    await client.Query(
        "SELECT * FROM dataset_id.table_id"
    )
);
```

It is possible to specify query options and cancellation token, both are optional.
As an example, we can fall back to Legacy SQL dialect (Google's BigQuery client uses Standard SQL by default):

```c#
using Google.Cloud.BigQuery.V2;

var rows = await _contract.FromRowsAsync(
    await client.Query(
        "SELECT * FROM dataset_id.table_id",
        options: new QueryOptions
        {
            UseLegacySql = true,
        },
        ct: cancellationToken),
    cancellationToken);
```

## License

Licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.

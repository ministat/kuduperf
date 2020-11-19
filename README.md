
# Target

This repo targets to evaluate kudu point lookup performance. The table has more than 100 columns, it DDL is like:

```
CREATE TABLE kudu_poc.kudu_marketing_item(
  item_id long not null,
  curnt_price double,
  price_update_time timestamp
  ...
)
USING kudu OPTIONS (
  kudu.tableHashPartitions 'item_id,512'
)

```
# Build
```
mvn clean package
```

# Run
```
java -jar kudu-mt-query-itemid-1.0-SNAPSHOT.jar -f item_ids.txt -k xxx0:7051,xxx1:7051,xxx2:7051 -t kudu_table -i 200
```

the item_ids.txt contains item_id:

```
114419336369
114419348892
114419348924
114419366922
114419369029
114419394436
114419395353
114419396218
114419399917
114419460482
114419463335
114419469847
114419477181
114419478283
114419502018
114419513353
114419520145
```

# Output
```
iteration: 1
Table 'kudu_marketing_item_store_nov13' colums: 134
Run 1 scans take 1056 ms
The SQL is like "select curnt_price from kudu_marketing_item_store_nov13 where item_id = ?"
item: 114417465925 price: 30.0
item: 114417507056 price: 7.99
item: 114417508976 price: 190.0
item: 114417509062 price: 12.71
item: 114417512889 price: 10.1
item: 114417515816 price: 37.06
item: 114417517279 price: 14.08
...
item: 114419513353 price: 39.99
item: 114419520145 price: 52.35
```

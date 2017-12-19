# Trane
A software package that takes any dataset as input and generates prediction problems relevant to the data. 

# Assumptions
Only filter and operate on a column whose type is TYPE\_VALUE.
- So filter operations are always applied on a TYPE\_VALUE column and doesn't change the column type.
- Row operations are always applied on a TYPE\_VALUE column and may generate a TYPE\_VALUE or TYPE\_BOOL column.
  - if Row operations generate TYPE\_VALUE, identity and diff transformation operations can be used. The output is TYPE\_VALUE.
  - if Row operations generate TYPE\_BOOL, only identity is applicable. The output is TYPE\_BOOL.
- The first, last, sum, count aggregation operations are applicable on both TYPE\_VALUE and TYPE\_BOOL. The FML operation only takes TYPE_VALUE.
- The NL description system only works with the previous assumptions.

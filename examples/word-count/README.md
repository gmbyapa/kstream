# KStream Examples - Word Count

```mermaid
graph TD;
A[ Text Lines ] --> B( Split Words )
--> C( Select word as key)
--> D( Aggregate - Count words ) --> E[ Word Count ]
```

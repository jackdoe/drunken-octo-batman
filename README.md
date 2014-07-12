drunken-octo-batman
===================

just some lucene examples (github suggested repo name drunken-octo-batman, how can you not use that!)

everything is in one java file [Main.java](https://github.com/jackdoe/drunken-octo-batman/blob/master/src/main/java/no/Main.java#L23)

* [how to create Directory and IndexWriter](https://github.com/jackdoe/drunken-octo-batman/blob/master/src/main/java/no/Main.java#L27)
* [how to load json file into the index using whitespace analyzer](https://github.com/jackdoe/drunken-octo-batman/blob/master/src/main/java/no/Main.java#L40)
* [how to extract data from the index without searching](https://github.com/jackdoe/drunken-octo-batman/blob/master/src/main/java/no/Main.java#L83)
* [how to search](https://github.com/jackdoe/drunken-octo-batman/blob/master/src/main/java/no/Main.java#L114)
 + [explanation about IndexSearcher.search()](https://github.com/jackdoe/drunken-octo-batman/blob/master/src/main/java/no/Main.java#L141)
 + search illustration without collector
   + [search without collector custom query with custom weight and custom scorer](https://github.com/jackdoe/drunken-octo-batman/blob/master/src/main/java/no/Main.java#L296)
   + [search without collector with custom query that can create its own scorer](https://github.com/jackdoe/drunken-octo-batman/blob/master/src/main/java/no/Main.java#L310)
   + [the query](https://github.com/jackdoe/drunken-octo-batman/blob/master/src/main/java/no/Main.java#L324)
   + [the weight](https://github.com/jackdoe/drunken-octo-batman/blob/master/src/main/java/no/Main.java#L354)
   + [the scorer](https://github.com/jackdoe/drunken-octo-batman/blob/master/src/main/java/no/Main.java#L393)
 + [search without using lucene's search infrastructure, using only IndexReader](https://github.com/jackdoe/drunken-octo-batman/blob/master/src/main/java/no/Main.java#L253)
   + [Primitive.search](https://github.com/jackdoe/drunken-octo-batman/blob/master/src/main/java/no/Main.java#L467)
   + [PrimitiveTerm](https://github.com/jackdoe/drunken-octo-batman/blob/master/src/main/java/no/Main.java#L489)
   + [PrimitiveAndQuery](https://github.com/jackdoe/drunken-octo-batman/blob/master/src/main/java/no/Main.java#L538)
   + [PrimitiveOrQuery](https://github.com/jackdoe/drunken-octo-batman/blob/master/src/main/java/no/Main.java#L628)


* todo:
  + term stats example
  + similarity examples
  + concurrent searches


* how to run the thing: sh run.sh (will do cat data.txt | java, to index the json content of [data.txt](https://github.com/jackdoe/drunken-octo-batman/blob/master/data.txt), the whole project requires [maven](http://maven.apache.org/what-is-maven.html) and java 1.7

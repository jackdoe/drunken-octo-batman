package no;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.io.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.core.*;
import org.apache.lucene.analysis.miscellaneous.*;
import org.apache.lucene.analysis.ngram.*;
import org.apache.lucene.analysis.payloads.*;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.FieldInfo.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.*;
import java.io.*;
import com.google.gson.*;
import com.google.gson.stream.*;
import java.util.*;


class Main {
    public static File ROOT = new File("/tmp/no.index");
    public static WhitespaceAnalyzer whitespace = new WhitespaceAnalyzer(Version.LUCENE_48);

    public static void main(String[] args) throws Exception {
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_48, whitespace);
        config.setWriteLockTimeout(60);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        Directory dir = new NIOFSDirectory(ROOT);
        IndexWriter writer = new IndexWriter(dir,config);

        load(writer);
        dump(writer);
        search(writer);
        writer.close();
    }

    public static void load(IndexWriter writer) throws Exception{
        /*

          read STDIN and parse json encoded array of hashes
          [ { "field": "value" }, .. ]

          # pseudo code
          use strict;
          use warnings;
          use JSON::SL;
          my $p = JSON::SL->new;
          $p->set_jsonpointer(["/^"]);
          while (my $buf = <STDIN>) {
              $p->feed($buf);
              while (my $obj = $p->fetch) {
                  my $doc = LUCENE::DOCUMENT->new();
                  while(my ($key,$value) = each(%$obj)) {
                      $doc->add($key,$value,LUCENE::FIELD::STORE,LUCENE::FIELD::INDEX::ANALYZED);
                  }
                  $writer->addDocument($doc);
              }
          }
          $writer->optimize();

        */
        JsonReader json = new JsonReader(new InputStreamReader(System.in, "UTF-8"));
        json.beginArray();
        Gson gson = new Gson();
        Map<String,String> map = new HashMap<String,String>();
        while (json.hasNext()) {
            map = (Map<String,String>) gson.fromJson(json, map.getClass());
            Document doc = new Document();
            for (Map.Entry<String,String> entry : map.entrySet()) {
                doc.add(new Field(entry.getKey(), entry.getValue(), Field.Store.YES, Field.Index.ANALYZED));
            }
            writer.addDocument(doc);
        }
        json.endArray();
        json.close();
//        writer.forceMerge(1);
        writer.commit();
    }

    public static void dump(IndexWriter writer) throws Exception {
        /*

          my $reader = $writer->reader();
          my $fields = $reader->fields();
          for my $field(@{ $fields }) {
              my $terms = $field->terms();
              while (my $term = $terms->next()) {
                  print $term->toString() . ": docFreq = " . $term->docFreq() . "\n";
              }
          }
          $reader->close();

         */
        IndexReader reader = DirectoryReader.open(writer,false);
        Fields fields = MultiFields.getFields(reader);
        if (fields != null) {
            for (String field : fields) {
                Terms terms = fields.terms(field);
                TermsEnum iterator = terms.iterator(null);
                BytesRef byteRef = null;
                while((byteRef = iterator.next()) != null) {
                    String t = new String(byteRef.bytes, byteRef.offset, byteRef.length);
                    System.out.println(t + ": docFreq = " + String.valueOf(iterator.docFreq()));
                }
            }
        }
        reader.close();
    }


    public static void search(IndexWriter writer) throws Exception{
        /*

          my $reader = $writer->reader();
          my $searcher = $reader->searcher();
          my $query = MatchAllDocsQuery->new();
          my $top = $searcher->search($query,10);
          print "total hits: $top->{total}\n";
          for my $hit(@{ $top->{hits} }) {
              my $doc_id = $hit->{doc_id};
              my $score = $hit->{score};
              my $doc = $searcher->doc($doc_id);
              my $explain = $searcher->explain($query,$doc_id);
              print "doc id: $doc_id, score: $score\n";
              for my $field($doc->fields()) {
                  print $field->name() . " : " . $field->stringValue() . "\n";
              }
          }
          $reader->close();

         */

        IndexReader reader = DirectoryReader.open(writer,false);
        NoIndexSearcher searcher = new NoIndexSearcher(reader);
        Query query = new MatchAllDocsQuery();
        TopDocs results = searcher.search(query, null,100);

        /*
          a little explanation about what searcher.search() does

          1) it will ask the query to create a Weight (https://github.com/apache/lucene-solr/blob/trunk/lucene/core/src/java/org/apache/lucene/search/Weight.java) using the top index reader
             the Weight object has access to the term stats of the reader, and can store some context which can be used
             while scoring
          2) for each leaf of the top index it will ask the weight to create a new BulkScorer using the current leaf(AtomicReaderContext)
              the BulkScorer will create the Weight's Scorer
          3) call scorer.score(leafCollector) (that is the BulkScorer)
             which does:
               int doc;
               // scorer.nextDoc() is the query's weight's scorer
               while ((doc = scorer.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                 collector.collect(doc);
               }

          4) profit

          as you can see the top level query is responsible to do what is right
          lets imagine we have this query:
              bool => {
                      must => [
                          { term => { "first_name" => "jack" } },
                          { term => { "last_name"  => "doe" } },
                      ]
              }
          so looking at the code in 3), we see that the bool query's weight's scorer.nextDoc() will be called
          and what it will do is basically this:
          when initialized it sorts the subScorers (the subqueries's weight's scorers) by cost() (in the term's case
          this is the number of documents), and it takes the first element and assigns it as a leading scorer (the one
          with smallest cost(), in our case lets imagine that this is "last_name.doe")

          pseudo code:

          sort subScorers
          leading_scorer = subScorers[0]

          nextDoc() {
              doc = leading_scorer.nextDoc();
              for(;;) {
              try_again: for(;;) {
                      for (i = 1; i < subScorers.lenght; i++) {
                          if (subScorers[i].doc < doc) {
                             doc_or_next_possible = subScorers[i].advance(doc)
                             if (doc_or_next_possible > doc) {
                                 doc = doc_or_next_possible
                                 break try_again;
                             }
                          }
                      }

                      return doc;
                  }
              }
              // move the leading scorer to the doc_or_next_possible found in some of the other scorers
              // and try again
              doc = leading_scorer.advance(doc);
          }

          a few words about advance(), all advance() calls in all Scorers are supposed to return document id that is >= of
          the requested id (it is undefined behavior if called with target that is less than the current position)

          so what happens is it takes the leading scorer's nextDoc() (lets say that is 78), and asks all other scorers
          to jump to 78, if any of those's advance(78) return a value that is > 78, the loop will break and
          try to advance the leading scorer to this value (lets say the other scorer returned 80), so we will have
          doc = leading_scorer.advance(80), and if the leading_scorer cant go to 80 it will just return the next
          possible id(lets say 90), and then tie looop will try to advance all the other scorers to 90 and so on.

          so you can see how jumpy it is, and how even a query with hundreds of terms can be executed very fast

          calling score() on the boolean query will basically do
              foreach subscorer
                  score += subscorer.score()
              return score;

           usually the weight's scorers also include Similarity score, which is basically calculating a number
           based on the term and index stats that Lucene has.


           as you can see from the example, the top level scorer dictates everything,
           and all queries even in the most complex query chain are aligned at the same document when the
           score is computed

         */

        System.out.println("total hits: " + results.totalHits);
        ScoreDoc[] hits = results.scoreDocs;
        for(int i = 0; i < hits.length; i++) {
            int docId = hits[i].doc;
            Document doc = searcher.doc(docId);
            Explanation explain = searcher.explain(query,docId);
            System.out.println(String.format("doc id: %d, score: %f",docId,hits[i].score));
            System.out.println(explain);
            for (IndexableField field : doc.getFields()) {
                System.out.println("\t" + field.name() + " : " + field.stringValue());
            }
        }

        /*
          you can see simplified and condensed version of search() with a custom
          lucene query that has its own Weight and Scorer, and the hack_with_weight()
          does the while (doc = scorer.nextDoc() != NO_MORE_DOCS) thing.
        */

        searcher.hack_with_weight(new NoTermQuery(new Term("first_name","jack")));

        /*
          even simpler, the search() is asking the query to create scorer without
          having Weight object to pass stats
        */
        searcher.hack_without_weight(new NoTermQuery(new Term("first_name","jack")));

        /*
          aa... lets just create our own Primitive queries that dont use any of lucene's
          infrastructure (except the DocsEnum so we can actually iterate thru the term's
          documents)

          this set of examples illustrate how hackable lucene actually is,
          we have created a PrimitiveTerm query, a PrimitiveAndQuery and PrimitiveOrQuery

          so we can do things like:
          and("first_name":"jack", or("middle_name":"doe","last_name":"doe"))

          and it also uses lucene's bounded PriorityQueue implementation to store the topN
          documents

        */
        ScoreDoc[] scored = Primitive.search(reader,new PrimitiveTerm(new Term("first_name","jack")),5);
        for (ScoreDoc s : scored) {
            System.out.println("doc: " + s.doc + ", score: " + s.score);
        }

        scored = new PrimitiveTerm(new Term("first_name","jack")).search(reader,5);
        for (ScoreDoc s : scored)
            System.out.println("doc: " + s.doc + ", score: " + s.score);

        PrimitiveAndQuery and = new PrimitiveAndQuery();
        and.add(new PrimitiveTerm(new Term("first_name","jack")));

        PrimitiveOrQuery or = new PrimitiveOrQuery();
        or.add(new PrimitiveTerm(new Term("middle_name","doe")));
        or.add(new PrimitiveTerm(new Term("last_name","doe")));
        and.add(or);

        for (ScoreDoc s : and.search(reader,50))
            System.out.println("and doc: " + s.doc + ", score: " + s.score);

        reader.close();
    }

    private static class NoIndexSearcher extends IndexSearcher {
        public NoIndexSearcher(IndexReader r) {
            super(r);
        }

        void hack_with_weight(Query query) throws Exception {
            Weight weight = createNormalizedWeight(query);
            for (AtomicReaderContext ctx : readerContext.leaves()) {
                Scorer scorer = weight.scorer(ctx, ctx.reader().getLiveDocs());
                if (scorer == null)
                    continue;
                int doc;
                int docBase = ctx.docBase;
                while ((doc = scorer.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    System.out.println("scoring " + (docBase + doc));
                }
            }
        }

        void hack_without_weight(NoTermQuery query) throws Exception {
            for (AtomicReaderContext ctx : readerContext.leaves()) {
                Scorer scorer = query.one_no_weight_scorer_please(readerContext,ctx);
                if (scorer == null)
                    continue;
                int doc;
                int docBase = ctx.docBase;
                while ((doc = scorer.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    System.out.println("scoring " + (docBase + doc));
                }
            }
        }
    }

    private static class NoTermQuery extends Query {
        Term term;
        public NoTermQuery(Term t) {
            term = t;
        }
        @Override
        public String toString(String field) { return "no-term [" + term.toString() + "]"; }

        public Scorer one_no_weight_scorer_please(final IndexReaderContext top, AtomicReaderContext context) throws Exception {
            final TermContext termState = TermContext.build(top, term);
            final TermState state = termState.get(context.ord);
            if (state == null)
                return null;

            final TermsEnum termsEnum = context.reader().terms(term.field()).iterator(null);
            termsEnum.seekExact(term.bytes(), state);
            Bits acceptDocs = context.reader().getLiveDocs();
            DocsEnum docs = termsEnum.docs(acceptDocs, null);
            assert docs != null;
            return new NoTermScorer(null,docs);
        }

        @Override
        public Weight createWeight(IndexSearcher searcher) throws IOException {
            final IndexReaderContext context = searcher.getTopReaderContext();
            final TermContext termState;
            termState = TermContext.build(context, term);
            return new NoTermWeight(termState);
        }

        final class NoTermWeight extends Weight {
            TermContext termStates;
            public NoTermWeight(TermContext s) {
                termStates = s;
            }
            @Override
            public String toString() { return "weight(" + NoTermQuery.this + ")"; }

            @Override
            public Query getQuery() { return NoTermQuery.this; }

            @Override
            public float getValueForNormalization() { return 1.0f; }

            @Override
            public void normalize(float queryNorm, float topLevelBoost) { }

            @Override
            public Scorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
                assert termStates.topReaderContext == ReaderUtil.getTopLevelContext(context) : "The top-reader used to create Weight (" + termStates.topReaderContext + ") is not the same as the current reader's top-reader (" + ReaderUtil.getTopLevelContext(context);
                final TermState state = termStates.get(context.ord);
                if (state == null)
                    return null;

                final TermsEnum termsEnum = context.reader().terms(term.field()).iterator(null);
                termsEnum.seekExact(term.bytes(), state);
                DocsEnum docs = termsEnum.docs(acceptDocs, null);
                assert docs != null;
                return new NoTermScorer(this, docs);
            }

            @Override
            public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
                Scorer scorer = scorer(context, context.reader().getLiveDocs());
                return new ComplexExplanation(false, 0.0f, "no matching term");
            }
        }


        final class NoTermScorer extends Scorer {
            private final DocsEnum docsEnum;
            NoTermScorer(Weight weight, DocsEnum td) {
                super(weight);
                this.docsEnum = td;
            }

            @Override
            public int docID() {
                return docsEnum.docID();
            }

            @Override
            public int freq() throws IOException {
                return docsEnum.freq();
            }

            @Override
            public int nextDoc() throws IOException {
                return docsEnum.nextDoc();
            }

            @Override
            public float score() throws IOException {
                return 1.0f;
            }

            @Override
            public int advance(int target) throws IOException {
                return docsEnum.advance(target);
            }

            @Override
            public long cost() {
                return docsEnum.cost();
            }

            @Override
            public String toString() { return "scorer(" + weight + ")"; }
        }
    }

    private static class Primitive {
        public int docID() throws Exception {
            throw new Exception("not implemented");
        }
        public int nextDoc() throws Exception {
            throw new Exception("not implemented");
        }

        public float score() throws Exception {
            throw new Exception("not implemented");
        }

        public int advance(int target) throws Exception {
            throw new Exception("not implemented");
        }

        public long costAfterContextIsSet() {
            return Long.MAX_VALUE;
        }

        public void setContext(AtomicReaderContext context) throws Exception {
            throw new Exception("not implemented");
        }

        public Primitive duplicate() throws Exception {
            throw new Exception("not implemented");
        }

        public ScoreDoc[] search(IndexReader r, int n) throws Exception {
            return Primitive.search(r,this,n);
        }

        public static ScoreDoc[] search(IndexReader r, Primitive query, int n) throws Exception {
            IndexReaderContext readerContext = r.getContext();
            PrimitiveHitQueue hq = new PrimitiveHitQueue(n);
            assert n > 0;
            for (AtomicReaderContext ctx : readerContext.leaves()) {
                query.setContext(ctx);
                int doc;
                int docBase = ctx.docBase;
                while ((doc = query.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    float score = query.score();
                    if (hq.size() < n || hq.top().score < score) {
                        hq.insertWithOverflow(new ScoreDoc(doc + docBase,score));
                    }
                }
            }
            final ScoreDoc[] scoreDocs = new ScoreDoc[hq.size()];
            for (int i = hq.size() - 1; i >= 0; i--)
                scoreDocs[i] = hq.pop();
            return scoreDocs;
        }
    }

    private static class PrimitiveTerm extends Primitive {
        DocsEnum docsEnum;
        Term term;

        public PrimitiveTerm(Term term) throws Exception {
            this.term = term;
        }

        @Override
        public int docID() {
            return docsEnum != null ? docsEnum.docID() : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public void setContext(AtomicReaderContext context) throws Exception {
            docsEnum = null;
            Terms terms = context.reader().terms(term.field());
            if (terms != null) {
                TermsEnum termsEnum = terms.iterator(null);
                if (termsEnum.seekExact(term.bytes()) != false)
                    docsEnum = termsEnum.docs(context.reader().getLiveDocs(), null);
            }
        }

        @Override
        public int nextDoc() throws Exception {
            return docsEnum != null ? docsEnum.nextDoc() : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) throws Exception {
            return docsEnum != null ? docsEnum.advance(target) : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public float score() throws Exception {
            assert docID() != DocIdSetIterator.NO_MORE_DOCS;
            return 1.5f;
        }
        @Override
        public long costAfterContextIsSet() {
            return docsEnum != null ? docsEnum.cost() : 0;
        }
        @Override
        public Primitive duplicate() throws Exception{
            return new PrimitiveTerm(term);
        }
    }

    public static class PrimitiveAndQuery extends Primitive {
        // most of it is borrowed from ConjunctionScorer.java
        List<Primitive> queries = new ArrayList<Primitive>();
        Primitive lead;
        int doc = -1;

        public void add(Primitive q) throws Exception {
            queries.add(q.duplicate());
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public void setContext(AtomicReaderContext context) throws Exception {
            doc = -1;
            for (Primitive q : queries) {
                q.setContext(context);
            }
            Collections.sort(queries, new Comparator<Primitive>() {
                    public int compare(Primitive o1, Primitive o2) {
                        return Long.compare(o1.costAfterContextIsSet(),o2.costAfterContextIsSet());
                    }
                });
            lead = queries.get(0);
        }

        @Override
        public long costAfterContextIsSet() {
            return queries.size() == 0 ? 0 : lead.costAfterContextIsSet();
        }


        int next(int target) throws Exception {
            for(;;) {
            try_again: for(;;) {
                    for (int i = 1; i < queries.size(); i++) {
                        Primitive q = queries.get(i);
                        if (q.docID() < target) {
                            q.advance(target);
                            if (q.docID() > target) {
                                // we found a document that is beyond our current
                                // target, so just use that as the new target and try again
                                target = q.docID();
                                break try_again;
                            }
                        }
                    }
                    return target;
                }
                target = lead.advance(target);
            }
        }

        @Override
        public int nextDoc() throws Exception {
            if (queries.size() == 0)
                return DocIdSetIterator.NO_MORE_DOCS;
            return doc = next(lead.nextDoc());
        }

        @Override
        public int advance(int target) throws Exception {
            if (queries.size() == 0)
                return DocIdSetIterator.NO_MORE_DOCS;
            return doc = next(lead.advance(target));
        }

        @Override
        public float score() throws Exception {
            float s = 0f;
            for (Primitive q : queries) {
                assert q.docID() == doc;
                s += q.score();
            }
            return s;
        }

        @Override
        public Primitive duplicate() throws Exception {
            PrimitiveAndQuery a = new PrimitiveAndQuery();
            for (Primitive q : queries)
                a.add(q);
            return a;
        }
    }


    public static class PrimitiveOrQuery extends Primitive {
        // most of it is borrowed from DisjunctionScorer.java
        List<Primitive> queries = new ArrayList<Primitive>();
        int doc = -1;
        Primitive[] a_queries;
        int num_queries;
        public void add(Primitive q) throws Exception {
            queries.add(q.duplicate());
        }

        private void heapify() throws Exception {
            for (int i = (num_queries >>> 1) - 1; i >= 0; i--) {
                heapAdjust(i);
            }
        }

        private void heapAdjust(int root) throws Exception {
            Primitive query = a_queries[root];
            int doc = query.docID();
            int i = root;
            while (i <= (num_queries >>> 1) - 1) {
                int lchild = (i << 1) + 1;
                Primitive lquery = a_queries[lchild];
                int ldoc = lquery.docID();
                int rdoc = Integer.MAX_VALUE, rchild = (i << 1) + 2;
                Primitive rquery = null;
                if (rchild < num_queries) {
                    rquery = a_queries[rchild];
                    rdoc = rquery.docID();
                }
                if (ldoc < doc) {
                    if (rdoc < ldoc) {
                        a_queries[i] = rquery;
                        a_queries[rchild] = query;
                        i = rchild;
                    } else {
                        a_queries[i] = lquery;
                        a_queries[lchild] = query;
                        i = lchild;
                    }
                } else if (rdoc < doc) {
                    a_queries[i] = rquery;
                    a_queries[rchild] = query;
                    i = rchild;
                } else {
                    return;
                }
            }
        }

        private void heapRemoveRoot() throws Exception {
            if (num_queries == 1) {
                a_queries[0] = null;
                num_queries = 0;
            } else {
                a_queries[0] = a_queries[num_queries - 1];
                a_queries[num_queries - 1] = null;
                --num_queries;
                heapAdjust(0);
            }
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public void setContext(AtomicReaderContext context) throws Exception {
            doc = -1;
            for (Primitive q : queries) {
                q.setContext(context);
            }
            a_queries = queries.toArray(new Primitive[queries.size()]);
            num_queries = queries.size();
            heapify();
        }

        @Override
        public final int nextDoc() throws Exception {
            if (queries.size() == 0)
                return DocIdSetIterator.NO_MORE_DOCS;

            while(true) {
                if (a_queries[0].nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    heapAdjust(0);
                } else {
                    heapRemoveRoot();
                    if (num_queries == 0)
                        return doc = DocIdSetIterator.NO_MORE_DOCS;
                }
                int docID = a_queries[0].docID();
                if (docID != doc)
                    return doc = docID;
            }
        }

        @Override
        public final int advance(int target) throws Exception {
            if (queries.size() == 0)
                return DocIdSetIterator.NO_MORE_DOCS;

            while(true) {
                if (a_queries[0].advance(target) != DocIdSetIterator.NO_MORE_DOCS) {
                    heapAdjust(0);
                } else {
                    heapRemoveRoot();
                    if (num_queries == 0)
                        return doc = DocIdSetIterator.NO_MORE_DOCS;
                }
                int docID = a_queries[0].docID();
                if (docID >= target)
                    return doc = docID;
            }
        }

        @Override
        public long costAfterContextIsSet() {
            long c = 0;
            for (Primitive q : queries)
                c += q.costAfterContextIsSet();
            return c;
        }

        @Override
        public float score() throws Exception {
            float s = 0f;
            for (Primitive q : queries) {
                if (q.docID() == doc)
                    s += q.score();
            }
            return s;
        }

        @Override
        public Primitive duplicate() throws Exception {
            PrimitiveOrQuery a = new PrimitiveOrQuery();
            for (Primitive q : queries)
                a.add(q);
            return a;
        }
    }

    private static class PrimitiveHitQueue extends org.apache.lucene.util.PriorityQueue<ScoreDoc> {
        PrimitiveHitQueue(int size) {
            super(size, false);
        }
        @Override
        protected ScoreDoc getSentinelObject() {
            return new ScoreDoc(Integer.MAX_VALUE, Float.NEGATIVE_INFINITY);
        }

        @Override
        protected final boolean lessThan(ScoreDoc hitA, ScoreDoc hitB) {
            if (hitA.score == hitB.score)
                return hitA.doc > hitB.doc;
            else
                return hitA.score < hitB.score;
        }
    }
}

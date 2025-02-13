package nlp;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.*;

import java.io.IOException;
import java.nio.file.Paths;

public class LuceneInvertedIndex implements AutoCloseable {
    private static final String INDEX_DIR = "indexDir"; // Directory to store index

    private final Directory directory;
    private final IndexWriter writer;
    private final IndexReader reader;

    public LuceneInvertedIndex() throws IOException {
        final var config = new IndexWriterConfig(new StandardAnalyzer());
        this.directory = FSDirectory.open(Paths.get(INDEX_DIR));

        try {
            this.writer = new IndexWriter(directory, config);
        } catch (IOException e) {
            this.directory.close();
            throw new RuntimeException(e);
        }
        try {
            this.reader = DirectoryReader.open(this.writer);
        } catch (IOException e) {
            this.directory.close();
            this.writer.close();
            throw new RuntimeException(e);
        }
    }

    public void index(String docId, String content) throws IOException {
        // Create a new document
        final var doc = new Document();
        doc.add(new StringField("docId", docId, Field.Store.YES));
        doc.add(new TextField("content", content, Field.Store.YES));

        // Add document to index
        writer.addDocument(doc);
    }

    public void search(String searchTerm) throws IOException {
        final var query = new TermQuery(new Term("content", searchTerm));
        final var searcher = new IndexSearcher(this.reader);
        final var results = searcher.search(query, 10);

        System.out.println("Documents containing '" + searchTerm + "':");
        for (final var hit : results.scoreDocs) {
            final var doc = searcher.doc(hit.doc);
            System.out.println("- " + doc.get("docId"));
        }
    }

    @Override
    public void close() throws Exception {
        this.directory.close();
    }
}

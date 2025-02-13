package nlp;

import opennlp.tools.tokenize.SimpleTokenizer;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class TokenizerExample {

    public static void main(String[] args) {
        // Obtain the singleton instance of SimpleTokenizer
        SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;

        final var pathEmails = Path.of("emails");
        final var dbPath = "rocksdb_data";

        try (final var store = new LuceneInvertedIndex();
             final var emails = Files.walk(pathEmails)) {

            emails.filter(p -> !p.toFile().getName().startsWith("."))
                    .filter(p -> p.toFile().isFile())
                    .forEach(p -> {
                        try {
                            final var pathEmail = pathEmails.relativize(p);
                            final var eid = String.valueOf(pathEmail);
                            final var sentence = Files.readString(p);

                            System.out.println("eid: " + eid + " tkns: " + sentence.length());
                            store.index(eid, sentence);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });

            store.search("yes");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

//        try (final var store = new CassandraExample();
//             final var emails = Files.walk(pathEmails)) {
//
//            store.createSchema();
//            emails.filter(p -> !p.toFile().getName().startsWith("."))
//                    .filter(p -> p.toFile().isFile())
//                    .forEach(p -> {
//                        try {
//                            final var pathEmail = pathEmails.relativize(p);
//                            final var eid = String.valueOf(pathEmail);
//                            final var sentence = Files.readString(p);
//                            // Tokenize the input sentence into an array of tokens
//                            final var tkns = tokenizer.tokenize(sentence);
//
//                            System.out.println("eid: " + eid + " tkns: " + tkns.length);
//                            store.insert(eid, tkns);
//                        } catch (IOException e) {
//                            throw new RuntimeException(e);
//                        }
//                    });
//
//            store.query();
//            store.query2("yes");
//            store.query2("no");
//            store.query2("reject");
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }

    }
}

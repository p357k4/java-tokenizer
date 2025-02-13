package nlp;

import org.rocksdb.*;

public class RocksDBKeyValueStore implements AutoCloseable {
    static {
        // Load the native RocksDB library.
        RocksDB.loadLibrary();
    }

    private final RocksDB db;

    public RocksDBKeyValueStore(String dbPath) throws RocksDBException {
        Options options = new Options().setCreateIfMissing(true).setMergeOperator(new StringAppendOperator());
        this.db = RocksDB.open(options, dbPath);
    }

    public void append(String key, String value) throws RocksDBException {
        db.merge(key.getBytes(), value.getBytes());
    }

    public String get(String key) throws RocksDBException {
        return new String(db.get(key.getBytes()));
    }

    @Override
    public void close() {
        db.close();
    }
}

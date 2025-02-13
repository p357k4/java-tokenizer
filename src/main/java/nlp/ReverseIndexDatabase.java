package nlp;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap; // Consider for concurrency

public class ReverseIndexDatabase {

    private static final String DATA_FILE_NAME = "reverse_index_data.dat";
    private static final String INDEX_FILE_NAME = "reverse_index_index.idx";

    private Path dataFilePath;
    private Path indexFilePath;
    private FileChannel dataFileChannel;
    private MappedByteBuffer dataBuffer;
    private Map<String, Long> keywordIndex; // In-memory index

    public ReverseIndexDatabase(String baseDir) throws IOException {
        Path baseDirPath = Path.of(baseDir);
        Files.createDirectories(baseDirPath); // Ensure directory exists

        this.dataFilePath = baseDirPath.resolve(DATA_FILE_NAME);
        this.indexFilePath = baseDirPath.resolve(INDEX_FILE_NAME);

        boolean dataFileExists = Files.exists(this.dataFilePath);

        dataFileChannel = FileChannel.open(dataFilePath,
                StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE); // Read/Write/Create
        dataBuffer = dataFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, dataFileChannel.size()); // Initially map existing size, will remap if needed


        loadIndex(); // Load or create index

        if (!dataFileExists) {
            System.out.println("Data file created: " + dataFilePath);
        }
        System.out.println("Database initialized.");
    }

    @SuppressWarnings("unchecked")
    private void loadIndex() throws IOException {
        if (Files.exists(indexFilePath)) {
            try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(indexFilePath))) {
                keywordIndex = (Map<String, Long>) ois.readObject();
                System.out.println("Index loaded from file: " + indexFilePath + ", Size: " + keywordIndex.size());
            } catch (ClassNotFoundException e) {
                System.err.println("Error loading index, class not found: " + e.getMessage());
                keywordIndex = new ConcurrentHashMap<>(); // Fallback to empty index
            }
        } else {
            System.out.println("Index file not found, creating new index.");
            keywordIndex = new ConcurrentHashMap<>();
            // Option to rebuild index from data file on first run if needed for robustness (but less crucial in this design)
        }
    }

    private void persistIndex() throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(indexFilePath))) {
            oos.writeObject(keywordIndex);
            System.out.println("Index persisted to file: " + indexFilePath + ", Size: " + keywordIndex.size());
        }
    }


    public void append(String keyword, long fileId) throws IOException {
        synchronized (dataFileChannel) { // Basic file-level locking for append operations

            Long existingOffset = keywordIndex.get(keyword);
            List<Long> fileIdList = new ArrayList<>();

            if (existingOffset != null) {
                fileIdList = readFileIdListFromDataFile(existingOffset); // Read existing list
                if (fileIdList.contains(fileId)) {
                    return; // Already added, avoid duplicates (optional behavior)
                }
            }
            fileIdList.add(fileId); // Append the new fileId

            byte[] keywordBytes = keyword.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] entryBytes = serializeEntry(keywordBytes, fileIdList);

            long currentFileSize = dataFileChannel.size(); // Get current file size
            long newFileSize = currentFileSize + entryBytes.length;

            if (newFileSize > dataBuffer.capacity()) { // Check if need to remap (if file grows beyond initial map)
                dataBuffer = dataFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, newFileSize); // Remap to larger size
            }

            dataBuffer.position((int) currentFileSize); // Position at the end for append
            dataBuffer.put(entryBytes); // Append new entry

            keywordIndex.put(keyword, currentFileSize); // Update index with the offset of the NEWLY appended entry

        }
    }


    public List<Long> search(String keyword) {
        Long offset = keywordIndex.get(keyword);
        if (offset == null) {
            return Collections.emptyList();
        }
        return readFileIdListFromDataFile(offset);
    }


    private List<Long> readFileIdListFromDataFile(long offset) {
        dataBuffer.position((int) offset); // Position at the start of the entry
        int keywordLength = dataBuffer.getInt();
        byte[] keywordBytes = new byte[keywordLength];
        dataBuffer.get(keywordBytes);
        String keyword = new String(keywordBytes, java.nio.charset.StandardCharsets.UTF_8); // Read keyword (verify if needed)

        int fileIdListLength = dataBuffer.getInt();
        List<Long> fileIds = new ArrayList<>(fileIdListLength);
        for (int i = 0; i < fileIdListLength; i++) {
            fileIds.add(dataBuffer.getLong());
        }
        return fileIds;
    }


    private byte[] serializeEntry(byte[] keywordBytes, List<Long> fileIds) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);

        dos.writeInt(keywordBytes.length);
        dos.write(keywordBytes);
        dos.writeInt(fileIds.size());
        for (long fileId : fileIds) {
            dos.writeLong(fileId);
        }
        return bos.toByteArray();
    }


    public void close() throws IOException {
        persistIndex();
        if (dataBuffer != null) {
           // No explicit unmapping needed in standard Java, GC will handle when MappedByteBuffer is no longer referenced,
           // but for cleaner shutdown in some scenarios (like explicit unmapping in other languages), you might consider more advanced NIO techniques if necessary.
        }
        if (dataFileChannel != null) {
            dataFileChannel.close();
        }
        System.out.println("Database closed.");
    }


    public static void main(String[] args) throws IOException {
        String dbDir = "reverse_index_db"; // Directory to store database files
        ReverseIndexDatabase db = new ReverseIndexDatabase(dbDir);

        db.append("java", 1001);
        db.append("java", 1002);
        db.append("search", 1003);
        db.append("api", 1001);
        db.append("api", 1003);
        db.append("api", 1004);
        db.append("java", 1001); // Append duplicate - should be handled (optional in append method)


        System.out.println("Search for 'java': " + db.search("java"));
        System.out.println("Search for 'search': " + db.search("search"));
        System.out.println("Search for 'api': " + db.search("api"));
        System.out.println("Search for 'nonexistent': " + db.search("nonexistent"));


        db.close();
    }
}
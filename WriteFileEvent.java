package unimelb.bitbox;

// Records related information to write a file.
// A FileWriterThread object accepts this event to issue
// FILE_BYTES_REQUESTs and write the file.

public class WriteFileEvent {

    //Records the file content source peer info.
    public String host;
    public int port;

    //FILE_CREATE_REQUEST or FILE_MODIFY_REQUEST
    public String request; 
    public String pathName;

    //fileDescriptor
    public String md5;
    public long lastModified;
    public long fileSize;

    public WriteFileEvent(String host, int port, String request, String pathName,
                          String md5, long lastModified, long fileSize) {
        this.host = host;
        this.port = port;
        this.request = request;
        this.pathName = pathName;
        this.md5 = md5;
        this.lastModified = lastModified;
        this.fileSize = fileSize;
    }
}

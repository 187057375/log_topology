
package utils;


public interface RedisListener {


    //void init(Tailer tailer);

    void fileNotFound();


    void fileRotated();


    void handle(String line);


    void handle(Exception ex);

}

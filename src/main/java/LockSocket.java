package main.java;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class LockSocket {

    private Socket socket;
    private boolean locked = false;

    public LockSocket(Socket socket) {
        this.socket = socket;
    }

    public Socket getSocket() {
        return socket;
    }

    public void lock()
    {
        while (isLocked());
        locked = true;
    }

    public void unLock()
    {
        locked = false;
    }

    public synchronized boolean isLocked()
    {
        return locked;
    }

    public OutputStream getOutputStream() throws IOException {
        return socket.getOutputStream();
    }

    public InputStream getInputStream() throws IOException {
        return socket.getInputStream();
    }

    public void close() throws IOException {
        socket.close();
    }

}

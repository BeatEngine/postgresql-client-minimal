package main.root;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ongres.scram.client.ScramClient;
import com.ongres.scram.client.ScramSession;
import com.ongres.scram.common.stringprep.StringPreparations;

public class PSQLminimal {

    public static Map<String,String> getParametersForStartup(String user, String database)
    {
        final Map<String,String> map = new HashMap<>();
        map.put("user", user);
        map.put("database", database);
        map.put("client_encoding", "UTF8");
        map.put("DateStyle", "ISO, MDY");
        return map;
    }



    public static Socket getSocket(final String host, int port) throws IOException {

        Socket socketUnsafe = getSocketUnsafe(host, port);
        OutputStream outputStream = socketUnsafe.getOutputStream();
        write(outputStream, intToBytes(8), false);
        write(outputStream, shortToBytes((short) 1234), false);
        write(outputStream, shortToBytes((short) 5679), true);

        int beresp = readN(socketUnsafe.getInputStream(), 1)[0];
        switch (beresp) {
            case 'E':
                // Server doesn't even know about the SSL handshake protocol
                Logger.getLogger(PSQLminimal.class.getName()).log(Level.INFO, "TLS unknown using raw socket");
                return socketUnsafe;
            case 'N':

                // Server does not support ssl
                Logger.getLogger(PSQLminimal.class.getName()).log(Level.INFO, "TLS not supported using raw socket");
                return socketUnsafe;
            case 'S':

                // Server supports ssl
                SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
                SSLSocket newConnection;
                try {
                    newConnection = (SSLSocket) factory.createSocket(socketUnsafe,
                            host, port, true);
                    // We must invoke manually, otherwise the exceptions are hidden
                    newConnection.setUseClientMode(true);
                    newConnection.startHandshake();
                } catch (IOException ex) {
                    throw new RuntimeException("SSL error: "+ ex.getMessage());
                }

                /*SSLSocketFactory sslSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();

                // Create an SSLSocket and connect it to the host and port
                SSLSocket sslSocket = (SSLSocket) sslSocketFactory.createSocket(host, port);

                // Perform the SSL handshake
                sslSocket.startHandshake();*/

                return newConnection;

            default:
                throw new RuntimeException("An error occurred while setting up the SSL connection.");
        }

    }

    public static Socket getSocketUnsafe(final String host, int port) throws IOException {

        Socket socket = new Socket(host, port);

        return socket;
    }

    protected static byte[] read(final InputStream inputStream) throws IOException {
        final ByteArrayOutputStream bao = new ByteArrayOutputStream();

        do
        {
            int read = inputStream.read();
            if(read == -1)
            {
                break;
            }
            bao.write(read);
        }while (true);
        return bao.toByteArray();
    }

    protected static byte[] read(final InputStream inputStream, int timeout) throws IOException {
        final ByteArrayOutputStream bao = new ByteArrayOutputStream();
        Thread thread = new Thread(() -> {
            do {
                int read = 0;
                try {
                    read = inputStream.read();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (read == -1) {
                    break;
                }
                bao.write(read);
            } while (true);
        });
        thread.start();
        try {
            thread.join(timeout);
        } catch (InterruptedException e) {
        }
        return bao.toByteArray();
    }

    protected static byte[] readUntil(final InputStream inputStream, int eof) throws IOException {
        final ByteArrayOutputStream bao = new ByteArrayOutputStream();

        do
        {
            int read = inputStream.read();
            if(read == -1 || read == eof)
            {
                break;
            }
            bao.write(read);
        }while (true);
        return bao.toByteArray();
    }

    protected static byte[] readN(final InputStream inputStream, int maxLength) throws IOException {
        final ByteArrayOutputStream bao = new ByteArrayOutputStream();
        int l = 0;
        do
        {
            int read = inputStream.read();
            if(read == -1)
            {
                break;
            }
            bao.write(read);
            l++;
            if(l >= maxLength)
            {
                break;
            }
        }while (true);
        return bao.toByteArray();
    }

    protected static byte[] readN(final InputStream inputStream, int maxLength, int timeout) throws IOException {
        final ByteArrayOutputStream bao = new ByteArrayOutputStream();
        Thread thread = new Thread(() -> {
            int l = 0;
            do
            {
                int read = 0;
                try {
                    read = inputStream.read();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if(read == -1)
                {
                    break;
                }
                bao.write(read);
                l++;
                if(l >= maxLength)
                {
                    break;
                }
            }while (true);

        });
        thread.start();
        try {
            thread.join(timeout);
        } catch (InterruptedException e) {
        }
        return bao.toByteArray();
    }

    protected static void write(final OutputStream os, final byte[] data) throws IOException {
        write(os, data, true);
    }

    protected static void write(final OutputStream os, final byte[] data, boolean flush) throws IOException {
        os.write(data);
        if(flush) {
            os.flush();
        }
    }

    protected static byte[] intToBytes(int i) {
        return new byte[]{
                (byte) (i >>> 24),
                (byte) (i >>> 16),
                (byte) (i >>> 8),
                (byte) i
        };
    }

    protected static int bytesToInt(byte[] b) {
        return (b[0]) << 24 |
                (b[1] & 0xFF) << 16 |
                (b[2] & 0xFF) << 8 |
                (b[3] & 0xFF);
    }

    protected static byte[] shortToBytes(short i) {
        return new byte[]{
                (byte) (i >>> 8),
                (byte) i
        };
    }

    protected static short bytesToShort(byte[] b) {
        return (short)((b[0] & 0xFF) << 8 |
                        (b[1] & 0xFF));
    }

    public static byte[] encodeBase64(final String input)
    {
        return input.getBytes(StandardCharsets.UTF_8);
        //return Base64.getEncoder().encode(input.getBytes(StandardCharsets.UTF_8));
    }

    public static void sendStartupPacket(final OutputStream pgStream, Map<String,String> params) throws IOException {

        // Precalculate message length and encode params.
        int length = 4 + 4;
        byte[][] encodedParams = new byte[params.size() * 2][];
        int i = 0;
        for (Map.Entry<String, String> e: params.entrySet()) {
            byte[] bytes = encodeBase64(e.getKey());
            encodedParams[i * 2] = bytes;
            bytes = encodeBase64(e.getValue());
            encodedParams[i * 2 + 1] = bytes;
            length += encodedParams[i * 2].length + 1 + encodedParams[i * 2 + 1].length + 1;
            i++;
        }

        length += 1; // Terminating \0

        // Send the startup message.
        write(pgStream, intToBytes(length), false);
        write(pgStream, shortToBytes((short) 3), false);// protocol major
        write(pgStream, shortToBytes((short) 0), false); // protocol minor
        for (byte[] encodedParam : encodedParams) {
            write(pgStream, encodedParam, false);
            write(pgStream, new byte[]{0}, false);
        }
        write(pgStream, new byte[]{0}, true);
    }

    /**
     * Encodes user/password/salt information in the following way: MD5(MD5(password + user) + salt).
     *
     * @param user The connecting user.
     * @param password The connecting user's password.
     * @param salt A four-salt sent by the server.
     * @return A 35-byte array, comprising the string "md5" and an MD5 digest.
     */
    public static byte[] encode(byte[] user, byte[] password, byte[] salt) {
        try {
            final MessageDigest md = MessageDigest.getInstance("MD5");

            md.update(password);
            md.update(user);
            byte[] digest = md.digest();

            final byte[] hexDigest = new byte[35];

            bytesToHex(digest, hexDigest, 0);
            md.update(hexDigest, 0, 32);
            md.update(salt);
            digest = md.digest();

            bytesToHex(digest, hexDigest, 3);
            hexDigest[0] = (byte) 'm';
            hexDigest[1] = (byte) 'd';
            hexDigest[2] = (byte) '5';

            return hexDigest;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Unable to encode password with MD5", e);
        }
    }
    private static final byte[] HEX_BYTES = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    private static void bytesToHex(byte[] bytes, byte[] hex, int offset) {
        int pos = offset;
        for (int i = 0; i < 16; i++) {
            //bit twiddling converts to int, so just do it once here for both operations
            final int c = bytes[i] & 0xFF;
            hex[pos++] = HEX_BYTES[c >> 4];
            hex[pos++] = HEX_BYTES[c & 0xF];
        }
    }




    public static void doAuthentication(final InputStream pgStream, final OutputStream pgStreamOut, String host, String user, final String password) throws IOException, SQLException {
        // Now get the response from the backend, either an error message
        // or an authentication request

        try {
            authloop: while (true) {
                int beresp = readN(pgStream, 1)[0];

                switch (beresp) {
                    case 'E':
                        // An error occurred
                        byte[] read = read(pgStream);
                        if(read.length > 19) {
                            String e = new String(read, StandardCharsets.UTF_8).substring(19, read.length-2);
                            throw new RuntimeException("Connection startup failed: " + e);
                        }
                        throw new RuntimeException("Connection startup failed");
                    case 'R':
                        // Authentication request.
                        // Get the message length
                        int msgLen = bytesToInt(readN(pgStream, Integer.BYTES));

                        // Get the type of request
                        int areq = bytesToInt(readN(pgStream, Integer.BYTES));

                        // Process the request.
                        switch (areq) {
                            case 5: { //MD5
                                byte[] md5Salt = readN(pgStream, 4);

                                byte[] digest = encode(user.getBytes(StandardCharsets.UTF_8),
                                                password.getBytes(StandardCharsets.UTF_8), md5Salt);


                                write(pgStreamOut, new byte[]{'p'}, false);
                                write(pgStreamOut, intToBytes(4+ digest.length+1), false);
                                write(pgStreamOut, digest, false);
                                write(pgStreamOut, new byte[]{0}, true);
                                break;
                            }

                            case 3: { //Password

                                write(pgStreamOut, new byte[]{'p'});
                                write(pgStreamOut, intToBytes(4+password.length()+1));
                                write(pgStreamOut, password.getBytes(StandardCharsets.UTF_8));
                                write(pgStreamOut, new byte[]{0}, true);
                                break;
                            }

                            case 0:
                                /* Cleanup after successful authentication */
                                break authloop; // We're done.

                            case 10: { // SASL
                                SCRAMhelper.setPassword(password);
                                SCRAMhelper.processServerMechanismsAndInit(pgStreamOut, pgStream);
                                SCRAMhelper.sendScramClientFirstMessage();

                                break;
                            }

                            case 11: // SASL Continue
                                SCRAMhelper.processServerFirstMessage(msgLen - 4 - 4);
                                break;

                            case 12: // SASL Final
                                SCRAMhelper.verifyServerSignature(msgLen - 4 - 4);
                                SCRAMhelper.close();
                                return;

                            default:
                                throw new RuntimeException(
                                        "The authentication type " + areq + " is not supported. ");
                        }

                        break;

                    case 0:
                    {
                        return;
                    }

                    default:
                        throw new RuntimeException("Protocol error. Session setup failed.");
                }
            }
        } finally {

        }
        /*byte[] bytes;
        int c = 0;
        do {
            bytes = readUntil(pgStream, 0);
            if(bytes.length == 0)
            {
                c++;
            }
            else
            {
                System.out.println(new String(bytes, StandardCharsets.UTF_8));
                c = 0;
            }
        }while (bytes.length == 0 && c < 9);*/



    }

    public static String getIp(String host, int port) throws IOException {
        Socket socket = getSocket(host, port);
        final String s = socket.getInetAddress().getHostAddress();
        socket.close();
        return s;
    }

    public static List<List<Object>> sendSimpleQuery(final InputStream inputStream, final OutputStream out, final String nativeSql, final List<String> columnLabels) throws IOException {

        byte[] encoded = nativeSql.getBytes(StandardCharsets.UTF_8);
        write(out, new byte[]{'Q'}, false);
        write(out, intToBytes(encoded.length + 4 + 1), false);
        write(out, encoded, false);
        write(out, new byte[]{0}, true);
        return processResults(inputStream, columnLabels);
    }

    private static Object castType(final int type, final byte[] data)
    {
        if(type == 0)
        {
            return new String(data, StandardCharsets.UTF_8);
        }
        else
        {
            return data;
        }
    }

    private static List<List<Object>> processResults(final InputStream inputStream, final List<String> labelsOut) throws IOException {
        List<List<Object>> resutSet = new ArrayList<>();
        List<Integer> castFields = new ArrayList<>();
        int c;
        String error = null;
        boolean endQuery = false;
        boolean lastS = false;

        // At the end of a command execution we have the CommandComplete
        // message to tell us we're done, but with a describeOnly command
        // we have no real flag to let us know we're done. We've got to
        // look for the next RowDescription or NoData message and return
        // from there.

        while (!endQuery) {
            byte[] r = readN(inputStream, 1);
            if(r.length == 0)
            {
                throw new RuntimeException("Timeout");
            }
            c = r[0];
            if(lastS && c != 'S' && c != 'E' && c != 'T' && c != 'D')
            {
                c = 0;
            }
            switch (c) {

                case 5:
                case 0:
                    break;

                case 'A': // Asynchronous Notify
                    int len = bytesToInt(readN(inputStream, Integer.BYTES));
                    int pid = bytesToInt(readN(inputStream, Integer.BYTES));
                    String msg = new String(readUntil(inputStream, 0));
                    String param = new String(readUntil(inputStream, 0));
                    lastS = false;
                    break;

                case '1': // Parse Complete (response to Parse)
                case '2': // Bind Complete (response to Bind)
                case '3': // Close Complete (response to Close)
                    len = bytesToInt(readN(inputStream, Integer.BYTES));
                    break;

                case 't': { // ParameterDescription
                    int length = bytesToInt(readN(inputStream, Integer.BYTES));

                    final byte[] bytes = readN(inputStream, length, 100);

                    final String meta = new String(bytes, StandardCharsets.UTF_8);
                    lastS = false;
                    break;
                }

                case 's': // Portal Suspended (end of Execute)
                case 'n': // No Data (response to Describe)
                    int applied = bytesToInt(readN(inputStream, Integer.BYTES));
                    break;


                case 'C': { // Command Status (end of Execute)
                    // Handle status.
                    int msgSz = bytesToInt(readN(inputStream, Integer.BYTES));
                    byte[] bytes = readN(inputStream, msgSz);
                    String s = new String(bytes, StandardCharsets.UTF_8);
                    if(s.startsWith("INSERT") || s.startsWith("DELETE") || s.startsWith("UPDATE"))
                    {
                        String count = s.substring(7);
                        if(count.contains(" "))
                        {
                            count = count.split(" ")[1];
                        }

                        count = count.substring(0, count.length()-1);
                        if(count.length() > 0)
                        {
                            ArrayList<Object> res = new ArrayList<>();
                            res.add(count.replace("Z","").trim());
                            resutSet.add(res);
                        }
                    }
                    lastS = false;
                    break;
                }

                case 'D': // Data Transfer (ongoing Execute response)
                    int msgSz = bytesToInt(readN(inputStream, Integer.BYTES));
                    short rows  = bytesToShort(readN(inputStream, Short.BYTES));
                    int dataToReadSize = msgSz - 4 - 2 - 4 * rows;

                    final List<Object> row = new ArrayList<>();
                    int f = 0;
                    for (int i = 0; i < rows; ++i) {

                        int size = bytesToInt(readN(inputStream, Integer.BYTES));
                        if (size != -1) {
                            byte[] field = readN(inputStream, size);
                            row.add(castType(castFields.get(f),field));
                        }
                        else
                        {
                            row.add(null);
                        }
                    }
                    resutSet.add(row);
                    lastS = false;
                    break;

                case 'E':
                    // Error Response (response to pretty much everything; backend then skips until Sync)
                    len = bytesToInt(readN(inputStream, Integer.BYTES));
                    error = new String(readN(inputStream, len), StandardCharsets.ISO_8859_1);
                    //byte[] read2 = read(inputStream, 50);
                    Logger.getLogger(PSQLminimal.class.getName()).log(Level.SEVERE, error.substring(1));
                    lastS = false;
                    break;

                case 'I': { // Empty Query (end of Execute)
                    //read(inputStream, 10);
                    return resutSet;
                }

                case 'N': // Notice Response
                    msgSz = bytesToInt(readN(inputStream, Integer.BYTES));
                    byte[] read1 = readN(inputStream,msgSz-4);
                    break;

                case 'S': // Parameter Status
                    msgSz = bytesToInt(readN(inputStream, Integer.BYTES));
                    byte[] readName = readUntil(inputStream, 0);
                    byte[] readValue = readUntil(inputStream, 0);
                    lastS = true;
                    break;

                case 'T': // Row Description (response to Describe)
                    bytesToInt(readN(inputStream, Integer.BYTES));
                    short fieldCount  = bytesToShort(readN(inputStream, Short.BYTES));

                    final List<String> labels = new ArrayList<>();

                    for (int i = 0; i < fieldCount; i++) {
                        String columnLabel = new String(readUntil(inputStream, 0), StandardCharsets.UTF_8);
                        int tableOid = bytesToInt(readN(inputStream, Integer.BYTES));
                        short positionInTable = bytesToShort(readN(inputStream, Short.BYTES));
                        int typeOid = bytesToInt(readN(inputStream, Integer.BYTES));
                        int typeLength = bytesToShort(readN(inputStream, Short.BYTES));
                        int typeModifier = bytesToInt(readN(inputStream, Integer.BYTES));
                        int formatType = bytesToShort(readN(inputStream, Short.BYTES));
                        castFields.add(formatType);
                        labels.add(columnLabel);
                    }
                    labelsOut.addAll(labels);
                    lastS = false;
                    break;

                case 'Z': // Ready For Query (eventual response to Sync)
                    bytesToInt(readN(inputStream, Integer.BYTES));
                    lastS = false;
                    break;

                case 'G': // CopyInResponse
                    throw new RuntimeException("Bulk transport (copy) not implemented!");
                case 'H': // CopyOutResponse
                    throw new RuntimeException("Bulk transport (copy) not implemented!");
                case 'c': // CopyDone
                    throw new RuntimeException("Bulk transport (copy) not implemented!");
                case 'd': // CopyData
                    throw new RuntimeException("Bulk transport (copy) not implemented!");

                default:
                    byte[] reade = read(inputStream,100);
                    throw new IOException("Unexpected packet type: " + c);
            }

        }
        return resutSet;
    }

}

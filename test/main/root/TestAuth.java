package main.root;

import main.root.PSQLminimal;
import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.net.Socket;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestAuth {

    private final String cluster = "abz-dft-1234";
    private final String ip = cluster + ".7tc.??.cloud";

    private final int port = 26257;
    private final String password = "???????";

    @Test
    public void testAuth() throws IOException, SQLException {
        final Socket socket = PSQLminimal.getSocket(ip, port);

        Map<String, String> params = PSQLminimal.getParametersForStartup("app", cluster+".defaultdb");

        PSQLminimal.sendStartupPacket(socket.getOutputStream(), params);

        PSQLminimal.doAuthentication(socket.getInputStream(), socket.getOutputStream(), ip, "app", password );
        final List<String> labels = new ArrayList<>();
        List<List<Object>> lists = PSQLminimal.sendSimpleQuery(socket.getInputStream(), socket.getOutputStream(), "SELECT * FROM pg_user;", labels);
        Assert.assertTrue(lists.size() > 0);
    }


    @Test
    public void testDDL() throws IOException, SQLException {
        final Socket socket = PSQLminimal.getSocket(ip, port);

        Map<String, String> params = PSQLminimal.getParametersForStartup("app", cluster+".defaultdb");

        PSQLminimal.sendStartupPacket(socket.getOutputStream(), params);

        PSQLminimal.doAuthentication(socket.getInputStream(), socket.getOutputStream(), ip, "app", password );
        final List<String> labels = new ArrayList<>();
        List<List<Object>> lists;// = PSQLminimal.sendSimpleQuery(socket.getInputStream(), socket.getOutputStream(), "DROP TABLE test123test456test;", labels);
        try {
            lists = PSQLminimal.sendSimpleQuery(socket.getInputStream(), socket.getOutputStream(), "CREATE TABLE test123test456test(id int, data VARCHAR(10));", labels);
        }
        catch (final Exception e){
            PSQLminimal.sendSimpleQuery(socket.getInputStream(), socket.getOutputStream(), "DROP TABLE test123test456test;", labels);
            throw new RuntimeException(e);
        }
        Assert.assertTrue(lists.size() == 0);
        lists = PSQLminimal.sendSimpleQuery(socket.getInputStream(), socket.getOutputStream(), "DROP TABLE test123test456test;", labels);
        Assert.assertTrue(lists.size() == 0);
    }

    @Test
    public void testInsertDeleteUpdate() throws IOException, SQLException {
        //final String host = PSQLminimal.getIp(ip, 26257);
        final Socket socket = PSQLminimal.getSocket(ip, port);

        Map<String, String> params = PSQLminimal.getParametersForStartup("app", cluster+".defaultdb");

        PSQLminimal.sendStartupPacket(socket.getOutputStream(), params);

        PSQLminimal.doAuthentication(socket.getInputStream(), socket.getOutputStream(), ip, "app", password );
        final List<String> labels = new ArrayList<>();
        List<List<Object>> lists;
        try {
            lists = PSQLminimal.sendSimpleQuery(socket.getInputStream(), socket.getOutputStream(), "DROP TABLE test123test456test;", labels);
        }catch (final Exception e)
        {
            final int i = 0;
        }
        lists = PSQLminimal.sendSimpleQuery(socket.getInputStream(), socket.getOutputStream(), "CREATE TABLE test123test456test(id int, data VARCHAR(10));", labels);

        lists = PSQLminimal.sendSimpleQuery(socket.getInputStream(), socket.getOutputStream(), "INSERT INTO test123test456test(id, data) VALUES (1, 'abc');", labels);
        Assert.assertTrue(lists.size() == 1);
        Assert.assertTrue("1".equals(lists.get(0).get(0)));
        lists = PSQLminimal.sendSimpleQuery(socket.getInputStream(), socket.getOutputStream(), "INSERT INTO test123test456test(id, data) VALUES (2, 'abc');", labels);
        lists = PSQLminimal.sendSimpleQuery(socket.getInputStream(), socket.getOutputStream(), "INSERT INTO test123test456test(id, data) VALUES (3, 'abc');", labels);

        lists = PSQLminimal.sendSimpleQuery(socket.getInputStream(), socket.getOutputStream(), "UPDATE test123test456test SET data = 'def' WHERE id = 3;", labels);
        Assert.assertTrue(lists.size() == 1);
        Assert.assertTrue("1".equals(lists.get(0).get(0)));

        lists = PSQLminimal.sendSimpleQuery(socket.getInputStream(), socket.getOutputStream(), "DELETE FROM test123test456test;", labels);
        Assert.assertTrue(lists.size() == 1);
        Assert.assertTrue("3".equals(lists.get(0).get(0)));

        PSQLminimal.sendSimpleQuery(socket.getInputStream(), socket.getOutputStream(), "DROP TABLE test123test456test;", labels);
    }

}
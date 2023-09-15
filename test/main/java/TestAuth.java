package main.java;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestAuth {

    private final String cluster = "tailed-werebat-7793";
    private final String ip = cluster + ".7tc.cockroachlabs.cloud";

    private final int port = 26257;
    private final String password = "6f6FR67fs7fs7plK";

    @Test
    public void testAuth() throws IOException, SQLException {
        final LockSocket socket = (LockSocket)PSQLminimal.getSocket(ip, port);

        Map<String, String> params = PSQLminimal.getParametersForStartup("app", cluster+".defaultdb");

        PSQLminimal.sendStartupPacket(socket.getOutputStream(), params);

        PSQLminimal.doAuthentication(socket, ip, "app", password );
        final List<String> labels = new ArrayList<>();
        List<List<Object>> lists = PSQLminimal.sendSimpleQuery(socket, "SELECT * FROM pg_user;", labels);
        Assert.assertTrue(lists.size() > 0);
        socket.close();
    }


    @Test
    public void testDDL() throws IOException, SQLException {
        final LockSocket socket = (LockSocket)PSQLminimal.getSocket(ip, port);

        Map<String, String> params = PSQLminimal.getParametersForStartup("app", cluster+".defaultdb");

        PSQLminimal.sendStartupPacket(socket.getOutputStream(), params);

        PSQLminimal.doAuthentication(socket, ip, "app", password );
        final List<String> labels = new ArrayList<>();
        List<List<Object>> lists;// = PSQLminimal.sendSimpleQuery(socket.getInputStream(), socket.getOutputStream(), "DROP TABLE test123test456test;", labels);
        try {
            lists = PSQLminimal.sendSimpleQuery(socket, "CREATE TABLE test123test456test(id int, data VARCHAR(10));", labels);
        }
        catch (final Exception e){
            PSQLminimal.sendSimpleQuery(socket, "DROP TABLE test123test456test;", labels);
            throw new RuntimeException(e);
        }
        Assert.assertTrue(lists.size() == 0);
        lists = PSQLminimal.sendSimpleQuery(socket, "DROP TABLE test123test456test;", labels);
        Assert.assertTrue(lists.size() == 0);
        socket.close();
    }

    public LockSocket getSession()
    {
        try {
            final LockSocket socket = (LockSocket)PSQLminimal.getSocket(ip, port);

            Map<String, String> params = PSQLminimal.getParametersForStartup("dater", cluster + ".defaultdb");

            PSQLminimal.sendStartupPacket(socket.getOutputStream(), params);

            PSQLminimal.doAuthentication(socket, ip, "dater", "f38xyx4LEqjkQbhJT9Fxvg");
            return socket;
        }
        catch (final Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testInsertDeleteUpdate() throws IOException, SQLException {
        //final String host = PSQLminimal.getIp(ip, 26257);
        final LockSocket socket = PSQLminimal.getSocket(ip, port);

        Map<String, String> params = PSQLminimal.getParametersForStartup("app", cluster+".defaultdb");

        PSQLminimal.sendStartupPacket(socket.getOutputStream(), params);

        PSQLminimal.doAuthentication(socket, ip, "app", password );
        final List<String> labels = new ArrayList<>();
        List<List<Object>> lists;
        try {
            lists = PSQLminimal.sendSimpleQuery(socket, "DROP TABLE test123test456test;", labels);
        }catch (final Exception e)
        {
            final int i = 0;
        }
        lists = PSQLminimal.sendSimpleQuery(socket, "CREATE TABLE test123test456test(id int, data VARCHAR(10));", labels);

        lists = PSQLminimal.sendSimpleQuery(socket, "INSERT INTO test123test456test(id, data) VALUES (1, 'abc');", labels);
        Assert.assertTrue(lists.size() == 1);
        Assert.assertTrue("1".equals(lists.get(0).get(0)));
        lists = PSQLminimal.sendSimpleQuery(socket, "INSERT INTO test123test456test(id, data) VALUES (2, 'abc');", labels);
        lists = PSQLminimal.sendSimpleQuery(socket, "INSERT INTO test123test456test(id, data) VALUES (3, 'abc');", labels);

        lists = PSQLminimal.sendSimpleQuery(socket, "UPDATE test123test456test SET data = 'def' WHERE id = 3;", labels);
        Assert.assertTrue(lists.size() == 1);
        Assert.assertTrue("1".equals(lists.get(0).get(0)));

        lists = PSQLminimal.sendSimpleQuery(socket, "DELETE FROM test123test456test;", labels);
        Assert.assertTrue(lists.size() == 1);
        Assert.assertTrue("3".equals(lists.get(0).get(0)));

        PSQLminimal.sendSimpleQuery(socket, "DROP TABLE test123test456test;", labels);
        socket.close();
    }

    @Test (timeout = 30000)
    public void realisticUsageTest() throws IOException {

        try
        {
            LockSocket session = getSession();
            List<String> labels = new ArrayList<>();
            PSQLminimal.sendSimpleQuery(session, "DROP TABLE \"testrealistic1\";", labels);
            PSQLminimal.close(session);
        }
        catch (final Exception e)
        {

        }
        try
        {
            LockSocket session = getSession();
            List<String> labels = new ArrayList<>();
            PSQLminimal.sendSimpleQuery(session, "DROP TABLE \"testrealistic2\";", labels);
            PSQLminimal.close(session);
        }
        catch (final Exception e)
        {

        }
        try
        {
            LockSocket session = getSession();
            List<String> labels = new ArrayList<>();
            PSQLminimal.sendSimpleQuery(session, "DROP TABLE \"testrealistic3\";", labels);
            PSQLminimal.close(session);
        }
        catch (final Exception e)
        {

        }
        try
        {
            LockSocket session = getSession();
            List<String> labels = new ArrayList<>();
            PSQLminimal.sendSimpleQuery(session, "DROP TABLE \"testrealistic4\";", labels);
            PSQLminimal.close(session);
        }
        catch (final Exception e)
        {

        }


        LockSocket session = getSession();
        List<String> labels = new ArrayList<>();
        PSQLminimal.sendSimpleQuery(session, "CREATE TABLE \"testrealistic1\" (id BIGINT, a VARCHAR(20), b BIGINT);", labels);
        PSQLminimal.close(session);
        session = getSession();
        PSQLminimal.sendSimpleQuery(session, "CREATE TABLE \"testrealistic2\" (id BIGINT, a VARCHAR(20), b BIGINT);", labels);
        PSQLminimal.close(session);
        session = getSession();
        PSQLminimal.sendSimpleQuery(session, "CREATE TABLE \"testrealistic3\" (id BIGINT, a VARCHAR(20), b BIGINT);", labels);
        PSQLminimal.close(session);
        session = getSession();
        PSQLminimal.sendSimpleQuery(session, "CREATE TABLE \"testrealistic4\" (id BIGINT, a VARCHAR(20), b BIGINT);", labels);

        PSQLminimal.close(session);
        session = getSession();
        PSQLminimal.sendSimpleQuery(session, "INSERT INTO \"testrealistic1\" (id, a , b ) VALUES (1, 'hallo', 23423426453);", labels);
        PSQLminimal.close(session);
        session = getSession();
        PSQLminimal.sendSimpleQuery(session, "INSERT INTO \"testrealistic2\" (id, a , b ) VALUES (2, 'fsefesf', 3455);", labels);
        PSQLminimal.close(session);
        session = getSession();
        PSQLminimal.sendSimpleQuery(session, "INSERT INTO \"testrealistic3\" (id, a , b ) VALUES (3, '3333fff', 3355335);", labels);
        PSQLminimal.close(session);
        session = getSession();
        PSQLminimal.sendSimpleQuery(session, "INSERT INTO \"testrealistic1\" (id, a , b ) VALUES (4, 'hasefllo', 53353);", labels);
        PSQLminimal.close(session);
        session = getSession();
        PSQLminimal.sendSimpleQuery(session, "INSERT INTO \"testrealistic1\" (id, a , b ) VALUES (5, 'hallo', 2342426453);", labels);
        PSQLminimal.close(session);



        session = getSession();
        PSQLminimal.sendSimpleQuery(session, "DELETE FROM \"testrealistic1\" WHERE id = 4;", labels);
        PSQLminimal.close(session);
        session = getSession();
        List<List<Object>> lists = PSQLminimal.sendSimpleQuery(session, "SELECT * FROM \"testrealistic1\";", labels);
        Assert.assertTrue(lists.size() == 2);
        PSQLminimal.close(session);
        session = getSession();
        List<List<Object>> lists1 = PSQLminimal.sendSimpleQuery(session, "UPDATE \"testrealistic3\" SET id = 6 WHERE id = 3", labels);
        Assert.assertTrue(lists1.size() == 1);
        PSQLminimal.close(session);

        session = getSession();
        PSQLminimal.sendSimpleQuery(session, "DROP TABLE \"testrealistic1\";", labels);
        PSQLminimal.close(session);
        session = getSession();
        PSQLminimal.sendSimpleQuery(session, "DROP TABLE \"testrealistic2\";", labels);
        PSQLminimal.close(session);
        session = getSession();
        PSQLminimal.sendSimpleQuery(session, "DROP TABLE \"testrealistic3\";", labels);
        PSQLminimal.close(session);
        session = getSession();
        PSQLminimal.sendSimpleQuery(session, "DROP TABLE \"testrealisti4\";", labels);
        PSQLminimal.close(session);
    }

}

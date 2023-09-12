package main.root;

import com.ongres.scram.client.ScramClient;
import com.ongres.scram.client.ScramSession;
import com.ongres.scram.common.exception.ScramException;
import com.ongres.scram.common.exception.ScramInvalidServerSignatureException;
import com.ongres.scram.common.exception.ScramParseException;
import com.ongres.scram.common.exception.ScramServerErrorException;
import com.ongres.scram.common.stringprep.StringPreparations;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SCRAMhelper {

    private static ScramClient scramClient;
    private static ScramSession scramSession;

    private static OutputStream out;
    private static InputStream in;
    private static ScramSession.ClientFinalProcessor clientFinalProcessor;

    private static String password;

    public static void setPassword(final String pw)
    {
        password = pw;
    }

    public static void processServerMechanismsAndInit(final OutputStream outputStream, final InputStream inputStream) throws IOException {
        List<String> mechanisms = new ArrayList<>();
        out = outputStream;
        in = inputStream;

        do
        {
            byte[] read = PSQLminimal.readUntil(inputStream, 0);
            if(read.length < 2)
            {
                break;
            }
            mechanisms.add(new String(read));
        }while (true);

        if (mechanisms.size() < 1) {
            throw new RuntimeException("No SCRAM mechanism(s) advertised by the server");
        }

        try {
            scramClient = ScramClient
                    .channelBinding(ScramClient.ChannelBinding.NO)
                    .stringPreparation(StringPreparations.NO_PREPARATION)
                    .selectMechanismBasedOnServerAdvertised(mechanisms.toArray(new String[]{}))
                    .setup();
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Invalid or unsupported by client SCRAM mechanisms", e);
        }
        scramSession = scramClient.scramSession("*");// Real username is ignored by server, uses startup one
    }

    private static void sendAuthenticationMessage(int bodyLength, Runnable bodySender)
            throws IOException {
        PSQLminimal.write(out, new byte[]{'p'}, false);
        PSQLminimal.write(out, PSQLminimal.intToBytes(Integer.SIZE / Byte.SIZE + bodyLength), false);
        bodySender.run();
    }

    public static void sendScramClientFirstMessage() throws IOException {

        String clientFirstMessage = scramSession.clientFirstMessage();

        String scramMechanismName = scramClient.getScramMechanism().getName();
        final byte[] scramMechanismNameBytes = scramMechanismName.getBytes(StandardCharsets.UTF_8);
        final byte[] clientFirstMessageBytes = clientFirstMessage.getBytes(StandardCharsets.UTF_8);
        sendAuthenticationMessage(
                (scramMechanismNameBytes.length + 1) + 4 + clientFirstMessageBytes.length,
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            PSQLminimal.write(out, scramMechanismName.getBytes(StandardCharsets.UTF_8), false);
                            PSQLminimal.write(out, new byte[]{0}, false);
                            PSQLminimal.write(out, PSQLminimal.intToBytes(clientFirstMessageBytes.length), false);
                            PSQLminimal.write(out, clientFirstMessageBytes, true);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
        );
    }

    public static void processServerFirstMessage(int length) throws IOException {
        String serverFirstMessage = new String(PSQLminimal.readN(in, length), StandardCharsets.UTF_8);

        if (scramSession == null) {
            throw new RuntimeException("SCRAM session does not exist");
        }

        ScramSession.ServerFirstProcessor serverFirstProcessor;
        try {
            serverFirstProcessor = scramSession.receiveServerFirstMessage(serverFirstMessage);
        } catch (ScramException e) {
            throw new RuntimeException("Invalid server-first-message: "+serverFirstMessage);
        }

        clientFinalProcessor = serverFirstProcessor.clientFinalProcessor(password);

        String clientFinalMessage = clientFinalProcessor.clientFinalMessage();
        Logger.getLogger(SCRAMhelper.class.getName()).log(Level.FINEST, " FE=> SASLResponse( {0} )", clientFinalMessage);

        final byte[] clientFinalMessageBytes = clientFinalMessage.getBytes(StandardCharsets.UTF_8);
        sendAuthenticationMessage(
                clientFinalMessageBytes.length,
                new Runnable() {

                    @Override
                    public void run() {
                        try {
                            PSQLminimal.write(out, clientFinalMessageBytes, true);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                }
        );
    }

    public static void verifyServerSignature(int length) throws IOException {
        String serverFinalMessage = new String(PSQLminimal.readN(in, length), StandardCharsets.UTF_8);
        Logger.getLogger(SCRAMhelper.class.getName()).log(Level.FINEST, " <=BE AuthenticationSASLFinal( {0} )", serverFinalMessage);

        if (clientFinalProcessor == null) {
            throw new RuntimeException("SCRAM client final processor does not exist");
        }
        try {
            clientFinalProcessor.receiveServerFinalMessage(serverFinalMessage);
        } catch (ScramParseException e) {
            throw new RuntimeException("Invalid server-final-message: "+serverFinalMessage);
        } catch (ScramServerErrorException e) {
            throw new RuntimeException("SCRAM authentication failed, server returned error: "+
                            e.getError().getErrorMessage());
        } catch (ScramInvalidServerSignatureException e) {
            throw new RuntimeException("Invalid server SCRAM signature");
        }
    }

    public static void close() throws IOException {

        int i = 0;
        do {
            byte[] read = PSQLminimal.readUntil(in, 0);
            String s = new String(read, StandardCharsets.UTF_8);
            if(read.length > 0)
            {
                i++;
                if(s.contains("\f\n"))
                {
                    break;
                }
            }
            else
            {
                i++;
            }

        }while (i < 1000);
        PSQLminimal.readN(in, 1);
    }
}

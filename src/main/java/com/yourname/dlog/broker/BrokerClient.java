package com.yourname.dlog.broker;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.Socket;

/**
 * A minimal client for talking to a single BrokerServer.
 * One connection per request for now - simple and correct;
 * we can pool connections later if needed.
 */
public class BrokerClient {

    private final String host;
    private final int port;
    private final ObjectMapper mapper = new ObjectMapper();

    public BrokerClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Response send(Request request) throws IOException {
        try (Socket socket = new Socket(host, port);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            out.println(mapper.writeValueAsString(request));
            String responseLine = in.readLine();
            return mapper.readValue(responseLine, Response.class);
        }
    }
}
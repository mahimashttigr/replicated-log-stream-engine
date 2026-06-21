package com.yourname.dlog.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Client for talking to a single BrokerServer.
 *
 * V1 (Day 2): opened a new TCP socket per request - simple but slow.
 * V2 (Day 12): keeps one persistent connection open and reuses it.
 *
 * Why this matters: every new Socket() incurs a TCP handshake (SYN,
 * SYN-ACK, ACK) plus OS kernel buffer allocation. At 1000 events/sec
 * that's 1000 handshakes/sec of pure overhead. Our benchmark showed
 * actual throughput of only 511 ev/s against a 1000 ev/s target -
 * profiling pointed directly at Socket construction as the bottleneck.
 *
 * Persistent connection reduces per-request overhead to just the
 * serialization + write + read cycle, which is orders of magnitude
 * cheaper than a full TCP handshake.
 *
 * Thread-safety: NOT safe for concurrent use from multiple threads.
 * Each Producer instance has its own BrokerClient instances (via
 * clientCache), and Producer is already used single-threaded in our
 * benchmark. For multi-threaded producers, use a connection pool.
 */
public class BrokerClient {

    private final String host;
    private final int port;
    private final ObjectMapper mapper = new ObjectMapper();

    private Socket socket;
    private BufferedReader in;
    private PrintWriter out;

    public BrokerClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Response send(Request request) throws IOException {
        ensureConnected();
        try {
            out.println(mapper.writeValueAsString(request));
            String responseLine = in.readLine();
            if (responseLine == null) {
                // Server closed the connection - reconnect and retry once
                closeQuietly();
                ensureConnected();
                out.println(mapper.writeValueAsString(request));
                responseLine = in.readLine();
            }
            return mapper.readValue(responseLine, Response.class);
        } catch (IOException e) {
            // On any I/O error, close and force reconnect next call
            closeQuietly();
            throw e;
        }
    }

    private void ensureConnected() throws IOException {
        if (socket == null || socket.isClosed() || !socket.isConnected()) {
            socket = new Socket(host, port);
            socket.setTcpNoDelay(true); // disable Nagle's algorithm - send immediately, don't buffer
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new PrintWriter(socket.getOutputStream(), true);
        }
    }

    private void closeQuietly() {
        try { if (socket != null) socket.close(); } catch (IOException ignored) {}
        socket = null;
        in = null;
        out = null;
    }
}
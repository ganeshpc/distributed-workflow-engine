package com.workflowengine.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Caps {@code /api/} POST, PUT, and PATCH bodies at 64 KB.
 *
 * <p>If {@code Content-Length} is present and too large, responds 413 without
 * reading. If it is absent, counts bytes and throws
 * {@link PayloadTooLargeException} at 65536 + 1. Runs at highest precedence
 * on the request thread. Not a Tomcat multipart setting.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class BodySizeFilter extends OncePerRequestFilter {

    /** Maximum accepted body size in bytes (64 KiB). */
    static final int MAX_BYTES = 65_536;

    /**
     * Enforces the limit then continues the chain.
     *
     * @param request HTTP request
     * @param response HTTP response
     * @param filterChain remaining filters
     */
    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {
        if (!shouldLimit(request)) {
            filterChain.doFilter(request, response);
            return;
        }

        long contentLength = request.getContentLengthLong();
        if (contentLength > MAX_BYTES) {
            writeTooLarge(response);
            return;
        }
        if (contentLength >= 0) {
            filterChain.doFilter(request, response);
            return;
        }

        filterChain.doFilter(new LimitedRequest(request), response);
    }

    /**
     * Limits mutating {@code /api/} requests only.
     *
     * @param request HTTP request
     * @return true when the 64 KB cap applies
     */
    private static boolean shouldLimit(HttpServletRequest request) {
        String method = request.getMethod();
        if (!"POST".equalsIgnoreCase(method)
                && !"PUT".equalsIgnoreCase(method)
                && !"PATCH".equalsIgnoreCase(method)) {
            return false;
        }
        String uri = request.getRequestURI();
        return uri != null && uri.startsWith("/api/");
    }

    /**
     * Writes the generic 413 JSON body if the response is still open.
     *
     * @param response HTTP response
     * @throws IOException if writing fails
     */
    static void writeTooLarge(HttpServletResponse response) throws IOException {
        if (response.isCommitted()) {
            return;
        }
        response.resetBuffer();
        response.setStatus(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.getWriter().write("{\"error\":\"Payload Too Large\"}");
    }

    /**
     * Wraps the body so missing {@code Content-Length} still hits the cap.
     */
    static final class LimitedRequest extends HttpServletRequestWrapper {

        private ServletInputStream inputStream;
        private BufferedReader reader;

        /**
         * @param request original request whose stream will be counted
         */
        LimitedRequest(HttpServletRequest request) {
            super(request);
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            if (inputStream == null) {
                inputStream = new LimitedServletInputStream(super.getInputStream());
            }
            return inputStream;
        }

        @Override
        public BufferedReader getReader() throws IOException {
            if (reader == null) {
                Charset charset = charset();
                reader = new BufferedReader(new InputStreamReader(getInputStream(), charset));
            }
            return reader;
        }

        private Charset charset() {
            String encoding = getCharacterEncoding();
            return encoding == null ? StandardCharsets.UTF_8 : Charset.forName(encoding);
        }
    }

    /**
     * Counts bytes and throws {@link PayloadTooLargeException} past {@link #MAX_BYTES}.
     */
    static final class LimitedServletInputStream extends ServletInputStream {

        private final ServletInputStream delegate;
        private int count;

        /**
         * @param delegate underlying stream
         */
        LimitedServletInputStream(ServletInputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public int read() throws IOException {
            int value = delegate.read();
            if (value != -1) {
                increment(1);
            }
            return value;
        }

        @Override
        public int read(byte[] buffer, int off, int len) throws IOException {
            int n = delegate.read(buffer, off, len);
            if (n > 0) {
                increment(n);
            }
            return n;
        }

        /**
         * @param n bytes just read
         * @throws PayloadTooLargeException when the running total exceeds the cap
         */
        private void increment(int n) {
            count += n;
            if (count > MAX_BYTES) {
                throw new PayloadTooLargeException();
            }
        }

        @Override
        public boolean isFinished() {
            return delegate.isFinished();
        }

        @Override
        public boolean isReady() {
            return delegate.isReady();
        }

        @Override
        public void setReadListener(ReadListener readListener) {
            delegate.setReadListener(readListener);
        }
    }
}

package com.workflowengine.web;

import jakarta.servlet.ServletRequest;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the 64 KB {@link BodySizeFilter} without Spring or Postgres.
 *
 * <p>Covers Content-Length 413, missing-length abort, and the JSON error body.
 */
class BodySizeFilterTest {

    private final BodySizeFilter filter = new BodySizeFilter();

    @Test
    void contentLengthOverLimitIs413WithoutReading() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/workflows");
        request.setContentType("application/json");
        request.setContent(new byte[BodySizeFilter.MAX_BYTES + 1]);
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, (req, res) -> {
            throw new AssertionError("filter chain must not run for oversized Content-Length");
        });

        assertThat(response.getStatus()).isEqualTo(413);
        assertThat(response.getContentAsString()).isEqualTo("{\"error\":\"Payload Too Large\"}");
    }

    @Test
    void missingContentLengthAbortsOnByteAfterLimit() throws Exception {
        MockHttpServletRequest request = requestWithoutContentLength(new byte[BodySizeFilter.MAX_BYTES + 1]);
        MockHttpServletResponse response = new MockHttpServletResponse();

        assertThatThrownBy(() -> filter.doFilter(request, response, (req, res) ->
                req.getInputStream().readAllBytes()))
                .isInstanceOf(PayloadTooLargeException.class);
    }

    @Test
    void missingContentLengthAllowsExactlyMaxBytes() throws Exception {
        byte[] payload = new byte[BodySizeFilter.MAX_BYTES];
        MockHttpServletRequest request = requestWithoutContentLength(payload);
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicReference<byte[]> read = new AtomicReference<>();

        filter.doFilter(request, response, (req, res) -> read.set(req.getInputStream().readAllBytes()));

        assertThat(read.get()).hasSize(BodySizeFilter.MAX_BYTES);
        assertThat(response.getStatus()).isEqualTo(200);
    }

    @Test
    void getAndActuatorAreNotLimited() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/actuator/health");
        request.setContent(new byte[BodySizeFilter.MAX_BYTES + 1]);
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicReference<ServletRequest> seen = new AtomicReference<>();

        filter.doFilter(request, response, (req, res) -> seen.set(req));

        assertThat(seen.get()).isSameAs(request);
    }

    private static MockHttpServletRequest requestWithoutContentLength(byte[] payload) {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/workflows") {
            @Override
            public int getContentLength() {
                return -1;
            }

            @Override
            public long getContentLengthLong() {
                return -1L;
            }
        };
        request.setContentType("application/json");
        request.setContent(payload);
        return request;
    }
}

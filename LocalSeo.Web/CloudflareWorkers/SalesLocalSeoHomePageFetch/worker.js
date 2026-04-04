const WORKER_NAME = "SalesLocalSeoHomePageFetch";
const ALLOWED_METHODS = new Set(["GET", "POST"]);

export default {
  async fetch(request) {
    if (!ALLOWED_METHODS.has(request.method)) {
      return jsonResponse(
        {
          success: false,
          workerName: WORKER_NAME,
          requestedUrl: null,
          finalUrl: null,
          statusCode: null,
          fetchedUtc: new Date().toISOString(),
          html: null,
          errorMessage: "Method not allowed. Use GET or POST."
        },
        405
      );
    }

    const requestedUrl = await getRequestedUrl(request);
    if (!requestedUrl) {
      return jsonResponse(
        {
          success: false,
          workerName: WORKER_NAME,
          requestedUrl: null,
          finalUrl: null,
          statusCode: null,
          fetchedUtc: new Date().toISOString(),
          html: null,
          errorMessage: "A target URL is required."
        },
        400
      );
    }

    const validationError = validateTargetUrl(requestedUrl);
    if (validationError) {
      return jsonResponse(
        {
          success: false,
          workerName: WORKER_NAME,
          requestedUrl,
          finalUrl: null,
          statusCode: null,
          fetchedUtc: new Date().toISOString(),
          html: null,
          errorMessage: validationError
        },
        400
      );
    }

    try {
      const fetchResponse = await fetch(requestedUrl, {
        method: "GET",
        redirect: "follow",
        headers: {
          "accept": "text/html,application/xhtml+xml"
        }
      });

      const contentType = (fetchResponse.headers.get("content-type") || "").toLowerCase();
      if (!contentType.includes("text/html")) {
        return jsonResponse(
          {
            success: false,
            workerName: WORKER_NAME,
            requestedUrl,
            finalUrl: fetchResponse.url || requestedUrl,
            statusCode: fetchResponse.status,
            fetchedUtc: new Date().toISOString(),
            html: null,
            errorMessage: `Expected HTML but received '${contentType || "unknown"}'.`
          },
          200
        );
      }

      const html = await fetchResponse.text();
      return jsonResponse({
        success: fetchResponse.ok,
        workerName: WORKER_NAME,
        requestedUrl,
        finalUrl: fetchResponse.url || requestedUrl,
        statusCode: fetchResponse.status,
        fetchedUtc: new Date().toISOString(),
        html: fetchResponse.ok ? html : null,
        errorMessage: fetchResponse.ok ? null : `Homepage fetch failed with HTTP ${fetchResponse.status}.`
      });
    } catch (error) {
      return jsonResponse(
        {
          success: false,
          workerName: WORKER_NAME,
          requestedUrl,
          finalUrl: null,
          statusCode: null,
          fetchedUtc: new Date().toISOString(),
          html: null,
          errorMessage: error instanceof Error ? error.message : "Unexpected worker error."
        },
        200
      );
    }
  }
};

async function getRequestedUrl(request) {
  const url = new URL(request.url);
  if (request.method === "GET") {
    return normalizeUrl(url.searchParams.get("url"));
  }

  const contentType = (request.headers.get("content-type") || "").toLowerCase();
  if (contentType.includes("application/json")) {
    try {
      const body = await request.json();
      return normalizeUrl(body?.url);
    } catch {
      return null;
    }
  }

  if (contentType.includes("application/x-www-form-urlencoded") || contentType.includes("multipart/form-data")) {
    const form = await request.formData();
    return normalizeUrl(form.get("url"));
  }

  const textBody = await request.text();
  return normalizeUrl(textBody);
}

function normalizeUrl(value) {
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
}

function validateTargetUrl(value) {
  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    return "Target URL must be a valid absolute URL.";
  }

  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    return "Target URL must use http or https.";
  }

  const host = (parsed.hostname || "").toLowerCase();
  if (!host) {
    return "Target URL must include a host name.";
  }

  if (host === "localhost" || host.endsWith(".local")) {
    return "Local or internal hosts are not allowed.";
  }

  if (isPrivateIpv4(host)) {
    return "Private network IP addresses are not allowed.";
  }

  return null;
}

function isPrivateIpv4(host) {
  const match = host.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/);
  if (!match) {
    return false;
  }

  const octets = match.slice(1).map(Number);
  if (octets.some((value) => Number.isNaN(value) || value < 0 || value > 255)) {
    return true;
  }

  return (
    octets[0] === 10 ||
    octets[0] === 127 ||
    (octets[0] === 169 && octets[1] === 254) ||
    (octets[0] === 172 && octets[1] >= 16 && octets[1] <= 31) ||
    (octets[0] === 192 && octets[1] === 168)
  );
}

function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store"
    }
  });
}

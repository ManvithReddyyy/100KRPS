import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

export const mixedSuccessRate = new Rate('mixed_success_rate');
export const tripCreationCount = new Counter('trips_created');
export const sseProbeCount = new Counter('sse_probes');

export const options = {
  scenarios: {
    mixed_traffic: {
      executor: 'ramping-arrival-rate',
      startRate: 1000,
      timeUnit: '1s',
      preAllocatedVUs: 500,
      maxVUs: 5000,
      stages: [
        { duration: '30s', target: 5000 },
        { duration: '1m',  target: 20000 },
        { duration: '30s', target: 20000 },
        { duration: '15s', target: 0 },
      ],
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<15', 'p(99)<35'],
    mixed_success_rate: ['rate>0.98'],
  },
};

const BASE_URL = __ENV.TARGET_URL || 'http://localhost:8081';

export function setup() {
  const tripPayload = JSON.stringify({
    dest_lat: 12.9716,
    dest_lng: 77.5946,
    arrival_radius_m: 50,
    expires_in_minutes: 120,
  });

  const res = http.post(`${BASE_URL}/v1/trips`, tripPayload, {
    headers: { 'Content-Type': 'application/json' },
  });

  if (res.status !== 201) {
    throw new Error(`Failed to create baseline trip: ${res.body}`);
  }

  const data = JSON.parse(res.body);
  return {
    primaryCode: data.share_code,
    primaryToken: data.owner_token,
  };
}

export default function (data) {
  const roll = Math.random();

  // 1. 2% Traffic: Dynamic Trip Creation (Cold path)
  if (roll < 0.02) {
    const payload = JSON.stringify({
      dest_lat: 17.3850 + (Math.random() - 0.5) * 0.1,
      dest_lng: 78.4867 + (Math.random() - 0.5) * 0.1,
      arrival_radius_m: 50,
      expires_in_minutes: 60,
    });

    const res = http.post(`${BASE_URL}/v1/trips`, payload, {
      headers: { 'Content-Type': 'application/json' },
    });

    const ok = check(res, { 'create 201': (r) => r.status === 201 });
    mixedSuccessRate.add(ok);
    if (ok) tripCreationCount.add(1);
    return;
  }

  // 2. 8% Traffic: Metadata & Snapshot Lookups (Viewer path)
  if (roll < 0.10) {
    const res = http.get(`${BASE_URL}/v1/trips/${data.primaryCode}`);
    const ok = check(res, { 'get trip 200': (r) => r.status === 200 });
    mixedSuccessRate.add(ok);
    sseProbeCount.add(1);
    return;
  }

  // 3. 90% Traffic: Hot GPS Ping Ingestion (Zero-Postgres path)
  const payload = JSON.stringify({
    lat: 12.9716 + (Math.random() - 0.5) * 0.02,
    lng: 77.5946 + (Math.random() - 0.5) * 0.02,
    speed_mps: 8.5,
  });

  const res = http.post(`${BASE_URL}/v1/trips/${data.primaryCode}/ping`, payload, {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${data.primaryToken}`,
    },
  });

  const ok = check(res, { 'ping 200': (r) => r.status === 200 });
  mixedSuccessRate.add(ok);
}

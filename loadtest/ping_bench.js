import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Custom metric trackers
export const pingLatency = new Trend('ping_duration_ms', true);
export const successRate = new Rate('success_rate');
export const totalPings = new Counter('total_pings_sent');

// ──────────────────────────────────────────────────────────────
// Benchmark Scenarios
// Targets up to 100,000 Requests/Second across a ramp-up curve
// ──────────────────────────────────────────────────────────────
export const options = {
  scenarios: {
    hot_ping_burst: {
      executor: 'ramping-arrival-rate',
      startRate: 1000,
      timeUnit: '1s',
      preAllocatedVUs: 500,
      maxVUs: 10000,
      stages: [
        { duration: '10s', target: 5000 },   // Smooth warm-up to 5k
        { duration: '20s', target: 15000 },  // Ramp to 15k
        { duration: '30s', target: 30000 },  // Peak at 30k
        { duration: '20s', target: 50000 },  // Push to 50k
        { duration: '15s', target: 0 },      // Cool down
      ],
    },
  },
  thresholds: {
    // 99% of requests must complete in under 20ms
    http_req_duration: ['p(95)<10', 'p(99)<25'],
    success_rate: ['rate>0.99'], // >99% success rate
  },
};

const BASE_URL = __ENV.TARGET_URL || 'http://localhost:8081';

// Pre-configured test trip fixture (created in setup phase)
export function setup() {
  const tripPayload = JSON.stringify({
    dest_lat: 17.4401,
    dest_lng: 78.3489,
    arrival_radius_m: 50,
    expires_in_minutes: 120,
  });

  const res = http.post(`${BASE_URL}/v1/trips`, tripPayload, {
    headers: { 'Content-Type': 'application/json' },
  });

  if (res.status !== 201) {
    throw new Error(`Setup failed to create trip: ${res.status} - ${res.body}`);
  }

  const trip = JSON.parse(res.body);
  console.log(`[SETUP] Benchmark trip initialized: code=${trip.share_code}`);
  return {
    shareCode: trip.share_code,
    ownerToken: trip.owner_token,
  };
}

export default function (data) {
  // Random small coordinate jitter around origin
  const lat = 17.3850 + (Math.random() - 0.5) * 0.05;
  const lng = 78.4867 + (Math.random() - 0.5) * 0.05;
  const speed = 10 + Math.random() * 15;

  const payload = JSON.stringify({
    lat: lat,
    lng: lng,
    speed_mps: speed,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${data.ownerToken}`,
      'X-Request-ID': `bench-${__VU}-${__ITER}`,
    },
    timeout: '2s',
  };

  const start = Date.now();
  const res = http.post(`${BASE_URL}/v1/trips/${data.shareCode}/ping`, payload, params);
  const dur = Date.now() - start;

  totalPings.add(1);
  pingLatency.add(dur);

  const ok = check(res, {
    'status is 202': (r) => r.status === 202 || r.status === 200,
    'has valid status': (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.status === 'OK' || body.status === 'ARRIVED';
      } catch (e) {
        return false;
      }
    },
  });

  successRate.add(ok);
}

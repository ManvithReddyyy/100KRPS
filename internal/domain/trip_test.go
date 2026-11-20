package domain

import (
	"encoding/hex"
	"math"
	"testing"
)

// ──────────────────── Haversine Tests ────────────────────

func TestHaversineMeters_KnownDistances(t *testing.T) {
	tests := []struct {
		name                       string
		lat1, lng1, lat2, lng2     float64
		wantMeters, toleranceMeters float64
	}{
		{
			name:            "same point",
			lat1: 12.9716, lng1: 77.5946, lat2: 12.9716, lng2: 77.5946,
			wantMeters: 0, toleranceMeters: 0.01,
		},
		{
			name:            "Bangalore to Chennai (~290 km)",
			lat1: 12.9716, lng1: 77.5946, lat2: 13.0827, lng2: 80.2707,
			wantMeters: 290_000, toleranceMeters: 5_000, // ±5 km
		},
		{
			name:            "New York to London (~5570 km)",
			lat1: 40.7128, lng1: -74.0060, lat2: 51.5074, lng2: -0.1278,
			wantMeters: 5_570_000, toleranceMeters: 30_000, // ±30 km
		},
		{
			name:            "short walk ~100m",
			lat1: 12.9716, lng1: 77.5946, lat2: 12.9725, lng2: 77.5946,
			wantMeters: 100, toleranceMeters: 5,
		},
		{
			name:            "antipodal points (~20000 km)",
			lat1: 0, lng1: 0, lat2: 0, lng2: 180,
			wantMeters: math.Pi * earthRadiusM, toleranceMeters: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := HaversineMeters(tc.lat1, tc.lng1, tc.lat2, tc.lng2)
			diff := math.Abs(got - tc.wantMeters)
			if diff > tc.toleranceMeters {
				t.Errorf("HaversineMeters(%v,%v → %v,%v) = %.1f m, want %.1f m (±%.0f m), diff=%.1f",
					tc.lat1, tc.lng1, tc.lat2, tc.lng2,
					got, tc.wantMeters, tc.toleranceMeters, diff)
			}
		})
	}
}

func TestHaversineMeters_Symmetry(t *testing.T) {
	d1 := HaversineMeters(12.9716, 77.5946, 13.0827, 80.2707)
	d2 := HaversineMeters(13.0827, 80.2707, 12.9716, 77.5946)
	if d1 != d2 {
		t.Errorf("Haversine not symmetric: %f != %f", d1, d2)
	}
}

// ──────────────────── Auth / Hashing Tests ────────────────────

func TestHashToken_Deterministic(t *testing.T) {
	token := "aa" + "bb" + "cc" + "dd" // 8 hex chars = 4 bytes
	// Pad to valid hex token (need even number of hex chars)
	token = "aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd" // 64 hex = 32 bytes

	h1, err := HashToken(token)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := HashToken(token)
	if err != nil {
		t.Fatal(err)
	}
	if !ConstantTimeHashCompare(h1, h2) {
		t.Error("same token produced different hashes")
	}
}

func TestHashToken_DifferentTokens(t *testing.T) {
	t1 := "aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd"
	t2 := "aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccde"

	h1, _ := HashToken(t1)
	h2, _ := HashToken(t2)
	if ConstantTimeHashCompare(h1, h2) {
		t.Error("different tokens produced same hash")
	}
}

func TestHashToken_InvalidHex(t *testing.T) {
	_, err := HashToken("not-valid-hex!")
	if err == nil {
		t.Error("expected error for invalid hex")
	}
}

func TestGenerateOwnerToken_Unique(t *testing.T) {
	t1, h1, err := GenerateOwnerToken()
	if err != nil {
		t.Fatal(err)
	}
	t2, h2, err := GenerateOwnerToken()
	if err != nil {
		t.Fatal(err)
	}
	if t1 == t2 {
		t.Error("two tokens should not be equal")
	}
	if ConstantTimeHashCompare(h1, h2) {
		t.Error("two token hashes should not be equal")
	}
	// Token should be 64 hex chars (32 bytes)
	if len(t1) != 64 {
		t.Errorf("token length = %d, want 64", len(t1))
	}
	// Hash should be 32 bytes (SHA-256)
	if len(h1) != 32 {
		t.Errorf("hash length = %d, want 32", len(h1))
	}
}

func TestTokenHashHex_RoundTrip(t *testing.T) {
	token, rawHash, err := GenerateOwnerToken()
	if err != nil {
		t.Fatal(err)
	}
	hexHash, err := TokenHashHex(token)
	if err != nil {
		t.Fatal(err)
	}
	// Convert rawHash to hex and compare
	if hex.EncodeToString(rawHash) != hexHash {
		t.Error("TokenHashHex does not match raw hash")
	}
}

func TestConstantTimeHashCompare(t *testing.T) {
	a := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	b := make([]byte, 32)
	copy(b, a)

	if !ConstantTimeHashCompare(a, b) {
		t.Error("identical slices should compare equal")
	}

	b[31] = 0xFF
	if ConstantTimeHashCompare(a, b) {
		t.Error("different slices should not compare equal")
	}
}

// ──────────────────── Share Code Tests ────────────────────

func TestGenerateShareCode_Format(t *testing.T) {
	code, err := GenerateShareCode()
	if err != nil {
		t.Fatal(err)
	}
	if len(code) != 8 {
		t.Errorf("share code length = %d, want 8", len(code))
	}
	for _, c := range code {
		if !((c >= 'A' && c <= 'Z') || (c >= '2' && c <= '7')) {
			t.Errorf("share code contains invalid char: %c", c)
		}
	}
}

func TestGenerateShareCode_Unique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		code, err := GenerateShareCode()
		if err != nil {
			t.Fatal(err)
		}
		if seen[code] {
			t.Fatalf("duplicate share code on iteration %d: %s", i, code)
		}
		seen[code] = true
	}
}

// ──────────────────── UUID Tests ────────────────────

func TestGenerateUUID_Format(t *testing.T) {
	id := GenerateUUID()
	// UUID v4 format: 8-4-4-4-12 hex chars with hyphens
	if len(id) != 36 {
		t.Fatalf("UUID length = %d, want 36, got %q", len(id), id)
	}
	if id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		t.Errorf("UUID missing hyphens: %s", id)
	}
}

func TestGenerateUUID_Unique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := GenerateUUID()
		if seen[id] {
			t.Fatalf("duplicate UUID on iteration %d", i)
		}
		seen[id] = true
	}
}

// ──────────────────── Validation Tests ────────────────────

func TestCreateTripRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		req     CreateTripRequest
		wantErr bool
	}{
		{"valid", CreateTripRequest{DestLat: 12.9, DestLng: 77.5, ExpiresMinutes: 60}, false},
		{"lat too low", CreateTripRequest{DestLat: -91, DestLng: 77.5, ExpiresMinutes: 60}, true},
		{"lat too high", CreateTripRequest{DestLat: 91, DestLng: 77.5, ExpiresMinutes: 60}, true},
		{"lng too low", CreateTripRequest{DestLat: 12.9, DestLng: -181, ExpiresMinutes: 60}, true},
		{"lng too high", CreateTripRequest{DestLat: 12.9, DestLng: 181, ExpiresMinutes: 60}, true},
		{"expiry too short", CreateTripRequest{DestLat: 12.9, DestLng: 77.5, ExpiresMinutes: 4}, true},
		{"expiry too long", CreateTripRequest{DestLat: 12.9, DestLng: 77.5, ExpiresMinutes: 721}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.req.Validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestCreateTripRequest_DefaultRadius(t *testing.T) {
	req := CreateTripRequest{DestLat: 12.9, DestLng: 77.5, ExpiresMinutes: 60}
	if err := req.Validate(); err != nil {
		t.Fatal(err)
	}
	if req.ArrivalRadius != 50 {
		t.Errorf("default radius = %f, want 50", req.ArrivalRadius)
	}
}

func TestPingRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		req     PingRequest
		wantErr bool
	}{
		{"valid", PingRequest{Lat: 12.9, Lng: 77.5}, false},
		{"valid with speed", PingRequest{Lat: 12.9, Lng: 77.5, SpeedMPS: 5.0}, false},
		{"lat too low", PingRequest{Lat: -91, Lng: 77.5}, true},
		{"lng too high", PingRequest{Lat: 12.9, Lng: 181}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.req.Validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

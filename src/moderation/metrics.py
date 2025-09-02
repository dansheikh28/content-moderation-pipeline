from __future__ import annotations


class Meter:
    def __init__(self):
        self.requests_total = 0
        self.flagged_total = 0
        self.cache_hits_total = 0
        self._lat_ms = []

    def inc(self, cache_hit: bool, flagged: bool, latency_ms: float):
        self.requests_total += 1
        if cache_hit:
            self.cache_hits_total += 1
        if flagged:
            self.flagged_total += 1
        self._lat_ms.append(float(latency_ms))
        if len(self._lat_ms) > 1000:
            self._lat_ms = self._lat_ms[-1000:]

    def snapshot(self):
        lat = self._lat_ms or [0.0]
        return {
            "requests_total": self.requests_total,
            "flagged_total": self.flagged_total,
            "cache_hits_total": self.cache_hits_total,
            "latency_ms_avg": round(sum(lat) / len(lat), 2),
        }


meter = Meter()
